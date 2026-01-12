"""
State Persistence Manager

Provides session recovery capability by persisting pipeline state to disk.
This prevents data loss on page refresh or accidental browser close.

Design Decisions:
1. Uses temp directory with session-specific ID
2. Saves after each major operation (SAP parse, extraction, edit)
3. Automatically loads on session start if state exists
4. Cleans up on explicit user reset
5. Thread-safe for concurrent writes

Future Considerations:
- Could be extended to persist to database for multi-device access
- Could add versioning for state migration
"""

import json
import pickle
import tempfile
import threading
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import hashlib
import logging

logger = logging.getLogger(__name__)


@dataclass
class StateSnapshot:
    """Serializable snapshot of pipeline state"""
    timestamp: str
    sap_data: Dict[str, Any]
    inbound_shipments: List[Dict[str, Any]]
    outbound_shipments: List[Dict[str, Any]]
    raw_responses: Dict[str, str]  # NEW: Store raw AI responses
    audit_entries: List[Dict[str, Any]]
    user_settings: Dict[str, Any]
    processing_stage: str  # Track where we are in the workflow
    

class StateManager:
    """
    Manages persistent state for session recovery.
    
    Usage:
        manager = StateManager()
        manager.save_sap_data(sap_data)
        # ... later, after refresh ...
        recovered = manager.load_state()
    """
    
    def __init__(self, session_id: Optional[str] = None):
        """
        Initialize with optional session ID.
        If not provided, generates one from timestamp.
        """
        self.session_id = session_id or self._generate_session_id()
        self._state_dir = Path(tempfile.gettempdir()) / "mgis_sessions"
        self._state_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        
        # In-memory cache
        self._current_state: Optional[StateSnapshot] = None
        self._raw_responses: Dict[str, str] = {}  # filename -> raw response
    
    def _generate_session_id(self) -> str:
        """Generate a unique session ID"""
        timestamp = datetime.now().isoformat()
        return hashlib.md5(timestamp.encode()).hexdigest()[:12]
    
    @property
    def state_file(self) -> Path:
        """Path to the state file"""
        return self._state_dir / f"mgis_state_{self.session_id}.pkl"
    
    @property
    def raw_responses_file(self) -> Path:
        """Path to raw responses file (kept separate for size)"""
        return self._state_dir / f"mgis_raw_{self.session_id}.json"
    
    def save_raw_response(self, document_id: str, raw_response: str):
        """
        Save raw AI response for debugging.
        
        This is critical for debugging extraction issues.
        """
        with self._lock:
            self._raw_responses[document_id] = raw_response
            
            # Persist to disk
            try:
                with open(self.raw_responses_file, 'w') as f:
                    json.dump(self._raw_responses, f, indent=2)
            except Exception as e:
                logger.warning(f"Failed to save raw responses: {e}")
    
    def get_raw_response(self, document_id: str) -> Optional[str]:
        """Get raw AI response for a document"""
        return self._raw_responses.get(document_id)
    
    def get_all_raw_responses(self) -> Dict[str, str]:
        """Get all raw responses for debugging UI"""
        return self._raw_responses.copy()
    
    def save_state(
        self,
        sap_data: Dict,
        inbound_shipments: List,
        outbound_shipments: List,
        audit_entries: List,
        user_settings: Dict,
        processing_stage: str
    ):
        """
        Save complete pipeline state.
        
        Called after:
        - SAP files parsed
        - Each PDF processed
        - User edits saved
        """
        with self._lock:
            # Convert shipments to dicts if they're dataclasses
            inbound_dicts = [
                s.to_dict() if hasattr(s, 'to_dict') else asdict(s)
                for s in inbound_shipments
            ]
            outbound_dicts = [
                s.to_dict() if hasattr(s, 'to_dict') else asdict(s)
                for s in outbound_shipments
            ]
            
            # Convert SAP data
            sap_dicts = {}
            for key, data in sap_data.items():
                if hasattr(data, '__dict__'):
                    sap_dicts[key] = {
                        'pdo_number': data.pdo_number,
                        'brands': data.brands,
                        'currency': data.currency,
                        'total_value': data.total_value,
                        'country_splits': data.country_splits,
                        'source_file': data.source_file,
                        'sheet_name': data.sheet_name
                    }
                else:
                    sap_dicts[key] = data
            
            snapshot = StateSnapshot(
                timestamp=datetime.now().isoformat(),
                sap_data=sap_dicts,
                inbound_shipments=inbound_dicts,
                outbound_shipments=outbound_dicts,
                raw_responses=self._raw_responses,
                audit_entries=[asdict(e) if hasattr(e, '__dict__') else e for e in audit_entries],
                user_settings=user_settings,
                processing_stage=processing_stage
            )
            
            self._current_state = snapshot
            
            try:
                with open(self.state_file, 'wb') as f:
                    pickle.dump(snapshot, f)
                logger.info(f"State saved to {self.state_file}")
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
    
    def load_state(self) -> Optional[StateSnapshot]:
        """
        Load state from disk if it exists.
        
        Returns None if no saved state.
        """
        with self._lock:
            if not self.state_file.exists():
                return None
            
            try:
                with open(self.state_file, 'rb') as f:
                    snapshot = pickle.load(f)
                
                # Also load raw responses
                if self.raw_responses_file.exists():
                    with open(self.raw_responses_file, 'r') as f:
                        self._raw_responses = json.load(f)
                
                self._current_state = snapshot
                logger.info(f"State loaded from {self.state_file}")
                return snapshot
                
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None
    
    def has_saved_state(self) -> bool:
        """Check if there's a saved state to recover"""
        return self.state_file.exists()
    
    def clear_state(self):
        """Clear all saved state"""
        with self._lock:
            self._current_state = None
            self._raw_responses = {}
            
            if self.state_file.exists():
                self.state_file.unlink()
            if self.raw_responses_file.exists():
                self.raw_responses_file.unlink()
            
            logger.info("State cleared")
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get summary of current state for UI display"""
        if self._current_state is None:
            if not self.load_state():
                return {'has_state': False}
        
        state = self._current_state
        return {
            'has_state': True,
            'timestamp': state.timestamp,
            'sap_pdos': len(state.sap_data),
            'inbound_count': len(state.inbound_shipments),
            'outbound_count': len(state.outbound_shipments),
            'processing_stage': state.processing_stage,
            'raw_responses_count': len(state.raw_responses)
        }
    
    @classmethod
    def list_sessions(cls) -> List[Dict[str, Any]]:
        """List all saved sessions (for recovery UI)"""
        state_dir = Path(tempfile.gettempdir()) / "mgis_sessions"
        if not state_dir.exists():
            return []
        
        sessions = []
        for state_file in state_dir.glob("mgis_state_*.pkl"):
            try:
                with open(state_file, 'rb') as f:
                    snapshot = pickle.load(f)
                sessions.append({
                    'session_id': state_file.stem.replace('mgis_state_', ''),
                    'timestamp': snapshot.timestamp,
                    'inbound_count': len(snapshot.inbound_shipments),
                    'outbound_count': len(snapshot.outbound_shipments)
                })
            except Exception:
                continue
        
        return sorted(sessions, key=lambda x: x['timestamp'], reverse=True)
    
    @classmethod
    def cleanup_old_sessions(cls, max_age_hours: int = 24):
        """Remove sessions older than max_age_hours"""
        state_dir = Path(tempfile.gettempdir()) / "mgis_sessions"
        if not state_dir.exists():
            return
        
        cutoff = datetime.now().timestamp() - (max_age_hours * 3600)
        
        for state_file in state_dir.glob("mgis_*.pkl"):
            if state_file.stat().st_mtime < cutoff:
                state_file.unlink()
                logger.info(f"Cleaned up old session: {state_file}")
        
        for raw_file in state_dir.glob("mgis_raw_*.json"):
            if raw_file.stat().st_mtime < cutoff:
                raw_file.unlink()
