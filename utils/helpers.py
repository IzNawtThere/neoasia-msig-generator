"""
Utilities Module

Contains:
1. Rate limiter for API calls
2. Audit trail management
3. Common helper functions
"""

import time
import threading
from datetime import datetime
from typing import List, Any, Optional, Dict
from dataclasses import dataclass, field, asdict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Thread-safe rate limiter for API calls.
    
    Design Decision: Using a simple time-based approach rather than
    token bucket because:
    1. Claude API has straightforward requests/minute limits
    2. Our use case is sequential, not concurrent
    3. Simpler to debug and understand
    
    The delay is applied BEFORE each call, not after, to ensure
    the first call after a batch also respects the limit.
    """
    
    def __init__(self, min_delay_seconds: float = 10.0):
        self.min_delay = min_delay_seconds
        self.last_call_time: Optional[float] = None
        self._lock = threading.Lock()
        self._call_count = 0
        
    def wait(self) -> float:
        """
        Wait if necessary to respect rate limit.
        Returns the actual wait time in seconds.
        """
        with self._lock:
            now = time.time()
            wait_time = 0.0
            
            if self.last_call_time is not None:
                elapsed = now - self.last_call_time
                if elapsed < self.min_delay:
                    wait_time = self.min_delay - elapsed
                    logger.debug(f"Rate limiter waiting {wait_time:.1f}s")
                    time.sleep(wait_time)
            
            self.last_call_time = time.time()
            self._call_count += 1
            return wait_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        return {
            'total_calls': self._call_count,
            'min_delay_seconds': self.min_delay,
            'last_call_time': self.last_call_time
        }
    
    def reset(self):
        """Reset the rate limiter state"""
        with self._lock:
            self.last_call_time = None
            self._call_count = 0


@dataclass
class AuditEntry:
    """Single audit log entry"""
    timestamp: datetime
    action: str
    record_reference: str
    field_name: Optional[str]
    old_value: Any
    new_value: Any
    source: str  # AI, SAP, USER, SYSTEM
    notes: str = ""


class AuditTrail:
    """
    Manages audit trail for all extraction and editing operations.
    
    Design Decision: In-memory for now, but structured to support
    persistence to database/file in the future.
    """
    
    def __init__(self):
        self.entries: List[AuditEntry] = []
        self._lock = threading.Lock()
    
    def log(self, action: str, reference: str, field: Optional[str],
            old_value: Any, new_value: Any, source: str, notes: str = ""):
        """Add an audit entry"""
        with self._lock:
            entry = AuditEntry(
                timestamp=datetime.now(),
                action=action,
                record_reference=reference,
                field_name=field,
                old_value=old_value,
                new_value=new_value,
                source=source,
                notes=notes
            )
            self.entries.append(entry)
            logger.debug(f"Audit: {action} on {reference}.{field}: {old_value} -> {new_value}")
    
    def log_extraction(self, reference: str, field: str, value: Any, source: str = "AI"):
        """Log an extraction event"""
        self.log("EXTRACTED", reference, field, None, value, source)
    
    def log_user_edit(self, reference: str, field: str, old_value: Any, new_value: Any):
        """Log a user edit"""
        self.log("USER_EDIT", reference, field, old_value, new_value, "USER")
    
    def log_validation(self, reference: str, issues: List[str]):
        """Log validation results"""
        self.log("VALIDATED", reference, None, None, issues, "SYSTEM",
                f"{len(issues)} issues found")
    
    def log_export(self, reference: str, destination: str):
        """Log export event"""
        self.log("EXPORTED", reference, None, None, destination, "SYSTEM")
    
    def get_entries_for_record(self, reference: str) -> List[AuditEntry]:
        """Get all audit entries for a specific record"""
        return [e for e in self.entries if e.record_reference == reference]
    
    def get_user_edits(self) -> List[AuditEntry]:
        """Get all user edits"""
        return [e for e in self.entries if e.action == "USER_EDIT"]
    
    def to_dataframe(self):
        """Convert to pandas DataFrame for export"""
        import pandas as pd
        return pd.DataFrame([asdict(e) for e in self.entries])
    
    def clear(self):
        """Clear all entries"""
        with self._lock:
            self.entries.clear()


def normalize_tracking_number(tracking: str) -> str:
    """
    Normalize tracking number by removing spaces and standardizing format.
    
    Examples:
        "8846 0237 3339" -> "884602373339"
        "884-602-373-339" -> "884602373339"
    """
    if not tracking:
        return ""
    return ''.join(c for c in tracking if c.isalnum())


def normalize_awb_number(awb: str) -> str:
    """
    Normalize AWB number to XXX-XXXXXXXX format.
    
    Examples:
        "235 30462681" -> "235-30462681"
        "235-30462681" -> "235-30462681"
        "23530462681" -> "235-30462681"
    """
    if not awb:
        return ""
    
    # Remove all non-alphanumeric
    clean = ''.join(c for c in awb if c.isalnum())
    
    # If 11 digits, format as XXX-XXXXXXXX
    if len(clean) == 11 and clean.isdigit():
        return f"{clean[:3]}-{clean[3:]}"
    
    # Return cleaned version if already has dash
    if '-' in awb:
        parts = awb.split('-')
        if len(parts) == 2:
            return f"{parts[0].strip()}-{parts[1].strip()}"
    
    return awb


def extract_pdo_numbers(text: str) -> List[str]:
    """
    Extract PDO numbers from a string.
    
    Examples:
        "PDO 2500444_dtd251006_NST.pdf" -> ["2500444"]
        "PDO 2500430 & 2500432_dtd250926_IFC.pdf" -> ["2500430", "2500432"]
        "PDO2500437,439,440,441_dtd251003_NST.pdf" -> ["2500437", "2500439", "2500440", "2500441"]
    """
    import re
    
    pdo_numbers = []
    
    # Pattern 1: PDO followed by number
    pattern1 = re.findall(r'PDO\s*(\d+)', text, re.IGNORECASE)
    pdo_numbers.extend(pattern1)
    
    # Pattern 2: Numbers separated by comma (partial numbers)
    # e.g., "2500437,439,440,441" should give full numbers
    partial_pattern = re.search(r'(\d{7}),(\d{3}(?:,\d{3})*)', text)
    if partial_pattern:
        base = partial_pattern.group(1)[:4]  # First 4 digits as base
        pdo_numbers.append(partial_pattern.group(1))
        for partial in partial_pattern.group(2).split(','):
            full_num = base + partial
            if full_num not in pdo_numbers:
                pdo_numbers.append(full_num)
    
    # Pattern 3: Numbers with & separator
    and_pattern = re.findall(r'(\d{7})\s*[&,]\s*(\d{7})', text)
    for match in and_pattern:
        for num in match:
            if num not in pdo_numbers:
                pdo_numbers.append(num)
    
    return list(set(pdo_numbers))


def extract_itr_number(text: str) -> Optional[str]:
    """
    Extract ITR or SOM number from text.
    
    Examples:
        "ITR 2502027_Invoice.pdf" -> "ITR 2502027"
        "ITR2502101" -> "ITR 2502101"
    """
    import re
    
    # Pattern: ITR or SOM followed by digits
    match = re.search(r'(ITR|SOM)\s*(\d+)', text, re.IGNORECASE)
    if match:
        prefix = match.group(1).upper()
        number = match.group(2)
        return f"{prefix} {number}"
    
    return None


def format_currency_value(value: float, currency: str) -> str:
    """Format a currency value for display"""
    if value is None:
        return ""
    
    # Format with thousands separator
    if currency in ['IDR', 'VND']:
        return f"{value:,.0f}"
    else:
        return f"{value:,.2f}"


class FileValidator:
    """
    Validates uploaded files before processing.
    
    Design Decision: Fail fast with clear error messages rather than
    cryptic errors deep in processing. This is a safeguard that respects
    user competence while preventing common mistakes.
    """
    
    # Magic bytes for file type validation
    MAGIC_BYTES = {
        'pdf': b'%PDF',
        'xlsx': b'PK\x03\x04',  # ZIP-based format
        'xls': b'\xd0\xcf\x11\xe0',  # OLE compound format
    }
    
    # Maximum file sizes (safety limits)
    MAX_SIZES = {
        'pdf': 100 * 1024 * 1024,   # 100 MB
        'xlsx': 50 * 1024 * 1024,   # 50 MB
        'xls': 50 * 1024 * 1024,    # 50 MB
    }
    
    @classmethod
    def validate_pdf(cls, file_obj) -> tuple[bool, str]:
        """
        Validate a PDF file.
        
        Returns (is_valid, error_message)
        """
        try:
            # Check file size
            file_obj.seek(0, 2)  # Seek to end
            size = file_obj.tell()
            file_obj.seek(0)  # Reset
            
            if size == 0:
                return False, "File is empty"
            
            if size > cls.MAX_SIZES['pdf']:
                return False, f"File too large ({size / 1024 / 1024:.1f} MB, max 100 MB)"
            
            # Check magic bytes
            header = file_obj.read(4)
            file_obj.seek(0)  # Reset
            
            if header != cls.MAGIC_BYTES['pdf']:
                return False, "File is not a valid PDF (invalid header)"
            
            return True, ""
            
        except Exception as e:
            return False, f"Error validating file: {e}"
    
    @classmethod
    def validate_excel(cls, file_obj) -> tuple[bool, str]:
        """
        Validate an Excel file (.xlsx or .xls).
        
        Returns (is_valid, error_message)
        """
        try:
            # Check file size
            file_obj.seek(0, 2)
            size = file_obj.tell()
            file_obj.seek(0)
            
            if size == 0:
                return False, "File is empty"
            
            if size > cls.MAX_SIZES['xlsx']:
                return False, f"File too large ({size / 1024 / 1024:.1f} MB, max 50 MB)"
            
            # Check magic bytes (xlsx or xls)
            header = file_obj.read(4)
            file_obj.seek(0)
            
            if header == cls.MAGIC_BYTES['xlsx']:
                return True, ""
            elif header == cls.MAGIC_BYTES['xls']:
                return True, ""
            else:
                return False, "File is not a valid Excel file (invalid header)"
            
        except Exception as e:
            return False, f"Error validating file: {e}"
    
    @classmethod
    def validate_filename(cls, filename: str, expected_type: str) -> tuple[bool, str]:
        """
        Validate filename matches expected type.
        
        Returns (is_valid, error_message)
        """
        if not filename:
            return False, "Filename is empty"
        
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        
        if expected_type == 'pdf' and ext != 'pdf':
            return False, f"Expected PDF file, got .{ext}"
        
        if expected_type == 'excel' and ext not in ['xlsx', 'xls']:
            return False, f"Expected Excel file (.xlsx/.xls), got .{ext}"
        
        return True, ""


def country_code_to_name(code: str) -> str:
    """
    Convert country code to full name.
    
    Design Note: This is a fallback mapping. The authoritative mapping
    should come from Settings.
    """
    mapping = {
        'US': 'UNITED STATES',
        'USA': 'UNITED STATES',
        'UK': 'UNITED KINGDOM',
        'GB': 'UNITED KINGDOM',
        'SG': 'SINGAPORE',
        'MY': 'MALAYSIA',
        'VN': 'VIETNAM',
        'ID': 'INDONESIA',
        'PH': 'PHILIPPINES',
        'KR': 'KOREA',
        'JP': 'JAPAN',
        'CN': 'CHINA',
        'DE': 'GERMANY',
        'FR': 'FRANCE',
        'IT': 'ITALY',
        'ES': 'SPAIN',
        'NL': 'NETHERLANDS',
        'CH': 'SWITZERLAND',
        'AU': 'AUSTRALIA',
        'CA': 'CANADA',
        'IL': 'ISRAEL',
        'BG': 'BULGARIA',
    }
    return mapping.get(code.upper(), code.upper())
