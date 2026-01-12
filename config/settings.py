"""
Configuration Management Module

Design Decisions:
1. All configuration is centralized here with sensible defaults
2. Environment variables override defaults (12-factor app principle)
3. Settings are validated at startup, not at runtime
4. Country mappings and other lookups are configurable for future markets
5. Schema version tracks breaking changes for data compatibility
6. Transport mode registry allows clean extensibility for SEA/TRUCK modes

Future Considerations:
- These settings could be loaded from a database or config service
- The user_inputs section documents what will be hardcoded later
- When SEA mode is needed, add to TRANSPORT_MODE_REGISTRY with detection patterns
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from pathlib import Path
from enum import Enum

# Schema version for data compatibility tracking
# Increment MAJOR for breaking changes, MINOR for additions
SCHEMA_VERSION = "1.0.0"


class TransportModeConfig:
    """
    Extensibility point for transport modes.
    
    Design Decision: Instead of hardcoding mode detection logic,
    we register each mode with its detection patterns. This allows
    adding SEA mode (or others) without touching extraction code.
    
    When SEA support is needed:
    1. Add detection keywords below
    2. Create sea_extraction.txt prompt
    3. No code changes required in extractors
    """
    
    # Registry: mode_name -> {keywords: [], prompt_file: str, document_types: []}
    REGISTRY: Dict[str, Dict[str, Any]] = {
        'COURIER': {
            'keywords': ['fedex', 'dhl', 'ups', 'tnt', 'aramex'],
            'prompt_file': 'inbound_extraction.txt',  # Uses default
            'document_types': ['COURIER_LABEL'],
            'tracking_pattern': r'\d{10,14}',  # 10-14 digits
        },
        'AIR': {
            'keywords': ['air waybill', 'awb', 'mawb', 'hawb', 'airlines', 'cargo'],
            'prompt_file': 'inbound_extraction.txt',  # Uses default
            'document_types': ['AIR_WAYBILL'],
            'tracking_pattern': r'\d{3}-\d{8}',  # XXX-XXXXXXXX
        },
        # SEA mode - Detection framework ready, awaiting real document examples
        # When examples are available:
        # 1. Verify keywords match actual documents
        # 2. Confirm tracking_pattern matches B/L format
        # 3. Create sea_extraction.txt if specialized prompt needed
        'SEA': {
            'keywords': ['bill of lading', 'b/l', 'ocean', 'vessel', 'container', 
                        'sea waybill', 'port of loading', 'port of discharge',
                        'maersk', 'msc', 'evergreen', 'cosco', 'hapag'],
            'prompt_file': 'inbound_extraction.txt',  # Uses default for now
            'document_types': ['BILL_OF_LADING'],
            'tracking_pattern': r'[A-Z]{4}\d{7}',  # Container number format
            'status': 'PENDING_EXAMPLES',  # Flag for future tracking
        },
    }
    
    @classmethod
    def detect_mode(cls, text: str) -> Optional[str]:
        """
        Detect transport mode from text content.
        
        Returns mode name if detected, None otherwise.
        """
        text_lower = text.lower()
        for mode, config in cls.REGISTRY.items():
            for keyword in config['keywords']:
                if keyword in text_lower:
                    return mode
        return None
    
    @classmethod
    def get_prompt_file(cls, mode: str) -> str:
        """Get the prompt file for a transport mode"""
        config = cls.REGISTRY.get(mode, {})
        return config.get('prompt_file', 'inbound_extraction.txt')
    
    @classmethod
    def is_mode_supported(cls, mode: str) -> bool:
        """Check if a mode is currently supported"""
        return mode in cls.REGISTRY


@dataclass
class APISettings:
    """Claude API configuration"""
    api_key: str = ""  # User-entered for now, can be env var later
    model: str = "claude-sonnet-4-20250514"
    delay_seconds: int = 10  # Critical: Prevents rate limiting
    max_retries: int = 3
    timeout_seconds: int = 60
    max_tokens: int = 2000


@dataclass  
class ProcessingSettings:
    """Document processing configuration"""
    pdf_zoom_factor: float = 2.0  # Higher = better quality but slower
    max_pages_per_document: int = 50  # Safety limit
    supported_image_formats: tuple = ("png", "jpg", "jpeg")
    

@dataclass
class ValidationSettings:
    """Data validation thresholds"""
    tracking_number_min_digits: int = 10  # Courier tracking typically 12 digits
    awb_pattern: str = r"^\d{3}-\d{8}$"  # XXX-XXXXXXXX format
    value_tolerance_percent: float = 5.0  # Acceptable SAP vs extracted mismatch
    require_date_for_export: bool = False  # Can export with missing dates
    require_tracking_for_export: bool = False  # Can export with missing tracking


@dataclass
class MappingSettings:
    """
    Lookup tables for code conversions.
    These are the authoritative mappings for the system.
    
    Design Note: Stored as config, not hardcoded, so they can be
    updated without code changes when new countries are added.
    """
    
    # SAP PO Country codes -> Declaration column names
    country_code_to_column: Dict[str, str] = field(default_factory=lambda: {
        'SG': 'SIN',
        'MY': 'MAL',
        'VN': 'VIT',
        'ID': 'Indonesia',
        'PH': 'PH'
    })
    
    # Carrier name detection patterns -> Transport Mode
    carrier_to_mode: Dict[str, str] = field(default_factory=lambda: {
        'fedex': 'COURIER',
        'dhl': 'COURIER', 
        'ups': 'COURIER',
        'tnt': 'COURIER',
        'turkish airlines': 'AIR',
        'singapore airlines': 'AIR',
        'cathay': 'AIR',
        'eva air': 'AIR',
        'lufthansa': 'AIR',
        'emirates': 'AIR',
        # SEA carriers can be added here when examples are available
    })
    
    # Known brand codes (for validation, not extraction)
    known_brands: List[str] = field(default_factory=lambda: [
        'NST', 'EXV', 'CPL', 'COC', 'IFC', 'PIE', 'INM', 'HPT', 
        'VIV', 'QTS', 'GTP', 'DKA'
    ])
    
    # Currency codes we expect to see
    valid_currencies: List[str] = field(default_factory=lambda: [
        'USD', 'EUR', 'SGD', 'MYR', 'PHP', 'IDR', 'VND'
    ])


@dataclass
class OutputSettings:
    """Excel output configuration"""
    # NeoAsia brand colors
    header_bg_color: str = "004d71"
    header_font_color: str = "FFFFFF"
    
    # Default values (only used when explicitly appropriate)
    default_destination: str = "SINGAPORE"
    default_fcl_lcl: str = "LCL"
    
    # Column ordering for output sheets
    inbound_columns: List[str] = field(default_factory=lambda: [
        'ETD DATE', 'BILL OF LADING / AIR WAYBILL', 'Incoterms',
        'Mode of transportation', 'VESSEL / TRUCK # / FLIGHT NO', 
        'VOYAGE FROM', 'TO', 'BRAND', 'DESCRIPTION OF GOODS', 
        'RATE', 'FCL / LCL', 'CURR', 'VALUE OF GOODS', 'COST',
        'Reference Document No.', 'Value (SIN)', 'Value (MAL)',
        'Value (VIT)', 'Value (Indonesia)', 'Value (PH)'
    ])
    
    # Currency section order for outbound sheet
    outbound_currency_order: List[str] = field(default_factory=lambda: [
        'MYR', 'USD', 'IDR', 'PHP', 'SGD', 'EUR'
    ])


@dataclass
class UserInputs:
    """
    Values that are currently user-entered but may be hardcoded later.
    
    Design Decision: These are isolated here so that when the decision
    is made to hardcode them, only this section needs to change.
    The rest of the system treats these as configuration.
    """
    declaration_period: str = ""  # e.g., "October-25"
    default_incoterms: Optional[str] = None  # None = don't default, show blank
    company_name: str = "NeoAsia (S) Pte Ltd"


@dataclass
class Settings:
    """
    Master settings container.
    
    Usage:
        settings = Settings.load()
        settings.api.delay_seconds  # Access nested settings
    """
    api: APISettings = field(default_factory=APISettings)
    processing: ProcessingSettings = field(default_factory=ProcessingSettings)
    validation: ValidationSettings = field(default_factory=ValidationSettings)
    mappings: MappingSettings = field(default_factory=MappingSettings)
    output: OutputSettings = field(default_factory=OutputSettings)
    user_inputs: UserInputs = field(default_factory=UserInputs)
    
    # Paths
    prompts_dir: Path = field(default_factory=lambda: Path(__file__).parent / "prompts")
    
    @classmethod
    def load(cls, api_key: str = "", declaration_period: str = "") -> 'Settings':
        """
        Factory method to create settings with overrides.
        
        Future: This could load from .env, database, or config service.
        """
        settings = cls()
        
        # Override from environment if available
        settings.api.api_key = api_key or os.environ.get('ANTHROPIC_API_KEY', '')
        settings.user_inputs.declaration_period = declaration_period
        
        # Ensure prompts directory exists and has required files
        settings._ensure_prompts_directory()
        
        return settings
    
    def _ensure_prompts_directory(self):
        """
        Ensure prompts directory exists with required prompt files.
        Creates default prompts if missing.
        """
        if not self.prompts_dir.exists():
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
        
        # Check for required prompt files
        required_prompts = ['inbound_extraction.txt', 'outbound_awb.txt', 'outbound_invoice.txt']
        
        for prompt_file in required_prompts:
            prompt_path = self.prompts_dir / prompt_file
            if not prompt_path.exists():
                # Create with embedded default (fallback)
                default_content = self._get_default_prompt(prompt_file)
                if default_content:
                    prompt_path.write_text(default_content)
    
    def _get_default_prompt(self, prompt_name: str) -> Optional[str]:
        """Get default prompt content for fallback"""
        defaults = {
            'inbound_extraction.txt': INBOUND_PROMPT_DEFAULT,
            'outbound_awb.txt': OUTBOUND_AWB_PROMPT_DEFAULT,
            'outbound_invoice.txt': OUTBOUND_INVOICE_PROMPT_DEFAULT,
        }
        return defaults.get(prompt_name)
    
    def validate(self) -> List[str]:
        """
        Validate settings and return list of issues.
        
        Returns empty list if all settings are valid.
        """
        issues = []
        
        if not self.api.api_key:
            issues.append("API key is required")
        
        if self.api.delay_seconds < 5:
            issues.append("API delay should be at least 5 seconds to avoid rate limits")
        
        if not self.prompts_dir.exists():
            issues.append(f"Prompts directory does not exist: {self.prompts_dir}")
        
        return issues
    
    def get_country_column(self, country_code: str) -> Optional[str]:
        """Safe lookup for country code mapping"""
        return self.mappings.country_code_to_column.get(country_code.upper())
    
    def detect_mode_from_carrier(self, carrier_name: str) -> Optional[str]:
        """Detect transport mode from carrier name"""
        if not carrier_name:
            return None
        carrier_lower = carrier_name.lower()
        for pattern, mode in self.mappings.carrier_to_mode.items():
            if pattern in carrier_lower:
                return mode
        return None


# =============================================================================
# Default Prompt Templates (Fallbacks)
# =============================================================================
# These are embedded in code as fallbacks. The actual prompts are loaded from
# files in config/prompts/ which can be edited without code changes.

INBOUND_PROMPT_DEFAULT = """You are analyzing a shipping document image for NeoAsia (S) Pte Ltd.

TASK: Extract shipping information. Identify document type first, then extract fields.

DOCUMENT TYPES:
- COURIER_LABEL: FedEx/DHL/UPS label with "TRK#" field
- AIR_WAYBILL: "Air Waybill"/"AWB"/"MAWB"/"HAWB" with XXX-XXXXXXXX format
- COMMERCIAL_INVOICE: Invoice with incoterms/values
- OTHER: Any other document

RESPONSE FORMAT (JSON only):
{
    "document_type": "COURIER_LABEL|AIR_WAYBILL|COMMERCIAL_INVOICE|OTHER",
    "tracking_or_awb": "number or null",
    "ship_date": "YYYY-MM-DD or null",
    "mode": "COURIER|AIR|null",
    "flight_numbers": [],
    "origin_country": "FULL NAME or null",
    "incoterms": "EXW|FOB|CIF|etc or null",
    "currency": "USD|EUR|etc or null",
    "total_value": number or null,
    "carrier": "name or null",
    "confidence": "HIGH|MEDIUM|LOW",
    "notes": "observations"
}

RULES:
1. For COURIER: tracking is 12+ digits from "TRK#" field, NOT alphanumeric codes
2. For AIR: AWB format XXX-XXXXXXXX
3. Return null for uncertain fields, never guess
4. Respond with valid JSON only"""

OUTBOUND_AWB_PROMPT_DEFAULT = """Extract from Air Waybill:

RESPONSE FORMAT (JSON only):
{
    "awb_number": "XXX-XXXXXXXX or null",
    "flight_info": "flight numbers or null",
    "flight_date": "YYYY-MM-DD or null",
    "destination": "city, country or null",
    "invoice_reference": "ITR/SOM number if visible or null",
    "confidence": "HIGH|MEDIUM|LOW",
    "notes": "observations"
}

Respond with valid JSON only."""

OUTBOUND_INVOICE_PROMPT_DEFAULT = """Extract from Invoice:

RESPONSE FORMAT (JSON only):
{
    "invoice_number": "ITR/SOM + digits or null",
    "date": "YYYY-MM-DD or null",
    "currency": "USD|MYR|PHP|IDR|SGD|EUR|null",
    "total_value": number or null,
    "destination_city": "city or null",
    "destination_country": "country or null",
    "description": "goods description or null",
    "confidence": "HIGH|MEDIUM|LOW",
    "notes": "observations"
}

Respond with valid JSON only."""
