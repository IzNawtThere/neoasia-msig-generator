"""
Domain Models Module

Design Principles:
1. Models represent the business domain, not the UI or storage format
2. All validation happens at model construction time
3. Models are immutable after creation (dataclass frozen=True)
4. Clear distinction between required and optional fields
5. Explicit handling of "unknown" vs "not provided" vs "not applicable"

Architecture Note:
These models serve as the contract between:
- Extractors (produce models)
- Validators (check models)
- Generators (consume models)
- UI (display/edit models)
"""

from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import re


class TransportMode(str, Enum):
    """
    Transport modes for shipments.
    
    Design Decision: Using Enum ensures type safety and prevents
    typos like "COURIR" or "Air" from propagating through the system.
    
    UNKNOWN is explicitly included for cases where we cannot determine
    the mode - this is different from None (not yet processed).
    """
    COURIER = "COURIER"  # FedEx, DHL, UPS, TNT
    AIR = "AIR"          # Air freight with AWB
    SEA = "SEA"          # Sea freight (future - no examples yet)
    TRUCK = "TRUCK"      # Ground transport (for outbound to Malaysia)
    UNKNOWN = "UNKNOWN"  # Could not determine from documents
    
    @classmethod
    def from_string(cls, value: str) -> 'TransportMode':
        """Safe conversion from string with fallback to UNKNOWN"""
        if not value:
            return cls.UNKNOWN
        try:
            return cls(value.upper())
        except ValueError:
            return cls.UNKNOWN


class DocumentType(str, Enum):
    """
    Types of documents we can encounter in PDF packages.
    
    Design Decision: Explicit enumeration allows us to:
    1. Track which document types we support
    2. Route to appropriate extractors
    3. Log unsupported types for future development
    """
    COURIER_LABEL = "COURIER_LABEL"      # FedEx/DHL/UPS shipping label
    AIR_WAYBILL = "AIR_WAYBILL"          # MAWB/HAWB documents
    COMMERCIAL_INVOICE = "COMMERCIAL_INVOICE"
    PACKING_LIST = "PACKING_LIST"
    CARGO_PERMIT = "CARGO_PERMIT"        # Singapore customs clearance
    SHIPMENT_REPORT = "SHIPMENT_REPORT"  # Internal NeoAsia form
    PURCHASE_ORDER = "PURCHASE_ORDER"    # Supplier PO
    BILL_OF_LADING = "BILL_OF_LADING"    # For SEA shipments (future)
    OTHER = "OTHER"                      # Unclassified
    UNKNOWN = "UNKNOWN"                  # Could not classify


class ExtractionConfidence(str, Enum):
    """
    Confidence level of AI extraction.
    
    Used to flag records that need human review.
    """
    HIGH = "HIGH"      # Clear document, high certainty
    MEDIUM = "MEDIUM"  # Some ambiguity, should verify
    LOW = "LOW"        # Significant uncertainty, requires review


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues"""
    ERROR = "ERROR"      # Must be fixed before export
    WARNING = "WARNING"  # Should review but can proceed
    INFO = "INFO"        # Informational only


@dataclass
class ValidationIssue:
    """A single validation issue"""
    severity: ValidationSeverity
    field: str
    message: str
    suggestion: Optional[str] = None


@dataclass
class ExtractionResult:
    """
    Result of extracting data from a single document page.
    
    Design Note: This is the raw extraction output before aggregation.
    Multiple ExtractionResults are combined into a ShipmentRecord.
    """
    document_type: DocumentType
    confidence: ExtractionConfidence
    
    # Extracted fields (all optional at this stage)
    tracking_or_awb: Optional[str] = None
    ship_date: Optional[date] = None
    mode: Optional[TransportMode] = None
    flight_numbers: List[str] = field(default_factory=list)
    origin_country: Optional[str] = None
    destination_country: Optional[str] = None
    incoterms: Optional[str] = None
    currency: Optional[str] = None
    total_value: Optional[float] = None
    carrier: Optional[str] = None
    
    # SEA mode specific fields
    vessel_info: Optional[str] = None     # Vessel name / voyage number
    container_number: Optional[str] = None  # Container ID (format: XXXX1234567)
    
    # Brand codes (from PURCHASE_ORDER Item No. column)
    brand_codes: List[str] = field(default_factory=list)
    
    # Metadata
    page_number: int = 0
    raw_response: str = ""  # For debugging
    notes: str = ""         # AI observations
    extraction_errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        result = asdict(self)
        result['document_type'] = self.document_type.value
        result['confidence'] = self.confidence.value
        result['mode'] = self.mode.value if self.mode else None
        result['ship_date'] = self.ship_date.isoformat() if self.ship_date else None
        return result


@dataclass
class SAPPDOData:
    """
    Data extracted from SAP Export Excel file.
    
    This is the SOURCE OF TRUTH for:
    - Brand information
    - Total values
    - Currency
    - Country splits
    
    Design Decision: SAP data is authoritative. If there's a mismatch
    between SAP and extracted document values, SAP wins (with a warning).
    """
    pdo_number: str
    brands: List[str]
    currency: str
    total_value: float
    country_splits: Dict[str, float]  # Column name -> value
    
    # Metadata
    source_file: str = ""
    sheet_name: str = ""
    row_count: int = 0
    
    def validate(self) -> List[ValidationIssue]:
        """Self-validation of SAP data"""
        issues = []
        
        # Check splits sum to total
        if self.country_splits:
            splits_sum = sum(self.country_splits.values())
            if abs(splits_sum - self.total_value) > 0.01:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="country_splits",
                    message=f"Splits ({splits_sum:.2f}) don't sum to total ({self.total_value:.2f})",
                    suggestion="Check SAP export for missing rows"
                ))
        
        # Check for empty brands
        if not self.brands:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="brands",
                message="No brand information found",
                suggestion="Brand column may be missing or empty"
            ))
        
        return issues


@dataclass
class InboundShipment:
    """
    Complete inbound shipment record ready for declaration.
    
    This combines:
    - AI-extracted shipping document data
    - SAP PDO data (source of truth for values)
    - User corrections (tracked separately)
    
    Design Decision: Fields use Optional to distinguish between:
    - None: Not yet extracted / not applicable
    - Empty string: Explicitly empty (user cleared it)
    - Value: Has data
    """
    # Identity
    reference: str  # PDO number(s) - primary key
    
    # Shipping Information (from documents)
    etd_date: Optional[date] = None
    tracking_or_awb: Optional[str] = None
    incoterms: Optional[str] = None
    mode: TransportMode = TransportMode.UNKNOWN
    flight_vessel: Optional[str] = None
    origin_country: Optional[str] = None
    destination_country: str = "SINGAPORE"
    
    # Product Information (from SAP)
    brands: List[str] = field(default_factory=list)
    description: Optional[str] = None
    
    # Financial Information (from SAP - source of truth)
    currency: Optional[str] = None
    total_value: Optional[float] = None
    country_splits: Dict[str, float] = field(default_factory=dict)
    
    # Processing Metadata
    source_files: List[str] = field(default_factory=list)
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    user_modified_fields: List[str] = field(default_factory=list)
    
    def get_brand_string(self) -> str:
        """Format brands for display"""
        return ", ".join(self.brands) if self.brands else ""
    
    def get_flight_string(self) -> str:
        """Format flight/vessel for display"""
        return self.flight_vessel or ""
    
    def validate(self) -> List[ValidationIssue]:
        """
        Validate the shipment record.
        
        Returns list of issues (empty list = valid).
        """
        issues = []
        
        # Required fields for export (warnings, not errors)
        if not self.tracking_or_awb:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="tracking_or_awb",
                message="Missing tracking number or AWB",
                suggestion="Check shipping label or air waybill"
            ))
        
        if not self.etd_date:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="etd_date",
                message="Missing ship date",
                suggestion="Check shipping label date field"
            ))
        
        # Mode consistency checks
        if self.mode == TransportMode.COURIER:
            if self.flight_vessel:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    field="flight_vessel",
                    message="COURIER mode typically doesn't have flight info",
                    suggestion="Verify mode is correct"
                ))
            # Validate tracking format for courier
            if self.tracking_or_awb:
                digit_count = sum(c.isdigit() for c in self.tracking_or_awb)
                if digit_count < 10:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        field="tracking_or_awb",
                        message=f"Tracking number has only {digit_count} digits (expected 12+)",
                        suggestion="Verify this is the tracking number, not a reference code"
                    ))
        
        elif self.mode == TransportMode.AIR:
            if not self.flight_vessel:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field="flight_vessel",
                    message="AIR mode typically should have flight number",
                    suggestion="Check air waybill for flight info"
                ))
        
        # Value checks
        if self.country_splits and self.total_value:
            splits_sum = sum(self.country_splits.values())
            if abs(splits_sum - self.total_value) > 0.01:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="country_splits",
                    message=f"Country splits ({splits_sum:.2f}) don't match total ({self.total_value:.2f})",
                    suggestion="Verify SAP data or manually adjust splits"
                ))
        
        self.validation_issues = issues
        return issues
    
    def has_errors(self) -> bool:
        """Check if there are any ERROR-level validation issues"""
        return any(i.severity == ValidationSeverity.ERROR for i in self.validation_issues)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for UI/serialization"""
        return {
            'reference': self.reference,
            'etd_date': self.etd_date.isoformat() if self.etd_date else None,
            'tracking_or_awb': self.tracking_or_awb,
            'incoterms': self.incoterms,
            'mode': self.mode.value,
            'flight_vessel': self.flight_vessel,
            'origin_country': self.origin_country,
            'destination_country': self.destination_country,
            'brands': self.get_brand_string(),
            'currency': self.currency,
            'total_value': self.total_value,
            **{k: v for k, v in self.country_splits.items()}
        }


@dataclass
class OutboundShipment:
    """
    Outbound shipment record for declaration.
    
    Design Note: Outbound is simpler than inbound because:
    1. Single source documents (AWB + Invoice pair)
    2. Currency determines the section in output
    3. Less complex routing (always FROM Singapore)
    """
    # Identity
    invoice_number: str  # ITR or SOM number
    
    # Shipping Information
    date: Optional[date] = None
    flight_vehicle: Optional[str] = None
    mode: TransportMode = TransportMode.AIR
    destination: Optional[str] = None
    
    # Product Information
    description: Optional[str] = None
    
    # Financial Information
    currency: Optional[str] = None
    value: Optional[float] = None
    
    # Constants for Singapore-origin shipments
    origin: str = "SINGAPORE"
    fcl_lcl: str = "LCL"
    
    # Metadata
    awb_file: Optional[str] = None
    invoice_file: Optional[str] = None
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    
    def validate(self) -> List[ValidationIssue]:
        """Validate outbound shipment"""
        issues = []
        
        if not self.invoice_number:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="invoice_number",
                message="Missing invoice number",
                suggestion="Check invoice document"
            ))
        
        if not self.currency:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="currency",
                message="Missing currency",
                suggestion="Check invoice for currency"
            ))
        
        if not self.value or self.value <= 0:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="value",
                message="Missing or invalid value",
                suggestion="Check invoice total"
            ))
        
        self.validation_issues = issues
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for UI/serialization"""
        return {
            'invoice_number': self.invoice_number,
            'date': self.date.isoformat() if self.date else None,
            'flight_vehicle': self.flight_vehicle,
            'mode': self.mode.value,
            'origin': self.origin,
            'destination': self.destination,
            'description': self.description,
            'currency': self.currency,
            'value': self.value
        }


@dataclass
class AuditEntry:
    """
    Single audit log entry for tracking changes.
    
    Design Decision: Full audit trail enables:
    1. Debugging extraction issues
    2. Understanding user corrections
    3. Compliance/traceability requirements
    """
    timestamp: datetime
    action: str  # EXTRACTED, USER_EDIT, VALIDATED, EXPORTED
    record_reference: str
    field_name: Optional[str]
    old_value: Any
    new_value: Any
    source: str  # AI, SAP, USER
    notes: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'action': self.action,
            'record_reference': self.record_reference,
            'field_name': self.field_name,
            'old_value': str(self.old_value) if self.old_value is not None else None,
            'new_value': str(self.new_value) if self.new_value is not None else None,
            'source': self.source,
            'notes': self.notes
        }


# Utility functions for date parsing
def parse_date_flexible(date_str: str) -> Optional[date]:
    """
    Parse dates from various formats found in shipping documents.
    
    Supported formats:
    - 23SEP25, 29SEP25 (courier labels)
    - 2025-09-23 (ISO)
    - 23/09/2025, 23-09-2025 (European)
    - 09/23/2025 (US)
    - 23-Sep-25, 23 Sep 2025
    
    Returns None if parsing fails (don't guess).
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Common patterns to try
    patterns = [
        (r'(\d{1,2})([A-Z]{3})(\d{2,4})', '%d%b%y'),  # 23SEP25
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),     # 2025-09-23
        (r'(\d{2})/(\d{2})/(\d{4})', '%d/%m/%Y'),     # 23/09/2025
        (r'(\d{2})-(\d{2})-(\d{4})', '%d-%m-%Y'),     # 23-09-2025
        (r'(\d{1,2})-([A-Za-z]{3})-(\d{2,4})', '%d-%b-%y'),  # 23-Sep-25
    ]
    
    # Try format DDMMMYY (23SEP25)
    match = re.match(r'(\d{1,2})([A-Z]{3})(\d{2})', date_str.upper())
    if match:
        try:
            day, month, year = match.groups()
            year_full = 2000 + int(year)
            month_map = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            if month in month_map:
                return date(year_full, month_map[month], int(day))
        except (ValueError, KeyError):
            pass
    
    # Try ISO format
    try:
        return datetime.fromisoformat(date_str).date()
    except ValueError:
        pass
    
    # Try other common formats
    for fmt in ['%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y', '%d %b %Y', '%d-%b-%Y']:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None
