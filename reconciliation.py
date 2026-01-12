"""
Reconciliation Module

Compares and reconciles data between SAP (source of truth) and extracted documents.
Generates warnings and suggestions for mismatches.

Design Decisions:
1. SAP is ALWAYS the source of truth for values
2. Documents are trusted for shipping info (tracking, dates, flights)
3. Mismatches are logged but don't block processing
4. User is shown clear warnings with reconciliation options

Future Considerations:
- Could add auto-reconciliation rules based on historical patterns
- Could flag persistent extraction issues for prompt tuning
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from enum import Enum
import logging

from models.shipment import (
    InboundShipment, SAPPDOData, ValidationIssue, ValidationSeverity
)

logger = logging.getLogger(__name__)


class ReconciliationType(str, Enum):
    """Types of reconciliation checks"""
    VALUE_MISMATCH = "VALUE_MISMATCH"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    COUNTRY_SPLIT_MISMATCH = "COUNTRY_SPLIT_MISMATCH"
    MISSING_IN_SAP = "MISSING_IN_SAP"
    MISSING_IN_DOCUMENT = "MISSING_IN_DOCUMENT"
    BRAND_MISMATCH = "BRAND_MISMATCH"


@dataclass
class ReconciliationIssue:
    """A single reconciliation issue"""
    issue_type: ReconciliationType
    severity: ValidationSeverity
    field: str
    sap_value: Any
    document_value: Any
    message: str
    suggestion: str
    auto_resolvable: bool = False  # Can be auto-fixed by using SAP value


@dataclass
class ReconciliationResult:
    """Result of reconciling a shipment with SAP data"""
    reference: str
    matched_pdo: Optional[str]
    issues: List[ReconciliationIssue] = field(default_factory=list)
    sap_used: bool = True  # Did we use SAP values?
    
    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0
    
    @property
    def has_errors(self) -> bool:
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)
    
    @property
    def has_warnings(self) -> bool:
        return any(i.severity == ValidationSeverity.WARNING for i in self.issues)
    
    def get_summary(self) -> str:
        if not self.has_issues:
            return "✅ Reconciled successfully"
        
        error_count = sum(1 for i in self.issues if i.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for i in self.issues if i.severity == ValidationSeverity.WARNING)
        
        parts = []
        if error_count:
            parts.append(f"🔴 {error_count} error(s)")
        if warning_count:
            parts.append(f"🟡 {warning_count} warning(s)")
        
        return " | ".join(parts)


class ReconciliationEngine:
    """
    Engine for reconciling extracted data with SAP.
    
    Usage:
        engine = ReconciliationEngine(tolerance_percent=5.0)
        result = engine.reconcile_inbound(shipment, sap_data)
    """
    
    def __init__(
        self,
        value_tolerance_percent: float = 5.0,
        auto_apply_sap_values: bool = True
    ):
        """
        Args:
            value_tolerance_percent: Acceptable % difference in values
            auto_apply_sap_values: If True, automatically use SAP values when mismatched
        """
        self.tolerance = value_tolerance_percent
        self.auto_apply = auto_apply_sap_values
    
    def reconcile_inbound(
        self,
        shipment: InboundShipment,
        sap_data: Dict[str, SAPPDOData],
        auto_apply: bool = None
    ) -> ReconciliationResult:
        """
        Reconcile an inbound shipment against SAP data.
        
        Args:
            shipment: The extracted inbound shipment
            sap_data: Dictionary of PDO number -> SAPPDOData
            auto_apply: Override instance setting for auto-apply
            
        Returns:
            ReconciliationResult with any issues found
        """
        if auto_apply is None:
            auto_apply = self.auto_apply
        
        result = ReconciliationResult(
            reference=shipment.reference,
            matched_pdo=None
        )
        
        # Find matching SAP data
        matched_sap = self._find_matching_sap(shipment.reference, sap_data)
        
        if not matched_sap:
            result.issues.append(ReconciliationIssue(
                issue_type=ReconciliationType.MISSING_IN_SAP,
                severity=ValidationSeverity.WARNING,
                field="reference",
                sap_value=None,
                document_value=shipment.reference,
                message=f"No SAP data found for {shipment.reference}",
                suggestion="Check if correct SAP export was uploaded"
            ))
            result.sap_used = False
            return result
        
        result.matched_pdo = matched_sap.pdo_number
        
        # Check currency match
        if shipment.currency and matched_sap.currency:
            if shipment.currency != matched_sap.currency:
                result.issues.append(ReconciliationIssue(
                    issue_type=ReconciliationType.CURRENCY_MISMATCH,
                    severity=ValidationSeverity.WARNING,
                    field="currency",
                    sap_value=matched_sap.currency,
                    document_value=shipment.currency,
                    message=f"Currency mismatch: SAP={matched_sap.currency}, Doc={shipment.currency}",
                    suggestion="SAP currency will be used",
                    auto_resolvable=True
                ))
                
                if auto_apply:
                    shipment.currency = matched_sap.currency
        
        # Check value match (with tolerance)
        if shipment.total_value and matched_sap.total_value:
            diff_pct = abs(shipment.total_value - matched_sap.total_value) / matched_sap.total_value * 100
            
            if diff_pct > self.tolerance:
                result.issues.append(ReconciliationIssue(
                    issue_type=ReconciliationType.VALUE_MISMATCH,
                    severity=ValidationSeverity.INFO if diff_pct < 10 else ValidationSeverity.WARNING,
                    field="total_value",
                    sap_value=matched_sap.total_value,
                    document_value=shipment.total_value,
                    message=f"Value differs by {diff_pct:.1f}%: SAP={matched_sap.total_value:.2f}, Doc={shipment.total_value:.2f}",
                    suggestion="SAP value will be used (source of truth)",
                    auto_resolvable=True
                ))
                
                if auto_apply:
                    shipment.total_value = matched_sap.total_value
        
        # Apply SAP values for fields we know SAP is authoritative for
        if auto_apply:
            # Currency
            shipment.currency = matched_sap.currency
            
            # Total value
            shipment.total_value = matched_sap.total_value
            
            # Brands
            if matched_sap.brands:
                shipment.brands = matched_sap.brands
            
            # Country splits
            if matched_sap.country_splits:
                shipment.country_splits = matched_sap.country_splits
        
        return result
    
    def _find_matching_sap(
        self,
        reference: str,
        sap_data: Dict[str, SAPPDOData]
    ) -> Optional[SAPPDOData]:
        """Find SAP data matching a shipment reference"""
        import re
        
        # Extract PDO numbers from reference
        pdo_pattern = r'(\d{7})'
        pdo_numbers = re.findall(pdo_pattern, reference)
        
        for pdo_num in pdo_numbers:
            # Try direct match
            for sheet_name, data in sap_data.items():
                if data.pdo_number == pdo_num:
                    return data
                if pdo_num in sheet_name:
                    return data
        
        return None
    
    def reconcile_batch(
        self,
        shipments: List[InboundShipment],
        sap_data: Dict[str, SAPPDOData]
    ) -> Dict[str, ReconciliationResult]:
        """
        Reconcile multiple shipments.
        
        Returns dictionary of reference -> ReconciliationResult
        """
        results = {}
        
        for shipment in shipments:
            result = self.reconcile_inbound(shipment, sap_data)
            results[shipment.reference] = result
        
        return results
    
    def generate_report(
        self,
        results: Dict[str, ReconciliationResult]
    ) -> str:
        """Generate a human-readable reconciliation report"""
        lines = ["=" * 60]
        lines.append("RECONCILIATION REPORT")
        lines.append("=" * 60)
        lines.append("")
        
        total = len(results)
        clean = sum(1 for r in results.values() if not r.has_issues)
        with_warnings = sum(1 for r in results.values() if r.has_warnings and not r.has_errors)
        with_errors = sum(1 for r in results.values() if r.has_errors)
        
        lines.append(f"Total Shipments: {total}")
        lines.append(f"  ✅ Clean: {clean}")
        lines.append(f"  🟡 Warnings: {with_warnings}")
        lines.append(f"  🔴 Errors: {with_errors}")
        lines.append("")
        
        # Detail issues
        for ref, result in results.items():
            if result.has_issues:
                lines.append(f"\n{ref} ({result.get_summary()}):")
                for issue in result.issues:
                    icon = "🔴" if issue.severity == ValidationSeverity.ERROR else "🟡" if issue.severity == ValidationSeverity.WARNING else "ℹ️"
                    lines.append(f"  {icon} {issue.field}: {issue.message}")
                    lines.append(f"      💡 {issue.suggestion}")
        
        return "\n".join(lines)


def merge_sap_into_shipment(
    shipment: InboundShipment,
    sap_data: SAPPDOData,
    overwrite_mode: str = "sap_wins"
) -> List[str]:
    """
    Merge SAP data into a shipment record.
    
    Args:
        shipment: The shipment to update
        sap_data: SAP data to merge
        overwrite_mode: How to handle conflicts
            - "sap_wins": SAP overwrites document values
            - "document_wins": Keep document values
            - "merge": Use SAP for financial, doc for shipping
    
    Returns:
        List of field names that were updated
    """
    updated_fields = []
    
    if overwrite_mode == "sap_wins":
        # SAP overwrites everything it has
        shipment.currency = sap_data.currency
        updated_fields.append('currency')
        
        shipment.total_value = sap_data.total_value
        updated_fields.append('total_value')
        
        shipment.brands = sap_data.brands
        updated_fields.append('brands')
        
        shipment.country_splits = sap_data.country_splits
        updated_fields.append('country_splits')
    
    elif overwrite_mode == "merge":
        # SAP for financial data only
        shipment.currency = sap_data.currency
        shipment.total_value = sap_data.total_value
        shipment.brands = sap_data.brands
        shipment.country_splits = sap_data.country_splits
        updated_fields.extend(['currency', 'total_value', 'brands', 'country_splits'])
    
    # document_wins: Do nothing
    
    return updated_fields
