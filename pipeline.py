"""
Main Processing Pipeline

Orchestrates:
1. SAP parsing
2. PDF processing
3. AI extraction
4. Data aggregation
5. Validation
6. Excel generation

Design Principles:
1. Pipeline stages are clearly separated
2. Progress can be tracked at each stage
3. Partial results are preserved (don't lose work on error)
4. Audit trail maintained throughout
"""

import os
import tempfile
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any, BinaryIO
from pathlib import Path

from config.settings import Settings
from models.shipment import (
    InboundShipment, OutboundShipment, SAPPDOData,
    TransportMode, ExtractionConfidence, ValidationIssue
)
from parsers.sap_parser import SAPParser, match_pdo_to_filename
from extractors.vision_extractor import (
    VisionExtractor, PDFProcessor, DocumentAggregator
)
from generators.excel_generator import ExcelGenerator
from utils.helpers import (
    RateLimiter, AuditTrail, extract_pdo_numbers, extract_itr_number
)
from classifiers.product_classifier import classify_description

logger = logging.getLogger(__name__)


@dataclass
class ProcessingProgress:
    """Tracks processing progress for UI updates"""
    stage: str = ""
    current_item: str = ""
    items_processed: int = 0
    total_items: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def progress_percent(self) -> float:
        if self.total_items == 0:
            return 0.0
        return (self.items_processed / self.total_items) * 100


@dataclass
class PipelineResult:
    """Result of pipeline processing"""
    success: bool
    inbound_shipments: List[InboundShipment] = field(default_factory=list)
    outbound_shipments: List[OutboundShipment] = field(default_factory=list)
    sap_data: Dict[str, SAPPDOData] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time_seconds: float = 0.0


class ProcessingPipeline:
    """
    Main processing pipeline for MGIS Insurance Declaration generation.
    
    Usage:
        pipeline = ProcessingPipeline(settings)
        pipeline.load_sap_files([file1, file2])
        pipeline.process_inbound_pdfs([pdf1, pdf2], progress_callback)
        result = pipeline.get_result()
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.audit = AuditTrail()
        self.rate_limiter = RateLimiter(settings.api.delay_seconds)
        
        # Initialize components
        self.sap_parser = SAPParser(settings)
        self.pdf_processor = PDFProcessor(settings.processing.pdf_zoom_factor)
        self.extractor: Optional[VisionExtractor] = None  # Lazy init (needs API key)
        self.excel_generator = ExcelGenerator(settings)
        
        # State
        self.sap_data: Dict[str, SAPPDOData] = {}
        self.inbound_shipments: List[InboundShipment] = []
        self.outbound_shipments: List[OutboundShipment] = []
        self._start_time: Optional[datetime] = None
    
    def _get_extractor(self) -> VisionExtractor:
        """Lazy initialization of extractor"""
        if self.extractor is None:
            self.extractor = VisionExtractor(self.settings, self.rate_limiter)
        return self.extractor
    
    # =========================================================================
    # Stage 1: SAP Data Loading
    # =========================================================================
    
    def load_sap_files(
        self,
        files: List[BinaryIO],
        progress_callback: Optional[Callable[[ProcessingProgress], None]] = None
    ) -> Dict[str, SAPPDOData]:
        """
        Load and parse SAP export files.
        
        Args:
            files: List of file-like objects (uploaded files)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary of PDO number -> SAPPDOData
        """
        from utils.helpers import FileValidator
        
        progress = ProcessingProgress(stage="SAP Parsing", total_items=len(files))
        
        for file in files:
            filename = getattr(file, 'name', 'unknown')
            progress.current_item = filename
            
            # Validate file before processing
            is_valid, error_msg = FileValidator.validate_excel(file)
            if not is_valid:
                progress.errors.append(f"Invalid file {filename}: {error_msg}")
                progress.items_processed += 1
                if progress_callback:
                    progress_callback(progress)
                continue
            
            try:
                parsed = self.sap_parser.parse_file(file)
                self.sap_data.update(parsed)
                
                # Audit
                for pdo_num, data in parsed.items():
                    self.audit.log_extraction(
                        pdo_num, "sap_data",
                        f"{data.currency} {data.total_value:.2f}",
                        "SAP"
                    )
                
                progress.items_processed += 1
                logger.info(f"Parsed {filename}: {len(parsed)} PDO sheets")
                
            except Exception as e:
                progress.errors.append(f"Failed to parse {filename}: {e}")
                logger.error(f"SAP parse error for {filename}: {e}")
            
            if progress_callback:
                progress_callback(progress)
        
        return self.sap_data
    
    # =========================================================================
    # Stage 2: Inbound PDF Processing
    # =========================================================================
    
    def process_inbound_pdfs(
        self,
        pdf_files: List[Dict[str, Any]],  # [{'name': str, 'path': str}]
        progress_callback: Optional[Callable[[ProcessingProgress], None]] = None
    ) -> List[InboundShipment]:
        """
        Process inbound PDF documents with AI extraction.
        
        Args:
            pdf_files: List of dicts with 'name' and 'path' keys
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of InboundShipment records
        """
        self._start_time = datetime.now()
        extractor = self._get_extractor()
        
        # Count total pages for progress
        total_pages = sum(
            self.pdf_processor.get_page_count(f['path'])
            for f in pdf_files
        )
        
        progress = ProcessingProgress(
            stage="Inbound Extraction",
            total_items=total_pages
        )
        
        for pdf_info in pdf_files:
            filename = pdf_info['name']
            pdf_path = pdf_info['path']
            
            progress.current_item = filename
            
            try:
                # Extract from all pages
                page_count = self.pdf_processor.get_page_count(pdf_path)
                page_results = []
                
                for page_num in range(page_count):
                    # Convert page to image
                    base64_img = self.pdf_processor.page_to_base64(pdf_path, page_num)
                    
                    # Extract with AI
                    result = extractor.extract_from_image(
                        base64_img,
                        prompt_type="inbound",
                        page_number=page_num + 1
                    )
                    page_results.append(result)
                    
                    progress.items_processed += 1
                    if progress_callback:
                        progress_callback(progress)
                    
                    # Log extraction
                    self.audit.log_extraction(
                        filename,
                        f"page_{page_num + 1}",
                        result.document_type.value,
                        "AI"
                    )
                
                # Aggregate results
                aggregated = DocumentAggregator.aggregate_inbound(page_results, filename)
                
                # Match with SAP data
                pdo_matches = match_pdo_to_filename(filename, self.sap_data)
                
                # Create shipment record
                shipment = self._create_inbound_shipment(filename, aggregated, pdo_matches)
                self.inbound_shipments.append(shipment)
                
            except Exception as e:
                progress.errors.append(f"Failed to process {filename}: {e}")
                logger.error(f"Inbound processing error for {filename}: {e}")
        
        return self.inbound_shipments
    
    def _create_inbound_shipment(
        self,
        filename: str,
        aggregated: Dict[str, Any],
        pdo_matches: List
    ) -> InboundShipment:
        """Create InboundShipment from aggregated extraction and SAP data"""
        
        # Build reference from PDO matches or filename
        if pdo_matches:
            reference = ", ".join(f"PDO{pdo[0]}" for pdo in pdo_matches)
        else:
            pdo_nums = extract_pdo_numbers(filename)
            reference = ", ".join(f"PDO{n}" for n in pdo_nums) if pdo_nums else filename
        
        # Get SAP data (combine if multiple matches)
        sap_brands = []
        total_value = 0.0
        currency = None
        country_splits = {}
        
        for _, sap_data in pdo_matches:
            sap_brands.extend(sap_data.brands)
            total_value += sap_data.total_value
            currency = currency or sap_data.currency
            for country, value in sap_data.country_splits.items():
                country_splits[country] = country_splits.get(country, 0) + value
        
        # BRAND CODE PRIORITY:
        # 1. Extracted brand_codes from PURCHASE_ORDER documents (source of truth)
        # 2. Fall back to SAP brands only if no extracted codes exist
        extracted_brands = aggregated.get('brand_codes', [])
        if extracted_brands:
            # Use extracted brand codes - they are explicitly from Item No. column
            brands = extracted_brands
        else:
            # Fall back to SAP brands (but don't infer/guess)
            brands = list(set(sap_brands)) if sap_brands else []
        
        # Flight string
        flight_vessel = " / ".join(aggregated['flight_numbers']) if aggregated['flight_numbers'] else None
        
        return InboundShipment(
            reference=reference,
            etd_date=aggregated.get('ship_date'),
            tracking_or_awb=aggregated.get('tracking_or_awb'),
            incoterms=aggregated.get('incoterms'),
            mode=aggregated.get('mode') or TransportMode.UNKNOWN,
            flight_vessel=flight_vessel,
            origin_country=aggregated.get('origin_country'),
            destination_country="SINGAPORE",
            brands=brands,
            currency=currency,
            total_value=total_value if total_value > 0 else None,
            country_splits=country_splits,
            source_files=[filename],
            extraction_confidence=aggregated.get('confidence', ExtractionConfidence.MEDIUM)
        )
    
    # =========================================================================
    # Stage 3: Outbound PDF Processing
    # =========================================================================
    
    def process_outbound_pdfs(
        self,
        awb_files: List[Dict[str, Any]],
        invoice_files: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[ProcessingProgress], None]] = None
    ) -> List[OutboundShipment]:
        """
        Process outbound AWB and Invoice PDFs.
        
        Processing Strategy:
        1. Process all AWBs first
        2. Process invoices and match to AWBs
        3. Create shipments from unmatched AWBs (AWB-only scenario)
        
        Args:
            awb_files: List of AWB file dicts with 'name' and 'path'
            invoice_files: List of Invoice file dicts with 'name' and 'path'
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of OutboundShipment records
        """
        extractor = self._get_extractor()
        
        total_files = len(awb_files) + len(invoice_files)
        progress = ProcessingProgress(
            stage="Outbound Extraction",
            total_items=total_files
        )
        
        # Process AWBs
        awb_extractions = {}
        matched_awbs = set()  # Track which AWBs have been matched to invoices
        
        for awb_info in awb_files:
            progress.current_item = awb_info['name']
            
            try:
                base64_img = self.pdf_processor.page_to_base64(awb_info['path'], 0)
                result = extractor.extract_from_image(base64_img, "outbound_awb")
                awb_extractions[awb_info['name']] = result
                
            except Exception as e:
                progress.errors.append(f"AWB extraction failed for {awb_info['name']}: {e}")
            
            progress.items_processed += 1
            if progress_callback:
                progress_callback(progress)
        
        # Process Invoices and match with AWBs
        for inv_info in invoice_files:
            progress.current_item = inv_info['name']
            
            try:
                base64_img = self.pdf_processor.page_to_base64(inv_info['path'], 0)
                inv_result = extractor.extract_from_image(base64_img, "outbound_invoice")
                
                # Find matching AWB
                itr_num = extract_itr_number(inv_info['name'])
                matching_awb = None
                matching_awb_name = None
                
                if itr_num:
                    for awb_name, awb_result in awb_extractions.items():
                        if itr_num.replace(' ', '').lower() in awb_name.replace(' ', '').lower():
                            matching_awb = awb_result
                            matching_awb_name = awb_name
                            matched_awbs.add(awb_name)
                            break
                
                # Create outbound shipment
                shipment = self._create_outbound_shipment(
                    inv_info['name'],
                    inv_result,
                    matching_awb
                )
                self.outbound_shipments.append(shipment)
                
            except Exception as e:
                progress.errors.append(f"Invoice extraction failed for {inv_info['name']}: {e}")
            
            progress.items_processed += 1
            if progress_callback:
                progress_callback(progress)
        
        # Create shipments for AWBs without matching invoices
        for awb_name, awb_result in awb_extractions.items():
            if awb_name not in matched_awbs:
                # AWB without invoice - create shipment from AWB only
                shipment = self._create_outbound_shipment(
                    awb_name,
                    None,  # No invoice
                    awb_result
                )
                self.outbound_shipments.append(shipment)
                progress.warnings.append(f"AWB {awb_name} processed without matching invoice")
        
        return self.outbound_shipments
    
    def _create_outbound_shipment(
        self,
        filename: str,
        inv_result,
        awb_result
    ) -> OutboundShipment:
        """
        Create OutboundShipment from extraction results.
        
        Data priority:
        - AWB: flight_numbers, ship_date, tracking_or_awb, description (from notes)
        - Invoice: currency, total_value, destination, description (in notes)
        - Invoice data takes priority over AWB for financial info
        """
        # Initialize data from results
        inv_date = None
        awb_date = None
        flight_vehicle = None
        destination = None
        currency = None
        value = None
        description = None
        
        # Extract AWB data
        if awb_result:
            # Get flight info from flight_numbers list
            if awb_result.flight_numbers:
                flight_vehicle = " / ".join(awb_result.flight_numbers)
            
            # Get date from AWB
            awb_date = awb_result.ship_date
            
            # Get destination from AWB
            if awb_result.destination_country:
                destination = awb_result.destination_country
            
            # Get currency from AWB (if available)
            if awb_result.currency:
                currency = awb_result.currency
            
            # Extract description from AWB notes
            if awb_result.notes and 'Description:' in awb_result.notes:
                desc_match = awb_result.notes.split('Description:')
                if len(desc_match) > 1:
                    # Get the part after "Description:" and before next "|"
                    desc_part = desc_match[1].split('|')[0].strip()
                    description = desc_part
        
        # Extract Invoice data (takes priority for financial info ONLY)
        if inv_result:
            # Invoice date (as fallback only)
            inv_date = inv_result.ship_date
            
            # Currency and value from invoice
            if inv_result.currency:
                currency = inv_result.currency
            if inv_result.total_value:
                value = inv_result.total_value
            
            # Destination from invoice (more detailed)
            if inv_result.destination_country:
                destination = inv_result.destination_country
            
            # Extract description from notes if present (only if AWB description not available)
            if not description and inv_result.notes and 'Description:' in inv_result.notes:
                desc_match = inv_result.notes.split('Description:')
                if len(desc_match) > 1:
                    description = desc_match[1].strip()
        
        # DATE PRIORITY FOR OUTBOUND: AWB "Executed on" date takes priority
        # The AWB date is when the shipment was actually executed/shipped
        # Invoice date is just document issuance date
        ship_date = awb_date or inv_date
        
        # Extract ITR number from filename if not found elsewhere
        itr_number = extract_itr_number(filename)
        
        # Classify the description into product category
        product_category = None
        if description:
            product_category = classify_description(description)
        
        return OutboundShipment(
            invoice_number=itr_number or filename,
            date=ship_date,
            flight_vehicle=flight_vehicle,
            mode=TransportMode.AIR,  # Outbound is typically AIR
            destination=destination,
            description=product_category,  # Use classified category instead of raw description
            currency=currency,
            value=value,
            awb_file=awb_result.raw_response[:100] if awb_result else None,
            invoice_file=filename
        )
    
    # =========================================================================
    # Stage 4: Validation
    # =========================================================================
    
    def validate_all(self) -> Dict[str, List[ValidationIssue]]:
        """
        Validate all shipment records.
        
        Returns:
            Dictionary mapping reference -> list of validation issues
        """
        issues = {}
        
        for shipment in self.inbound_shipments:
            shipment_issues = shipment.validate()
            if shipment_issues:
                issues[shipment.reference] = shipment_issues
                self.audit.log_validation(
                    shipment.reference,
                    [f"{i.severity.value}: {i.message}" for i in shipment_issues]
                )
        
        for shipment in self.outbound_shipments:
            shipment_issues = shipment.validate()
            if shipment_issues:
                issues[shipment.invoice_number] = shipment_issues
                self.audit.log_validation(
                    shipment.invoice_number,
                    [f"{i.severity.value}: {i.message}" for i in shipment_issues]
                )
        
        return issues
    
    # =========================================================================
    # Stage 5: Excel Generation
    # =========================================================================
    
    def generate_excel(self, declaration_period: str) -> bytes:
        """
        Generate the final Excel declaration file.
        
        Args:
            declaration_period: Period string like "October-25"
            
        Returns:
            Excel file as bytes
        """
        buffer = self.excel_generator.generate(
            self.inbound_shipments,
            self.outbound_shipments,
            declaration_period
        )
        
        # Log export
        for shipment in self.inbound_shipments:
            self.audit.log_export(shipment.reference, "Excel")
        for shipment in self.outbound_shipments:
            self.audit.log_export(shipment.invoice_number, "Excel")
        
        return buffer.read()
    
    # =========================================================================
    # Results and State Management
    # =========================================================================
    
    def get_result(self) -> PipelineResult:
        """Get the current pipeline result"""
        elapsed = 0.0
        if self._start_time:
            elapsed = (datetime.now() - self._start_time).total_seconds()
        
        return PipelineResult(
            success=len(self.inbound_shipments) > 0 or len(self.outbound_shipments) > 0,
            inbound_shipments=self.inbound_shipments,
            outbound_shipments=self.outbound_shipments,
            sap_data=self.sap_data,
            processing_time_seconds=elapsed
        )
    
    def update_inbound_shipment(self, index: int, updates: Dict[str, Any]):
        """Update an inbound shipment (user edit)"""
        if 0 <= index < len(self.inbound_shipments):
            shipment = self.inbound_shipments[index]
            
            for field, new_value in updates.items():
                old_value = getattr(shipment, field, None)
                if old_value != new_value:
                    setattr(shipment, field, new_value)
                    shipment.user_modified_fields.append(field)
                    self.audit.log_user_edit(shipment.reference, field, old_value, new_value)
    
    def update_outbound_shipment(self, index: int, updates: Dict[str, Any]):
        """Update an outbound shipment (user edit)"""
        if 0 <= index < len(self.outbound_shipments):
            shipment = self.outbound_shipments[index]
            
            for field, new_value in updates.items():
                old_value = getattr(shipment, field, None)
                if old_value != new_value:
                    setattr(shipment, field, new_value)
                    self.audit.log_user_edit(shipment.invoice_number, field, old_value, new_value)
    
    def get_audit_trail(self):
        """Get the audit trail for export"""
        return self.audit.to_dataframe()
    
    def clear(self):
        """Reset pipeline state"""
        self.sap_data.clear()
        self.inbound_shipments.clear()
        self.outbound_shipments.clear()
        self.audit.clear()
        self._start_time = None
