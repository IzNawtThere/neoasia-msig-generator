"""
Vision Extractor Module

Responsible for:
1. Converting PDF pages to images
2. Calling Claude Vision API
3. Parsing JSON responses into ExtractionResult models

Design Decisions:
1. Single responsibility: just does extraction, doesn't aggregate
2. Rate limiting handled externally by RateLimiter
3. All prompts loaded from external files for easy tuning
4. Raw responses stored for debugging
"""

import fitz  # PyMuPDF
import base64
import json
import re
import logging
from pathlib import Path
from datetime import date
from typing import Optional, List, Dict, Any, BinaryIO

import anthropic

from models.shipment import (
    ExtractionResult, DocumentType, TransportMode, 
    ExtractionConfidence, parse_date_flexible
)
from config.settings import Settings
from utils.helpers import RateLimiter, normalize_tracking_number, normalize_awb_number

logger = logging.getLogger(__name__)


class VisionExtractorError(Exception):
    """Custom exception for extraction errors"""
    pass


class PDFProcessor:
    """
    Converts PDF pages to base64 images for Vision API.
    """
    
    def __init__(self, zoom_factor: float = 2.0):
        self.zoom_factor = zoom_factor
    
    def get_page_count(self, pdf_path: str) -> int:
        """Get the number of pages in a PDF"""
        doc = fitz.open(pdf_path)
        count = len(doc)
        doc.close()
        return count
    
    def page_to_base64(self, pdf_path: str, page_num: int) -> str:
        """
        Convert a single PDF page to base64 PNG.
        
        Args:
            pdf_path: Path to PDF file
            page_num: 0-indexed page number
            
        Returns:
            Base64 encoded PNG string
        """
        doc = fitz.open(pdf_path)
        
        if page_num >= len(doc):
            doc.close()
            raise ValueError(f"Page {page_num} does not exist (max: {len(doc)-1})")
        
        page = doc[page_num]
        mat = fitz.Matrix(self.zoom_factor, self.zoom_factor)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        doc.close()
        
        return base64.standard_b64encode(img_bytes).decode('utf-8')
    
    def all_pages_to_base64(self, pdf_path: str, max_pages: int = 50) -> List[str]:
        """
        Convert all pages of a PDF to base64 images.
        
        Args:
            pdf_path: Path to PDF file
            max_pages: Maximum pages to process (safety limit)
            
        Returns:
            List of base64 encoded PNG strings
        """
        doc = fitz.open(pdf_path)
        pages = []
        
        for i in range(min(len(doc), max_pages)):
            page = doc[i]
            mat = fitz.Matrix(self.zoom_factor, self.zoom_factor)
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("png")
            pages.append(base64.standard_b64encode(img_bytes).decode('utf-8'))
        
        doc.close()
        return pages


class VisionExtractor:
    """
    Extracts information from document images using Claude Vision API.
    
    Usage:
        extractor = VisionExtractor(settings)
        result = extractor.extract_from_image(base64_image, "inbound")
    """
    
    def __init__(self, settings: Settings, rate_limiter: Optional[RateLimiter] = None):
        self.settings = settings
        self.rate_limiter = rate_limiter or RateLimiter(settings.api.delay_seconds)
        self._client: Optional[anthropic.Anthropic] = None
        self._prompts: Dict[str, str] = {}
        self._load_prompts()
    
    def _load_prompts(self):
        """Load extraction prompts from files"""
        prompts_dir = self.settings.prompts_dir
        
        # Inbound prompt
        inbound_path = prompts_dir / "inbound_extraction.txt"
        if inbound_path.exists():
            self._prompts['inbound'] = inbound_path.read_text()
        else:
            logger.warning(f"Inbound prompt not found at {inbound_path}")
            self._prompts['inbound'] = self._get_default_inbound_prompt()
        
        # Outbound AWB prompt (separate file)
        outbound_awb_path = prompts_dir / "outbound_awb.txt"
        if outbound_awb_path.exists():
            self._prompts['outbound_awb'] = outbound_awb_path.read_text()
        else:
            logger.warning(f"Outbound AWB prompt not found at {outbound_awb_path}")
            self._prompts['outbound_awb'] = self._get_default_outbound_awb_prompt()
        
        # Outbound Invoice prompt (separate file)
        outbound_invoice_path = prompts_dir / "outbound_invoice.txt"
        if outbound_invoice_path.exists():
            self._prompts['outbound_invoice'] = outbound_invoice_path.read_text()
        else:
            logger.warning(f"Outbound invoice prompt not found at {outbound_invoice_path}")
            self._prompts['outbound_invoice'] = self._get_default_outbound_invoice_prompt()
    
    def _get_default_inbound_prompt(self) -> str:
        """Fallback prompt if file not found"""
        return """Extract shipping information from this document image.
        Return JSON with: document_type, tracking_or_awb, ship_date (YYYY-MM-DD), 
        mode (COURIER/AIR), flight_numbers [], origin_country, destination_country,
        incoterms, currency, total_value, carrier, confidence (HIGH/MEDIUM/LOW), notes"""
    
    def _get_default_outbound_awb_prompt(self) -> str:
        """Fallback prompt for outbound AWB"""
        return """Extract Air Waybill information from this document.
        Return JSON with: awb_number, flight_info, flight_date (YYYY-MM-DD),
        destination, invoice_reference, confidence (HIGH/MEDIUM/LOW), notes"""
    
    def _get_default_outbound_invoice_prompt(self) -> str:
        """Fallback prompt for outbound invoice"""
        return """Extract Invoice information from this document.
        Return JSON with: invoice_number, date (YYYY-MM-DD), currency,
        total_value, destination_city, destination_country, description,
        confidence (HIGH/MEDIUM/LOW), notes"""
    
    @property
    def client(self) -> anthropic.Anthropic:
        """Lazy initialization of API client"""
        if self._client is None:
            if not self.settings.api.api_key:
                raise VisionExtractorError("API key not configured")
            self._client = anthropic.Anthropic(api_key=self.settings.api.api_key)
        return self._client
    
    def extract_from_image(
        self, 
        base64_image: str, 
        prompt_type: str = "inbound",
        page_number: int = 0
    ) -> ExtractionResult:
        """
        Extract data from a single image.
        
        Args:
            base64_image: Base64 encoded PNG image
            prompt_type: Type of prompt to use ("inbound", "outbound_awb", "outbound_invoice")
            page_number: Page number for tracking
            
        Returns:
            ExtractionResult with extracted data
        """
        # Rate limit
        self.rate_limiter.wait()
        
        prompt = self._prompts.get(prompt_type, self._prompts.get('inbound'))
        
        try:
            response = self.client.messages.create(
                model=self.settings.api.model,
                max_tokens=self.settings.api.max_tokens,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": base64_image
                                }
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            )
            
            raw_response = response.content[0].text
            return self._parse_response(raw_response, page_number, prompt_type)
            
        except anthropic.RateLimitError as e:
            logger.error(f"Rate limit exceeded: {e}")
            return ExtractionResult(
                document_type=DocumentType.UNKNOWN,
                confidence=ExtractionConfidence.LOW,
                page_number=page_number,
                raw_response="",
                extraction_errors=[f"Rate limit exceeded: {e}"]
            )
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return ExtractionResult(
                document_type=DocumentType.UNKNOWN,
                confidence=ExtractionConfidence.LOW,
                page_number=page_number,
                raw_response="",
                extraction_errors=[str(e)]
            )
    
    def _parse_response(self, raw_response: str, page_number: int, prompt_type: str = "inbound") -> ExtractionResult:
        """
        Parse Claude's response into ExtractionResult.
        
        Handles various response formats based on prompt_type:
        - inbound: Standard shipping document extraction
        - outbound_awb: AWB-specific fields (flight_info, flight_date, awb_number)
        - outbound_invoice: Invoice-specific fields (invoice_number, date, etc.)
        """
        errors = []
        
        # Extract JSON from response
        json_match = re.search(r'\{[\s\S]*\}', raw_response)
        if not json_match:
            return ExtractionResult(
                document_type=DocumentType.UNKNOWN,
                confidence=ExtractionConfidence.LOW,
                page_number=page_number,
                raw_response=raw_response,
                extraction_errors=["No JSON found in response"]
            )
        
        try:
            data = json.loads(json_match.group())
        except json.JSONDecodeError as e:
            return ExtractionResult(
                document_type=DocumentType.UNKNOWN,
                confidence=ExtractionConfidence.LOW,
                page_number=page_number,
                raw_response=raw_response,
                extraction_errors=[f"JSON parse error: {e}"]
            )
        
        # Parse confidence (common to all types)
        conf_str = data.get('confidence', 'MEDIUM')
        try:
            confidence = ExtractionConfidence(conf_str)
        except ValueError:
            confidence = ExtractionConfidence.MEDIUM
        
        # Route to appropriate parser based on prompt type
        if prompt_type == "outbound_awb":
            return self._parse_outbound_awb_response(data, raw_response, page_number, confidence, errors)
        elif prompt_type == "outbound_invoice":
            return self._parse_outbound_invoice_response(data, raw_response, page_number, confidence, errors)
        else:
            return self._parse_inbound_response(data, raw_response, page_number, confidence, errors)
    
    def _parse_inbound_response(
        self, data: dict, raw_response: str, page_number: int, 
        confidence: ExtractionConfidence, errors: List[str]
    ) -> ExtractionResult:
        """Parse inbound document extraction response"""
        # Parse document type
        doc_type_str = data.get('document_type', 'UNKNOWN')
        try:
            doc_type = DocumentType(doc_type_str)
        except ValueError:
            doc_type = DocumentType.UNKNOWN
        
        # Parse mode
        mode_str = data.get('mode')
        mode = TransportMode.from_string(mode_str) if mode_str else None
        
        # Parse date
        date_str = data.get('ship_date')
        ship_date = parse_date_flexible(date_str) if date_str else None
        
        # Normalize tracking/AWB
        tracking = data.get('tracking_or_awb')
        if tracking and mode == TransportMode.COURIER:
            tracking = normalize_tracking_number(tracking)
        elif tracking and mode == TransportMode.AIR:
            tracking = normalize_awb_number(tracking)
        
        # Parse flight numbers
        flight_numbers = data.get('flight_numbers', [])
        if isinstance(flight_numbers, str):
            flight_numbers = [f.strip() for f in flight_numbers.split('/') if f.strip()]
        
        # Parse brand codes (from PURCHASE_ORDER documents)
        brand_codes = data.get('brand_codes', [])
        if isinstance(brand_codes, str):
            # Handle comma-separated string
            brand_codes = [b.strip().upper() for b in brand_codes.split(',') if b.strip()]
        elif isinstance(brand_codes, list):
            # Validate and normalize - only accept 3-letter codes
            valid_codes = []
            for code in brand_codes:
                if isinstance(code, str) and len(code.strip()) == 3 and code.strip().isalpha():
                    valid_codes.append(code.strip().upper())
            brand_codes = list(set(valid_codes))  # Deduplicate
        else:
            brand_codes = []
        
        return ExtractionResult(
            document_type=doc_type,
            confidence=confidence,
            tracking_or_awb=tracking,
            ship_date=ship_date,
            mode=mode,
            flight_numbers=flight_numbers,
            origin_country=data.get('origin_country'),
            destination_country=data.get('destination_country'),
            incoterms=data.get('incoterms'),
            currency=data.get('currency'),
            total_value=data.get('total_value'),
            carrier=data.get('carrier'),
            vessel_info=data.get('vessel_info'),
            container_number=data.get('container_number'),
            brand_codes=brand_codes,
            page_number=page_number,
            raw_response=raw_response,
            notes=data.get('notes', ''),
            extraction_errors=errors
        )
    
    def _parse_outbound_awb_response(
        self, data: dict, raw_response: str, page_number: int,
        confidence: ExtractionConfidence, errors: List[str]
    ) -> ExtractionResult:
        """
        Parse outbound AWB extraction response.
        
        AWB responses use different field names:
        - awb_number -> tracking_or_awb
        - flight_number (or flight_info) -> flight_numbers
        - flight_date -> ship_date
        - destination -> destination_country
        - description -> stored in notes (VERBATIM for classification)
        - currency -> currency field
        """
        # Parse AWB number
        awb_number = data.get('awb_number')
        if awb_number:
            awb_number = normalize_awb_number(awb_number)
        
        # Parse flight number - check new field name first, then legacy
        flight_number = data.get('flight_number') or data.get('flight_info')
        flight_numbers = []
        if flight_number:
            # Extract flight numbers from formats like "VN654", "SQ914/09-Sep" or "SQ914 / VN654"
            # Match patterns like SQ914, VN654, etc.
            flight_matches = re.findall(r'[A-Z]{2}\d{3,4}', str(flight_number).upper())
            if flight_matches:
                flight_numbers = flight_matches
            elif flight_number and flight_number.strip():
                # Use as-is if no pattern match but has content
                flight_numbers = [flight_number.strip()]
        
        # Parse flight date (from "Executed on" field primarily)
        date_str = data.get('flight_date')
        ship_date = parse_date_flexible(date_str) if date_str else None
        
        # Parse destination
        destination = data.get('destination')
        
        # Get description VERBATIM for classification
        description = data.get('description', '')
        
        # Build notes with description if available
        notes_parts = []
        if data.get('notes'):
            notes_parts.append(data.get('notes'))
        if description:
            # Store description exactly as extracted for multi-label classification
            notes_parts.append(f"Description: {description}")
        if data.get('invoice_reference'):
            notes_parts.append(f"Invoice: {data.get('invoice_reference')}")
        
        return ExtractionResult(
            document_type=DocumentType.AIR_WAYBILL,
            confidence=confidence,
            tracking_or_awb=awb_number,
            ship_date=ship_date,
            mode=TransportMode.AIR,
            flight_numbers=flight_numbers,
            origin_country="SINGAPORE",  # Outbound always from Singapore
            destination_country=destination,
            currency=data.get('currency'),
            page_number=page_number,
            raw_response=raw_response,
            notes=" | ".join(notes_parts) if notes_parts else "",
            extraction_errors=errors
        )
    
    def _parse_outbound_invoice_response(
        self, data: dict, raw_response: str, page_number: int,
        confidence: ExtractionConfidence, errors: List[str]
    ) -> ExtractionResult:
        """
        Parse outbound invoice extraction response.
        
        Invoice responses include:
        - invoice_number
        - date
        - currency, total_value
        - destination_city, destination_country
        - description
        """
        # Parse date
        date_str = data.get('date')
        ship_date = parse_date_flexible(date_str) if date_str else None
        
        # Parse value
        total_value = data.get('total_value')
        if isinstance(total_value, str):
            # Remove any currency symbols or commas
            total_value = re.sub(r'[^\d.]', '', total_value)
            try:
                total_value = float(total_value)
            except ValueError:
                total_value = None
        
        # Build destination string
        dest_city = data.get('destination_city')
        dest_country = data.get('destination_country')
        destination = None
        if dest_city and dest_country:
            destination = f"{dest_city}, {dest_country}"
        elif dest_country:
            destination = dest_country
        elif dest_city:
            destination = dest_city
        
        return ExtractionResult(
            document_type=DocumentType.COMMERCIAL_INVOICE,
            confidence=confidence,
            tracking_or_awb=data.get('invoice_number'),  # Store invoice number here
            ship_date=ship_date,
            mode=TransportMode.AIR,
            origin_country="SINGAPORE",
            destination_country=destination,
            currency=data.get('currency'),
            total_value=total_value,
            page_number=page_number,
            raw_response=raw_response,
            notes=data.get('notes', '') + f" | Description: {data.get('description', 'N/A')}",
            extraction_errors=errors
        )


class DocumentAggregator:
    """
    Aggregates extraction results from multiple pages into a single shipment record.
    
    Design Decisions:
    1. COURIER_LABEL, AIR_WAYBILL, and BILL_OF_LADING have highest priority for shipping info
    2. COMMERCIAL_INVOICE has priority for incoterms
    3. First valid value wins (don't overwrite with later pages)
    4. Flight numbers and vessel info are merged from all sources
    """
    
    @staticmethod
    def aggregate_inbound(
        results: List[ExtractionResult],
        filename: str
    ) -> Dict[str, Any]:
        """
        Aggregate multiple page extractions into a single inbound record.
        
        Returns dictionary suitable for InboundShipment construction.
        """
        aggregated = {
            'tracking_or_awb': None,
            'ship_date': None,
            'mode': None,
            'flight_numbers': [],
            'vessel_info': None,
            'container_number': None,
            'origin_country': None,
            'incoterms': None,
            'carrier': None,
            'brand_codes': [],  # Brand codes from PURCHASE_ORDER documents
            'confidence': ExtractionConfidence.LOW,
            'raw_responses': [],
            'errors': []
        }
        
        # Priority order for document types (shipping documents)
        priority_types = [
            DocumentType.COURIER_LABEL, 
            DocumentType.AIR_WAYBILL,
            DocumentType.BILL_OF_LADING  # SEA mode support
        ]
        
        for result in results:
            aggregated['raw_responses'].append(result.raw_response)
            aggregated['errors'].extend(result.extraction_errors)
            
            # Update confidence (highest wins)
            if result.confidence.value < aggregated['confidence'].value:
                aggregated['confidence'] = result.confidence
            
            # High priority document types
            is_priority = result.document_type in priority_types
            
            # Tracking/AWB - priority docs first
            if result.tracking_or_awb:
                if is_priority or aggregated['tracking_or_awb'] is None:
                    aggregated['tracking_or_awb'] = result.tracking_or_awb
            
            # Ship date
            if result.ship_date:
                if is_priority or aggregated['ship_date'] is None:
                    aggregated['ship_date'] = result.ship_date
            
            # Mode
            if result.mode and result.mode != TransportMode.UNKNOWN:
                if is_priority or aggregated['mode'] is None:
                    aggregated['mode'] = result.mode
            
            # Carrier
            if result.carrier:
                if is_priority or aggregated['carrier'] is None:
                    aggregated['carrier'] = result.carrier
            
            # Origin country
            if result.origin_country:
                if is_priority or aggregated['origin_country'] is None:
                    aggregated['origin_country'] = result.origin_country
            
            # Incoterms (from any doc, typically invoice)
            if result.incoterms and aggregated['incoterms'] is None:
                aggregated['incoterms'] = result.incoterms
            
            # Flight numbers (merge all - for AIR mode)
            if result.flight_numbers:
                aggregated['flight_numbers'].extend(result.flight_numbers)
            
            # Vessel info (for SEA mode)
            if result.vessel_info and aggregated['vessel_info'] is None:
                aggregated['vessel_info'] = result.vessel_info
            
            # Container number (for SEA mode)
            if result.container_number and aggregated['container_number'] is None:
                aggregated['container_number'] = result.container_number
            
            # Brand codes (from PURCHASE_ORDER documents only)
            if result.brand_codes and result.document_type == DocumentType.PURCHASE_ORDER:
                aggregated['brand_codes'].extend(result.brand_codes)
        
        # De-duplicate flight numbers
        aggregated['flight_numbers'] = list(set(aggregated['flight_numbers']))
        
        # De-duplicate brand codes
        aggregated['brand_codes'] = sorted(list(set(aggregated['brand_codes'])))
        
        # Combine flight/vessel info for unified display
        if aggregated['mode'] == TransportMode.SEA:
            # For SEA mode, use vessel info
            if aggregated['vessel_info']:
                aggregated['flight_vessel'] = aggregated['vessel_info']
                if aggregated['container_number']:
                    aggregated['flight_vessel'] += f" / {aggregated['container_number']}"
        else:
            # For AIR/COURIER, use flight numbers
            if aggregated['flight_numbers']:
                aggregated['flight_vessel'] = " / ".join(aggregated['flight_numbers'])
        
        return aggregated
