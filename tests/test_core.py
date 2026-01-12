"""
Test Suite for MGIS Insurance Declaration Generator

Testing Strategy:
1. Unit tests for each module (parsers, models, generators)
2. Integration tests for pipeline flows
3. Mock API tests for rate limiting
4. Validation tests for data integrity

Run tests with: pytest tests/ -v
"""

import pytest
import pandas as pd
from datetime import date
from io import BytesIO
import json

# Import modules to test
import sys
sys.path.insert(0, '..')

from models.shipment import (
    InboundShipment, OutboundShipment, SAPPDOData,
    TransportMode, DocumentType, ExtractionConfidence,
    ValidationSeverity, ExtractionResult, parse_date_flexible
)
from config.settings import Settings, MappingSettings
from utils.helpers import (
    normalize_tracking_number, normalize_awb_number,
    extract_pdo_numbers, extract_itr_number, RateLimiter
)


# ============================================================================
# Model Tests
# ============================================================================

class TestTransportMode:
    """Tests for TransportMode enum"""
    
    def test_from_string_valid(self):
        assert TransportMode.from_string("COURIER") == TransportMode.COURIER
        assert TransportMode.from_string("AIR") == TransportMode.AIR
        assert TransportMode.from_string("courier") == TransportMode.COURIER
        assert TransportMode.from_string("Air") == TransportMode.AIR
    
    def test_from_string_invalid(self):
        assert TransportMode.from_string("invalid") == TransportMode.UNKNOWN
        assert TransportMode.from_string("") == TransportMode.UNKNOWN
        assert TransportMode.from_string(None) == TransportMode.UNKNOWN


class TestDateParsing:
    """Tests for flexible date parsing"""
    
    def test_courier_format(self):
        """Test DDMMMYY format from courier labels"""
        assert parse_date_flexible("23SEP25") == date(2025, 9, 23)
        assert parse_date_flexible("01JAN25") == date(2025, 1, 1)
        assert parse_date_flexible("29SEP25") == date(2025, 9, 29)
    
    def test_iso_format(self):
        """Test ISO date format"""
        assert parse_date_flexible("2025-09-23") == date(2025, 9, 23)
    
    def test_european_format(self):
        """Test DD/MM/YYYY format"""
        assert parse_date_flexible("23/09/2025") == date(2025, 9, 23)
    
    def test_invalid_date(self):
        """Test that invalid dates return None"""
        assert parse_date_flexible("invalid") is None
        assert parse_date_flexible("") is None
        assert parse_date_flexible(None) is None


class TestInboundShipment:
    """Tests for InboundShipment validation"""
    
    def test_courier_validation_warning(self):
        """Test that COURIER mode with flight info generates warning"""
        shipment = InboundShipment(
            reference="PDO2500444",
            mode=TransportMode.COURIER,
            flight_vessel="TK1314"  # Should warn
        )
        issues = shipment.validate()
        
        # Should have a warning about flight info on COURIER
        assert any(
            i.severity == ValidationSeverity.INFO and "flight" in i.message.lower()
            for i in issues
        )
    
    def test_tracking_validation(self):
        """Test tracking number validation for COURIER"""
        shipment = InboundShipment(
            reference="PDO2500444",
            mode=TransportMode.COURIER,
            tracking_or_awb="ABC123"  # Too few digits
        )
        issues = shipment.validate()
        
        # Should warn about short tracking number
        assert any(
            "digits" in i.message.lower()
            for i in issues
        )
    
    def test_country_splits_validation(self):
        """Test that splits must sum to total"""
        shipment = InboundShipment(
            reference="PDO2500444",
            total_value=1000.0,
            country_splits={'SIN': 400.0, 'MAL': 400.0}  # Only 800, not 1000
        )
        issues = shipment.validate()
        
        # Should have ERROR about split mismatch
        assert any(
            i.severity == ValidationSeverity.ERROR and "split" in i.message.lower()
            for i in issues
        )
    
    def test_valid_shipment_no_errors(self):
        """Test that valid shipment has no ERROR-level issues"""
        shipment = InboundShipment(
            reference="PDO2500444",
            etd_date=date(2025, 9, 23),
            tracking_or_awb="884602373339",  # 12 digits
            mode=TransportMode.COURIER,
            origin_country="UNITED STATES",
            total_value=1000.0,
            country_splits={'SIN': 600.0, 'MAL': 400.0}
        )
        issues = shipment.validate()
        
        assert not shipment.has_errors()


class TestSAPPDOData:
    """Tests for SAP PDO data validation"""
    
    def test_splits_sum_validation(self):
        """Test that country splits must sum to total"""
        pdo = SAPPDOData(
            pdo_number="2500444",
            brands=["NST"],
            currency="USD",
            total_value=1000.0,
            country_splits={'SIN': 600.0, 'MAL': 300.0}  # Only 900
        )
        issues = pdo.validate()
        
        assert any(i.severity == ValidationSeverity.ERROR for i in issues)
    
    def test_valid_pdo_no_errors(self):
        """Test valid PDO has no errors"""
        pdo = SAPPDOData(
            pdo_number="2500444",
            brands=["NST"],
            currency="USD",
            total_value=1000.0,
            country_splits={'SIN': 600.0, 'MAL': 400.0}
        )
        issues = pdo.validate()
        
        assert not any(i.severity == ValidationSeverity.ERROR for i in issues)


# ============================================================================
# Utility Tests
# ============================================================================

class TestTrackingNormalization:
    """Tests for tracking number normalization"""
    
    def test_remove_spaces(self):
        assert normalize_tracking_number("8846 0237 3339") == "884602373339"
    
    def test_remove_dashes(self):
        assert normalize_tracking_number("884-602-373-339") == "884602373339"
    
    def test_already_clean(self):
        assert normalize_tracking_number("884602373339") == "884602373339"
    
    def test_empty(self):
        assert normalize_tracking_number("") == ""
        assert normalize_tracking_number(None) == ""


class TestAWBNormalization:
    """Tests for AWB number normalization"""
    
    def test_format_with_space(self):
        assert normalize_awb_number("235 30462681") == "235-30462681"
    
    def test_already_formatted(self):
        assert normalize_awb_number("235-30462681") == "235-30462681"
    
    def test_no_separator(self):
        assert normalize_awb_number("23530462681") == "235-30462681"


class TestPDOExtraction:
    """Tests for PDO number extraction from filenames"""
    
    def test_single_pdo(self):
        result = extract_pdo_numbers("PDO 2500444_dtd251006_NST.pdf")
        assert "2500444" in result
    
    def test_multiple_pdo_ampersand(self):
        result = extract_pdo_numbers("PDO 2500430 & 2500432_dtd250926_IFC.pdf")
        assert "2500430" in result
        assert "2500432" in result
    
    def test_multiple_pdo_comma(self):
        result = extract_pdo_numbers("PDO2500437,439,440,441_dtd251003_NST.pdf")
        # Should extract base + partials
        assert "2500437" in result


class TestITRExtraction:
    """Tests for ITR/SOM number extraction"""
    
    def test_itr_with_space(self):
        assert extract_itr_number("ITR 2502027_Invoice.pdf") == "ITR 2502027"
    
    def test_itr_no_space(self):
        assert extract_itr_number("ITR2502101") == "ITR 2502101"
    
    def test_som_number(self):
        assert extract_itr_number("SOM 2580125") == "SOM 2580125"
    
    def test_no_match(self):
        assert extract_itr_number("invoice.pdf") is None


class TestRateLimiter:
    """Tests for rate limiter"""
    
    def test_first_call_no_wait(self):
        limiter = RateLimiter(min_delay_seconds=10.0)
        # First call should not wait
        wait_time = limiter.wait()
        # Allow small margin for execution time
        assert wait_time < 0.1
    
    def test_call_count(self):
        limiter = RateLimiter(min_delay_seconds=0.1)
        limiter.wait()
        limiter.wait()
        limiter.wait()
        
        stats = limiter.get_stats()
        assert stats['total_calls'] == 3
    
    def test_reset(self):
        limiter = RateLimiter(min_delay_seconds=0.1)
        limiter.wait()
        limiter.reset()
        
        stats = limiter.get_stats()
        assert stats['total_calls'] == 0


# ============================================================================
# Settings Tests
# ============================================================================

class TestSettings:
    """Tests for Settings configuration"""
    
    def test_default_settings(self):
        settings = Settings()
        
        assert settings.api.delay_seconds == 10
        assert settings.api.max_retries == 3
        assert 'SG' in settings.mappings.country_code_to_column
    
    def test_country_mapping(self):
        settings = Settings()
        
        assert settings.get_country_column('SG') == 'SIN'
        assert settings.get_country_column('MY') == 'MAL'
        assert settings.get_country_column('VN') == 'VIT'
        assert settings.get_country_column('XX') is None
    
    def test_carrier_mode_detection(self):
        settings = Settings()
        
        assert settings.detect_mode_from_carrier("FedEx Express") == "COURIER"
        assert settings.detect_mode_from_carrier("DHL") == "COURIER"
        assert settings.detect_mode_from_carrier("Turkish Airlines") == "AIR"
        assert settings.detect_mode_from_carrier("Unknown Carrier") is None


# ============================================================================
# Integration Tests (Mocked)
# ============================================================================

class TestExtractionResultParsing:
    """Tests for parsing AI extraction results"""
    
    def test_extraction_result_to_dict(self):
        result = ExtractionResult(
            document_type=DocumentType.COURIER_LABEL,
            confidence=ExtractionConfidence.HIGH,
            tracking_or_awb="884602373339",
            ship_date=date(2025, 9, 23),
            mode=TransportMode.COURIER,
            origin_country="UNITED STATES"
        )
        
        d = result.to_dict()
        
        assert d['document_type'] == 'COURIER_LABEL'
        assert d['confidence'] == 'HIGH'
        assert d['mode'] == 'COURIER'
        assert d['ship_date'] == '2025-09-23'
    
    def test_sea_mode_extraction_result(self):
        """Test SEA mode specific fields in ExtractionResult"""
        result = ExtractionResult(
            document_type=DocumentType.BILL_OF_LADING,
            confidence=ExtractionConfidence.MEDIUM,
            tracking_or_awb="MAEU1234567",
            ship_date=date(2025, 10, 15),
            mode=TransportMode.SEA,
            origin_country="CHINA",
            vessel_info="EVER GIVEN / V.123",
            container_number="MSKU1234567"
        )
        
        d = result.to_dict()
        
        assert d['document_type'] == 'BILL_OF_LADING'
        assert d['mode'] == 'SEA'
        assert d['vessel_info'] == "EVER GIVEN / V.123"
        assert d['container_number'] == "MSKU1234567"


class TestTransportModeConfig:
    """Tests for transport mode detection configuration"""
    
    def test_courier_detection(self):
        from config.settings import TransportModeConfig
        
        assert TransportModeConfig.detect_mode("FedEx shipping label") == "COURIER"
        assert TransportModeConfig.detect_mode("DHL Express document") == "COURIER"
        assert TransportModeConfig.detect_mode("UPS tracking") == "COURIER"
    
    def test_air_detection(self):
        from config.settings import TransportModeConfig
        
        assert TransportModeConfig.detect_mode("Air Waybill") == "AIR"
        assert TransportModeConfig.detect_mode("Master AWB document") == "AIR"
    
    def test_sea_detection(self):
        from config.settings import TransportModeConfig
        
        assert TransportModeConfig.detect_mode("Bill of Lading") == "SEA"
        assert TransportModeConfig.detect_mode("Container shipping") == "SEA"
        assert TransportModeConfig.detect_mode("Vessel voyage document") == "SEA"
        assert TransportModeConfig.detect_mode("MAERSK shipping") == "SEA"
    
    def test_mode_supported(self):
        from config.settings import TransportModeConfig
        
        assert TransportModeConfig.is_mode_supported("COURIER")
        assert TransportModeConfig.is_mode_supported("AIR")
        assert TransportModeConfig.is_mode_supported("SEA")


class TestDocumentTypeEnum:
    """Tests for document type enumeration"""
    
    def test_all_document_types_exist(self):
        expected_types = [
            'COURIER_LABEL', 'AIR_WAYBILL', 'BILL_OF_LADING',
            'COMMERCIAL_INVOICE', 'PACKING_LIST', 'CARGO_PERMIT',
            'SHIPMENT_REPORT', 'PURCHASE_ORDER', 'OTHER', 'UNKNOWN'
        ]
        
        for doc_type in expected_types:
            assert hasattr(DocumentType, doc_type), f"Missing DocumentType: {doc_type}"


# ============================================================================
# Product Classifier Tests
# ============================================================================

class TestProductClassifier:
    """Tests for product classification"""
    
    def test_medical_device_profhilo_syringe(self):
        """Profhilo syringe should classify as Medical Devices"""
        from classifiers.product_classifier import classify_description
        result = classify_description("Profhilo 3, 2%, Box 1 Syringe 64mg 2ml")
        assert result == "Medical Devices"
    
    def test_skincare_haenkenium_cream(self):
        """Profhilo Haenkenium Cream should classify as Skincare Products"""
        from classifiers.product_classifier import classify_description
        result = classify_description("Profhilo Haenkenium Cream")
        assert result == "Skincare Products"
    
    def test_mixed_products(self):
        """Mixed products should return multiple categories"""
        from classifiers.product_classifier import classify_description, get_classifier
        from classifiers.product_classifier import ProductCategory
        
        classifier = get_classifier()
        # This description has both syringe (medical device) and cream (skincare)
        # but Haenkenium Cream is specifically skincare
        result = classifier.classify("Profhilo 3, 2%, Box 1 Syringe 64mg 2ml & Profhilo Haenkenium Cream")
        
        # Should have both categories
        assert ProductCategory.MEDICAL_DEVICES in result.categories
        assert ProductCategory.SKINCARE_PRODUCTS in result.categories
    
    def test_unknown_product(self):
        """Unknown products should return Unknown"""
        from classifiers.product_classifier import classify_description
        result = classify_description("Random Product XYZ")
        assert result == "Unknown"
    
    def test_empty_description(self):
        """Empty description should return Unknown"""
        from classifiers.product_classifier import classify_description
        result = classify_description("")
        assert result == "Unknown"
    
    def test_keyword_syringe_medical_device(self):
        """Keyword 'syringe' should trigger Medical Devices"""
        from classifiers.product_classifier import classify_description
        result = classify_description("Injectable Syringe Pack")
        assert result == "Medical Devices"
    
    def test_keyword_cream_skincare(self):
        """Keyword 'cream' should trigger Skincare Products"""
        from classifiers.product_classifier import classify_description
        result = classify_description("Moisturizing Cream 50ml")
        assert result == "Skincare Products"
    
    def test_verbatim_skincare_oral_supplements(self):
        """AWB label 'SKINCARE PRODUCTS & ORAL SUPPLEMENTS' should return both categories"""
        from classifiers.product_classifier import classify_description, get_classifier
        from classifiers.product_classifier import ProductCategory
        
        # Exact AWB label format
        classifier = get_classifier()
        result = classifier.classify("SKINCARE PRODUCTS & ORAL SUPPLEMENTS")
        
        assert ProductCategory.SKINCARE_PRODUCTS in result.categories
        assert ProductCategory.ORAL_SUPPLEMENTS in result.categories
        assert result.confidence >= 0.9  # Should be high confidence for verbatim match
        
        # String version
        result_str = classify_description("SKINCARE PRODUCTS & ORAL SUPPLEMENTS")
        assert "Oral Supplements" in result_str
        assert "Skincare Products" in result_str
    
    def test_verbatim_skincare_only(self):
        """AWB label 'SKINCARE PRODUCTS' should return single category"""
        from classifiers.product_classifier import classify_description
        
        result = classify_description("SKINCARE PRODUCTS")
        assert result == "Skincare Products"
    
    def test_verbatim_medical_devices(self):
        """AWB label 'MEDICAL DEVICES' should return Medical Devices"""
        from classifiers.product_classifier import classify_description
        
        result = classify_description("MEDICAL DEVICES")
        assert result == "Medical Devices"


class TestBrandCodeExtraction:
    """Tests for brand code extraction from Item No. patterns"""
    
    def test_brand_code_validation(self):
        """Test brand code validation - only 3-letter alpha codes"""
        # Simulate what vision_extractor does
        def validate_brand_codes(codes):
            if isinstance(codes, str):
                codes = [b.strip().upper() for b in codes.split(',') if b.strip()]
            elif isinstance(codes, list):
                valid_codes = []
                for code in codes:
                    if isinstance(code, str) and len(code.strip()) == 3 and code.strip().isalpha():
                        valid_codes.append(code.strip().upper())
                codes = list(set(valid_codes))
            else:
                codes = []
            return codes
        
        # Valid codes
        assert validate_brand_codes(['NST', 'HLC', 'END']) == ['END', 'HLC', 'NST'] or \
               set(validate_brand_codes(['NST', 'HLC', 'END'])) == {'NST', 'HLC', 'END'}
        
        # Deduplication
        result = validate_brand_codes(['NST', 'NST', 'NST'])
        assert len(result) == 1
        assert 'NST' in result
        
        # Invalid codes filtered out
        assert validate_brand_codes(['NSTX', '12', 'NS', 'HLC']) == ['HLC']
        
        # Empty handling
        assert validate_brand_codes([]) == []
        assert validate_brand_codes(None) == []
    
    def test_extraction_result_brand_codes(self):
        """Test ExtractionResult includes brand_codes field"""
        from models.shipment import ExtractionResult, DocumentType, ExtractionConfidence
        
        result = ExtractionResult(
            document_type=DocumentType.PURCHASE_ORDER,
            confidence=ExtractionConfidence.HIGH,
            brand_codes=['NST', 'HLC']
        )
        
        assert result.brand_codes == ['NST', 'HLC']
        
        # Empty brand_codes by default
        result2 = ExtractionResult(
            document_type=DocumentType.COURIER_LABEL,
            confidence=ExtractionConfidence.HIGH
        )
        assert result2.brand_codes == []


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
