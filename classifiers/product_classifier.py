"""
Product Classifier Module

Classifies product descriptions into predefined categories for Marine Insurance declarations.

Categories:
- Medical Devices: Injectable fillers, syringes, medical equipment
- Skincare Products: Creams, serums, lotions, topical products
- Oral Supplements: Vitamins, supplements, capsules, tablets

Design Decisions:
1. Pattern-based matching with brand/product knowledge
2. Supports multi-label classification
3. Extensible category system
4. Returns confidence scores and reasoning
"""

from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)


class ProductCategory(str, Enum):
    """Product categories for insurance declaration"""
    MEDICAL_DEVICES = "Medical Devices"
    SKINCARE_PRODUCTS = "Skincare Products"
    ORAL_SUPPLEMENTS = "Oral Supplements"
    PHARMACEUTICAL = "Pharmaceutical Products"
    UNKNOWN = "Unknown"


@dataclass
class ClassificationResult:
    """Result of product classification"""
    categories: List[ProductCategory]
    confidence: float  # 0.0 to 1.0
    reasoning: str
    matched_patterns: List[str] = field(default_factory=list)
    
    def to_string(self) -> str:
        """Convert to display string for declaration"""
        if not self.categories:
            return "Unknown"
        if len(self.categories) == 1:
            return self.categories[0].value
        # Multiple categories - use "and" separator
        cat_names = [c.value for c in self.categories]
        if len(cat_names) == 2:
            return f"{cat_names[0]} and {cat_names[1]}"
        return ", ".join(cat_names[:-1]) + f" and {cat_names[-1]}"


class ProductClassifier:
    """
    Classifies product descriptions into insurance categories.
    
    Uses a combination of:
    1. Brand name recognition (e.g., "Profhilo" -> Medical Devices)
    2. Keyword patterns (e.g., "syringe" -> Medical Devices)
    3. Product type indicators (e.g., "cream" -> Skincare)
    
    This is designed to be extensible - new brands and patterns can be added
    without code changes by updating the classification rules.
    """
    
    def __init__(self):
        self._init_classification_rules()
    
    def _init_classification_rules(self):
        """Initialize classification rules"""
        
        # VERBATIM LABELS - These are pre-classified labels that appear on AWBs
        # Highest priority - if description exactly or substantially matches, use directly
        self.verbatim_labels: Dict[str, List[ProductCategory]] = {
            # Exact matches from AWB "Nature and Quantity of Goods" field
            'skincare products & oral supplements': [ProductCategory.SKINCARE_PRODUCTS, ProductCategory.ORAL_SUPPLEMENTS],
            'skincare products and oral supplements': [ProductCategory.SKINCARE_PRODUCTS, ProductCategory.ORAL_SUPPLEMENTS],
            'oral supplements & skincare products': [ProductCategory.ORAL_SUPPLEMENTS, ProductCategory.SKINCARE_PRODUCTS],
            'skincare products': [ProductCategory.SKINCARE_PRODUCTS],
            'oral supplements': [ProductCategory.ORAL_SUPPLEMENTS],
            'oral supplement': [ProductCategory.ORAL_SUPPLEMENTS],
            'medical devices': [ProductCategory.MEDICAL_DEVICES],
            'medical device': [ProductCategory.MEDICAL_DEVICES],
            'medical devices & skincare products': [ProductCategory.MEDICAL_DEVICES, ProductCategory.SKINCARE_PRODUCTS],
            'skincare products & medical devices': [ProductCategory.SKINCARE_PRODUCTS, ProductCategory.MEDICAL_DEVICES],
            'pharmaceutical products': [ProductCategory.PHARMACEUTICAL],
        }
        
        # Brand -> Category mapping (case-insensitive)
        # These are known NeoAsia brands/products
        self.brand_categories: Dict[str, ProductCategory] = {
            # Medical Devices (M2 Division)
            'profhilo': ProductCategory.MEDICAL_DEVICES,
            'viscoderm': ProductCategory.MEDICAL_DEVICES,
            'nucleofill': ProductCategory.MEDICAL_DEVICES,
            'aliaxin': ProductCategory.MEDICAL_DEVICES,
            'belotero': ProductCategory.MEDICAL_DEVICES,
            'radiesse': ProductCategory.MEDICAL_DEVICES,
            'juvederm': ProductCategory.MEDICAL_DEVICES,
            'restylane': ProductCategory.MEDICAL_DEVICES,
            'teosyal': ProductCategory.MEDICAL_DEVICES,
            'sculptra': ProductCategory.MEDICAL_DEVICES,
            'ellanse': ProductCategory.MEDICAL_DEVICES,
            'sunekos': ProductCategory.MEDICAL_DEVICES,
            'jalupro': ProductCategory.MEDICAL_DEVICES,
            'lumi eyes': ProductCategory.MEDICAL_DEVICES,
            'ejal': ProductCategory.MEDICAL_DEVICES,
            'xela rederm': ProductCategory.MEDICAL_DEVICES,
            
            # Skincare Products (M1 Division - Skincare)
            'haenkenium': ProductCategory.SKINCARE_PRODUCTS,  # Profhilo Haenkenium is skincare
            'heliocare': ProductCategory.SKINCARE_PRODUCTS,
            'endocare': ProductCategory.SKINCARE_PRODUCTS,
            'neostrata': ProductCategory.SKINCARE_PRODUCTS,
            'isdin': ProductCategory.SKINCARE_PRODUCTS,
            'skinceuticals': ProductCategory.SKINCARE_PRODUCTS,
            'obagi': ProductCategory.SKINCARE_PRODUCTS,
            'zo skin health': ProductCategory.SKINCARE_PRODUCTS,
            'dermaceutic': ProductCategory.SKINCARE_PRODUCTS,
            'biopelle': ProductCategory.SKINCARE_PRODUCTS,
            
            # Oral Supplements (M1 Division - Oral)
            'imedeen': ProductCategory.ORAL_SUPPLEMENTS,
            'perfectil': ProductCategory.ORAL_SUPPLEMENTS,
            'nutrafol': ProductCategory.ORAL_SUPPLEMENTS,
            'viviscal': ProductCategory.ORAL_SUPPLEMENTS,
            'collagen supplements': ProductCategory.ORAL_SUPPLEMENTS,
        }
        
        # Keyword patterns for each category
        # These are ordered by priority - more specific patterns first
        self.keyword_patterns: Dict[ProductCategory, List[str]] = {
            ProductCategory.MEDICAL_DEVICES: [
                r'\bsyringe\b',
                r'\binjectable\b',
                r'\bfiller\b',
                r'\bimplant\b',
                r'\b\d+mg\b.*\bml\b',  # Dosage format like "64mg 2ml"
                r'\bhyaluronic acid\b',
                r'\bbiorevital',
                r'\bskin booster\b',
                r'\bmesotherapy\b',
                r'\bpeel\b',  # Chemical peels are medical devices
                r'\blaser\b',
                r'\bdevice\b',
                r'\bsterile\b',
                r'\bmedical\b',
            ],
            ProductCategory.SKINCARE_PRODUCTS: [
                r'\bcream\b',
                r'\bserum\b',
                r'\blotion\b',
                r'\bmoisturiz',  # moisturizer, moisturizing
                r'\bcleanser\b',
                r'\btoner\b',
                r'\bmask\b',
                r'\bsunscreen\b',
                r'\bspf\b',
                r'\banti.?aging\b',
                r'\bskincare\b',
                r'\bskin\s*care\b',
                r'\btopical\b',
                r'\bcosmetic\b',
            ],
            ProductCategory.ORAL_SUPPLEMENTS: [
                r'\bsupplement\b',
                r'\bcapsule\b',
                r'\btablet\b',
                r'\bvitamin\b',
                r'\bcollagen\b.*\boral\b',
                r'\boral\b.*\bcollagen\b',
                r'\bnutrition\b',
                r'\bdietary\b',
                r'\bpill\b',
                r'\bsoftgel\b',
            ],
            ProductCategory.PHARMACEUTICAL: [
                r'\bdrug\b',
                r'\bpharmaceutical\b',
                r'\bmedicine\b',
                r'\bprescription\b',
            ],
        }
        
        # Special compound patterns (brand + product type combinations)
        self.compound_rules: List[Tuple[str, List[ProductCategory]]] = [
            # "Profhilo Haenkenium Cream" = Skincare (cream overrides Profhilo brand)
            (r'profhilo\s+haenkenium\s+cream', [ProductCategory.SKINCARE_PRODUCTS]),
            # "Profhilo ... Syringe" = Medical Device
            (r'profhilo.*syringe', [ProductCategory.MEDICAL_DEVICES]),
        ]
    
    def classify(self, description: str) -> ClassificationResult:
        """
        Classify a product description into categories.
        
        Args:
            description: Product description text (e.g., from AWB)
            
        Returns:
            ClassificationResult with categories, confidence, and reasoning
        """
        if not description:
            return ClassificationResult(
                categories=[ProductCategory.UNKNOWN],
                confidence=0.0,
                reasoning="No description provided"
            )
        
        desc_lower = description.lower().strip()
        matched_categories: Set[ProductCategory] = set()
        matched_patterns: List[str] = []
        reasoning_parts: List[str] = []
        
        # STEP 0: Check for verbatim labels FIRST (highest priority)
        # AWBs often have pre-classified labels like "SKINCARE PRODUCTS & ORAL SUPPLEMENTS"
        for label, categories in self.verbatim_labels.items():
            if label in desc_lower:
                for cat in categories:
                    matched_categories.add(cat)
                matched_patterns.append(f"verbatim:{label}")
                reasoning_parts.append(f"Verbatim label match: '{label}'")
        
        # If we got a verbatim match, return immediately with high confidence
        if matched_categories:
            return ClassificationResult(
                categories=sorted(list(matched_categories), key=lambda x: x.value),
                confidence=0.95,  # Very high confidence for verbatim matches
                reasoning=" | ".join(reasoning_parts),
                matched_patterns=matched_patterns
            )
        
        # Step 1: Check compound rules first (highest priority for brand-based detection)
        for pattern, categories in self.compound_rules:
            if re.search(pattern, desc_lower):
                for cat in categories:
                    matched_categories.add(cat)
                matched_patterns.append(f"compound:{pattern}")
                reasoning_parts.append(f"Matched compound rule: {pattern}")
        
        # Step 2: Check brand names
        for brand, category in self.brand_categories.items():
            if brand in desc_lower:
                matched_categories.add(category)
                matched_patterns.append(f"brand:{brand}")
                reasoning_parts.append(f"Brand '{brand}' -> {category.value}")
        
        # Step 3: Check keyword patterns
        for category, patterns in self.keyword_patterns.items():
            for pattern in patterns:
                if re.search(pattern, desc_lower):
                    matched_categories.add(category)
                    matched_patterns.append(f"keyword:{pattern}")
                    reasoning_parts.append(f"Keyword '{pattern}' -> {category.value}")
                    break  # One match per category is enough
        
        # Step 4: Handle special cases
        # If we have both "Profhilo" (Medical Device) and "Cream" (Skincare),
        # check if it's ONLY "Haenkenium Cream" (skincare line from Profhilo)
        # BUT if there's also syringe/injectable, keep both categories
        if ProductCategory.MEDICAL_DEVICES in matched_categories and \
           ProductCategory.SKINCARE_PRODUCTS in matched_categories:
            if 'haenkenium cream' in desc_lower:
                # Check if there's a strong medical device indicator (syringe, injectable)
                has_medical_device_keyword = any(
                    p.startswith('keyword:') and 'syringe' in p.lower() 
                    for p in matched_patterns
                ) or any(
                    p.startswith('keyword:') and 'injectable' in p.lower()
                    for p in matched_patterns
                )
                
                if not has_medical_device_keyword:
                    # Only the Profhilo brand is triggering Medical Devices
                    # Remove it since Haenkenium Cream is specifically skincare
                    matched_categories.discard(ProductCategory.MEDICAL_DEVICES)
                    reasoning_parts.append("Haenkenium Cream is skincare, not medical device")
        
        # Calculate confidence
        confidence = self._calculate_confidence(matched_patterns)
        
        # Build result
        if not matched_categories:
            return ClassificationResult(
                categories=[ProductCategory.UNKNOWN],
                confidence=0.2,
                reasoning="No matching patterns found",
                matched_patterns=matched_patterns
            )
        
        return ClassificationResult(
            categories=sorted(list(matched_categories), key=lambda x: x.value),
            confidence=confidence,
            reasoning=" | ".join(reasoning_parts),
            matched_patterns=matched_patterns
        )
    
    def _calculate_confidence(self, matched_patterns: List[str]) -> float:
        """Calculate confidence based on matched patterns"""
        if not matched_patterns:
            return 0.2
        
        # Base confidence
        confidence = 0.5
        
        # Brand matches are high confidence
        brand_matches = sum(1 for p in matched_patterns if p.startswith('brand:'))
        confidence += brand_matches * 0.2
        
        # Keyword matches add moderate confidence
        keyword_matches = sum(1 for p in matched_patterns if p.startswith('keyword:'))
        confidence += keyword_matches * 0.1
        
        # Compound rules are highest confidence
        compound_matches = sum(1 for p in matched_patterns if p.startswith('compound:'))
        confidence += compound_matches * 0.25
        
        return min(confidence, 1.0)
    
    def add_brand(self, brand: str, category: ProductCategory):
        """Add a new brand -> category mapping"""
        self.brand_categories[brand.lower()] = category
        logger.info(f"Added brand mapping: {brand} -> {category.value}")
    
    def add_keyword_pattern(self, category: ProductCategory, pattern: str):
        """Add a new keyword pattern to a category"""
        if category not in self.keyword_patterns:
            self.keyword_patterns[category] = []
        self.keyword_patterns[category].append(pattern)
        logger.info(f"Added keyword pattern: {pattern} -> {category.value}")


# Singleton instance for convenience
_classifier_instance: Optional[ProductClassifier] = None

def get_classifier() -> ProductClassifier:
    """Get the singleton classifier instance"""
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = ProductClassifier()
    return _classifier_instance


def classify_description(description: str) -> str:
    """
    Convenience function to classify a description and return display string.
    
    Args:
        description: Product description text
        
    Returns:
        Category string suitable for declaration (e.g., "Medical Devices")
    """
    classifier = get_classifier()
    result = classifier.classify(description)
    return result.to_string()
