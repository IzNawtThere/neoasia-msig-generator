"""
SAP Export Parser Module

Responsible for:
1. Reading SAP Export Excel files
2. Extracting PDO data (Brand, Values, Country splits)
3. Validating data integrity

Design Decisions:
1. SAP is the SOURCE OF TRUTH for financial data
2. Parser handles multi-table sheets (main data + batch info)
3. Returns validated SAPPDOData models

Known Data Patterns in SAP Exports:
- Each sheet = one PDO
- Sheet name = PDO number (e.g., "PDO2500453")
- First table: Line items with Brand, Total, PO Country
- Second table (if present): Batch/lot information (ignore for declaration)
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Optional, BinaryIO, Tuple
from pathlib import Path

from models.shipment import SAPPDOData, ValidationIssue, ValidationSeverity
from config.settings import Settings

logger = logging.getLogger(__name__)


class SAPParserError(Exception):
    """Custom exception for SAP parsing errors"""
    pass


class SAPParser:
    """
    Parser for SAP Export Excel files.
    
    Usage:
        parser = SAPParser(settings)
        pdo_data = parser.parse_file(uploaded_file)
    """
    
    # Values that should be ignored when found in Brand column
    # (these appear in secondary header rows)
    INVALID_BRAND_VALUES = frozenset({
        'Location', 'Brand', 'System Number', 'nan', 'NaN', '', 
        'Status', 'Item No.', 'Batch', 'Whse'
    })
    
    # Column name patterns to identify relevant columns
    COLUMN_PATTERNS = {
        'item_no': ['Item No.', 'Item Number', 'ItemNo'],
        'brand': ['Brand'],
        'total': ['Total (Doc)', 'Total (Document)', 'Total'],
        'po_country': ['PO Country', 'POCountry', 'PO_Country'],
        'currency': ['Currency', 'Curr'],
    }
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.country_mapping = settings.mappings.country_code_to_column
    
    def parse_file(self, file_source: BinaryIO) -> Dict[str, SAPPDOData]:
        """
        Parse SAP Export Excel file.
        
        Args:
            file_source: File-like object (uploaded file or path)
            
        Returns:
            Dictionary mapping PDO number to SAPPDOData
        """
        try:
            xl = pd.ExcelFile(file_source)
        except Exception as e:
            raise SAPParserError(f"Failed to open Excel file: {e}")
        
        results = {}
        
        for sheet_name in xl.sheet_names:
            try:
                pdo_data = self._parse_sheet(xl, sheet_name, str(getattr(file_source, 'name', 'unknown')))
                if pdo_data:
                    results[sheet_name] = pdo_data
                    logger.info(f"Parsed {sheet_name}: {pdo_data.currency} {pdo_data.total_value:.2f}")
            except Exception as e:
                logger.warning(f"Failed to parse sheet {sheet_name}: {e}")
                continue
        
        return results
    
    def _parse_sheet(self, xl: pd.ExcelFile, sheet_name: str, source_file: str) -> Optional[SAPPDOData]:
        """
        Parse a single sheet from the Excel file.
        
        Strategy:
        1. Read sheet without header to find header row
        2. Identify header row by looking for 'Item No.' column
        3. Re-read with proper header
        4. Extract data row by row, filtering out secondary tables
        """
        # Read raw to find structure
        df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        
        # Find header row
        header_row = self._find_header_row(df_raw)
        if header_row is None:
            logger.debug(f"No valid header found in {sheet_name}")
            return None
        
        # Re-read with header
        df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)
        
        # Map columns to our expected names
        column_map = self._map_columns(df.columns)
        
        if 'total' not in column_map:
            logger.warning(f"No Total column found in {sheet_name}")
            return None
        
        # Extract data
        brands = set()
        currency = None
        total_value = 0.0
        country_splits = {}
        row_count = 0
        
        for idx, row in df.iterrows():
            # Check if this is a valid data row
            item_no = self._get_cell_value(row, column_map.get('item_no'))
            if not item_no or item_no in self.INVALID_BRAND_VALUES:
                continue
            
            # Extract total value and currency
            total_str = self._get_cell_value(row, column_map.get('total'))
            if total_str:
                parsed = self._parse_currency_value(total_str)
                if parsed:
                    curr, amount = parsed
                    if currency is None:
                        currency = curr
                    total_value += amount
                    row_count += 1
                    
                    # Country split
                    country_code = self._get_cell_value(row, column_map.get('po_country'))
                    if country_code:
                        column_name = self.country_mapping.get(country_code.upper())
                        if column_name:
                            country_splits[column_name] = country_splits.get(column_name, 0) + amount
            
            # Extract brand
            brand = self._get_cell_value(row, column_map.get('brand'))
            if brand and brand not in self.INVALID_BRAND_VALUES:
                brands.add(brand)
        
        if total_value == 0:
            logger.debug(f"No valid data rows in {sheet_name}")
            return None
        
        # Extract PDO number from sheet name
        pdo_match = re.search(r'(\d{7})', sheet_name)
        pdo_number = pdo_match.group(1) if pdo_match else sheet_name
        
        return SAPPDOData(
            pdo_number=pdo_number,
            brands=list(brands),
            currency=currency or 'USD',
            total_value=total_value,
            country_splits=country_splits,
            source_file=source_file,
            sheet_name=sheet_name,
            row_count=row_count
        )
    
    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """Find the row containing column headers"""
        for idx, row in df.iterrows():
            row_values = [str(v).strip() for v in row.values if pd.notna(v)]
            if 'Item No.' in row_values:
                return idx
        return None
    
    def _map_columns(self, columns: pd.Index) -> Dict[str, str]:
        """
        Map actual column names to our expected names.
        
        Returns dict like {'total': 'Total (Doc)', 'brand': 'Brand'}
        """
        result = {}
        
        for col in columns:
            col_str = str(col).strip()
            
            for key, patterns in self.COLUMN_PATTERNS.items():
                if any(p.lower() == col_str.lower() or p in col_str for p in patterns):
                    result[key] = col
                    break
        
        return result
    
    def _get_cell_value(self, row: pd.Series, column: Optional[str]) -> Optional[str]:
        """Safely get a cell value as string"""
        if column is None or column not in row.index:
            return None
        
        value = row[column]
        if pd.isna(value):
            return None
        
        return str(value).strip()
    
    def _parse_currency_value(self, value_str: str) -> Optional[Tuple[str, float]]:
        """
        Parse a currency value string like "USD 1,234.56"
        
        Returns:
            Tuple of (currency_code, amount) or None
        """
        if not value_str:
            return None
        
        match = re.match(r'([A-Z]{3})\s*([\d,\.]+)', str(value_str))
        if match:
            currency = match.group(1)
            amount_str = match.group(2).replace(',', '')
            try:
                amount = float(amount_str)
                return (currency, amount)
            except ValueError:
                return None
        
        return None
    
    def validate_pdo_data(self, pdo_data: SAPPDOData) -> List[ValidationIssue]:
        """
        Validate parsed PDO data.
        
        Returns list of validation issues.
        """
        return pdo_data.validate()


def match_pdo_to_filename(filename: str, pdo_data: Dict[str, SAPPDOData]) -> List[Tuple[str, SAPPDOData]]:
    """
    Match a PDF filename to SAP PDO data.
    
    Matching Strategy:
    1. Extract PDO numbers from filename
    2. Try exact match against pdo_number field
    3. Try substring match against sheet name
    4. Log warnings for unmatched PDOs
    
    Args:
        filename: PDF filename like "PDO 2500444_dtd251006_NST.pdf"
        pdo_data: Dictionary of parsed SAP PDO data
        
    Returns:
        List of (pdo_number, SAPPDOData) tuples that match
    """
    from utils.helpers import extract_pdo_numbers
    import logging
    logger = logging.getLogger(__name__)
    
    matches = []
    pdo_numbers = extract_pdo_numbers(filename)
    
    if not pdo_numbers:
        logger.debug(f"No PDO numbers found in filename: {filename}")
        return matches
    
    if not pdo_data:
        logger.warning(f"No SAP data available to match against filename: {filename}")
        return matches
    
    for pdo_num in pdo_numbers:
        found = False
        
        # Try exact match first
        for sheet_name, data in pdo_data.items():
            # Method 1: Exact match on pdo_number
            if data.pdo_number == pdo_num:
                matches.append((pdo_num, data))
                found = True
                logger.debug(f"Exact match: PDO {pdo_num} -> {sheet_name}")
                break
            
            # Method 2: PDO number substring in sheet name
            if pdo_num in sheet_name:
                matches.append((pdo_num, data))
                found = True
                logger.debug(f"Substring match: PDO {pdo_num} in sheet '{sheet_name}'")
                break
            
            # Method 3: Fuzzy match - last 5 digits
            if len(pdo_num) >= 5 and len(data.pdo_number) >= 5:
                if pdo_num[-5:] == data.pdo_number[-5:]:
                    matches.append((pdo_num, data))
                    found = True
                    logger.debug(f"Fuzzy match: PDO {pdo_num} ~ {data.pdo_number}")
                    break
        
        if not found:
            # List available PDO numbers for debugging
            available_pdos = [d.pdo_number for d in pdo_data.values()]
            logger.warning(
                f"No SAP match for PDO {pdo_num} from filename '{filename}'. "
                f"Available PDOs in SAP: {available_pdos}"
            )
    
    return matches
