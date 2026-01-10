"""
Column Mapper for Recorder Data

Automatically maps recorder CSV columns to canonical field names using
fuzzy matching and common variations.
"""

import csv
import json
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from difflib import SequenceMatcher


# Canonical field names we need to map to
CANONICAL_FIELDS = {
    "recording_date": ["recording date", "record date", "date recorded", "filing date", "doc date"],
    "doc_type": ["document type", "instrument type", "doc type", "instrument", "type"],
    "lender_name": ["lender", "beneficiary", "mortgagee", "grantee", "payee", "creditor"],
    "borrower_name": ["borrower", "grantor", "trustor", "mortgagor", "debtor", "obligor"],
    "property_address": ["property address", "situs address", "address", "property location", "situs"],
    "property_city": ["city", "property city", "situs city"],
    "property_state": ["state", "property state", "situs state"],
    "property_zip": ["zip", "zip code", "postal code", "property zip", "situs zip"],
    "loan_amount": ["loan amount", "principal", "original principal", "amount", "loan value", "face amount"],
    "interest_rate": ["interest rate", "rate", "apr", "interest"],
    "maturity_date": ["maturity date", "maturity", "due date", "payoff date"],
    "loan_term": ["loan term", "term", "loan period", "months", "years"],
    "apn": ["apn", "parcel id", "parcel_id", "account id", "account_id", "parcel number", "assessor parcel number"]
}

# Required fields (missing these will flag rows for review)
REQUIRED_FIELDS = ["lender_name", "loan_amount"]


def normalize_column_name(col: str) -> str:
    """Normalize column name for comparison."""
    return col.lower().strip().replace("_", " ").replace("-", " ")


def similarity_score(s1: str, s2: str) -> float:
    """Calculate similarity score between two strings."""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def find_best_match(column_name: str, candidates: List[str], threshold: float = 0.6) -> Optional[str]:
    """
    Find best matching canonical field for a column name.
    
    Args:
        column_name: The column name from the CSV
        candidates: List of candidate canonical field names
        threshold: Minimum similarity score to consider a match
        
    Returns:
        Best matching canonical field name or None
    """
    normalized_col = normalize_column_name(column_name)
    best_match = None
    best_score = 0.0
    
    for candidate in candidates:
        # Check direct match first
        if normalized_col == normalize_column_name(candidate):
            return candidate
        
        # Check against aliases
        for canonical, aliases in CANONICAL_FIELDS.items():
            if candidate == canonical:
                for alias in aliases:
                    score = similarity_score(normalized_col, alias)
                    if score > best_score and score >= threshold:
                        best_score = score
                        best_match = canonical
                # Also check direct match with canonical name
                score = similarity_score(normalized_col, canonical)
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = canonical
    
    return best_match


def map_columns(csv_path: Path) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    Map CSV columns to canonical field names.
    
    Args:
        csv_path: Path to the recorder CSV file
        
    Returns:
        Tuple of (column_mapping, field_status)
        column_mapping: Dict mapping canonical field -> CSV column name
        field_status: Dict with 'found', 'missing', 'ambiguous' lists
    """
    column_mapping = {}
    field_status = {
        "found": [],
        "missing": [],
        "ambiguous": []
    }
    
    # Read header
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader)
    
    print(f"Found {len(header)} columns in CSV:")
    for col in header:
        print(f"  - {col}")
    print()
    
    # Try to map each canonical field
    for canonical_field in CANONICAL_FIELDS.keys():
        # Skip if already mapped (avoid duplicate mappings)
        if canonical_field in column_mapping:
            continue
            
        best_column = None
        best_score = 0.0
        
        for csv_column in header:
            # Skip if this column is already mapped to another field
            if csv_column in column_mapping.values():
                continue
                
            normalized_csv = normalize_column_name(csv_column)
            normalized_canonical = normalize_column_name(canonical_field)
            
            # Direct match
            if normalized_csv == normalized_canonical:
                best_column = csv_column
                best_score = 1.0
                break
            
            # Check aliases
            for alias in CANONICAL_FIELDS[canonical_field]:
                score = similarity_score(normalized_csv, alias)
                if score > best_score:
                    best_score = score
                    best_column = csv_column
        
        # Use match if score is good enough
        # Require higher threshold for optional fields
        required_threshold = 0.5
        optional_fields = ["interest_rate", "maturity_date", "loan_term"]
        if canonical_field in optional_fields:
            required_threshold = 0.7  # Higher threshold for optional fields
        
        if best_score >= required_threshold:
            column_mapping[canonical_field] = best_column
            field_status["found"].append(canonical_field)
            print(f"[OK] {canonical_field:20s} -> {best_column:30s} (score: {best_score:.2f})")
        else:
            field_status["missing"].append(canonical_field)
            print(f"[--] {canonical_field:20s} -> NOT FOUND")
    
    print()
    
    # Check required fields
    missing_required = [f for f in REQUIRED_FIELDS if f not in column_mapping]
    if missing_required:
        print(f"[!] WARNING: Missing required fields: {', '.join(missing_required)}")
        print("  Rows will be flagged for review.")
    
    return column_mapping, field_status


def save_mapping(mapping: Dict[str, str], output_path: Path):
    """Save column mapping to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, indent=2)
    print(f"[OK] Saved column mapping to {output_path}")


def load_mapping(mapping_path: Path) -> Dict[str, str]:
    """Load column mapping from JSON file."""
    with open(mapping_path, 'r', encoding='utf-8') as f:
        return json.load(f)

