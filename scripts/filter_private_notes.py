"""
Filter Private Note Leads

Filters recorder data to identify private-held mortgage notes,
explicitly excluding banks and servicers.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple
from collections import Counter


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

MAX_LOAN_AMOUNT = 500000
MIN_AGE_YEARS = 3
MAX_AGE_YEARS = 20

# Hard-exclude banks and servicers
BANK_SERVICER_KEYWORDS = [
    "BANK", "N.A.", "MORTGAGE", "SERVICING",
    "WELLS FARGO", "WELLS",
    "JPMORGAN", "JP MORGAN", "CHASE",
    "BANK OF AMERICA", "BANK OF AMER", "BOA",
    "CITIBANK", "CITI",
    "U.S. BANK", "US BANK", "USBANK",
    "PNC",
    "TRUIST",
    "CAPITAL ONE", "CAPITALONE",
    "REGIONS BANK", "REGIONS",
    "SUNTRUST",
    "BB&T", "BBT",
    "TD BANK",
    "HSBC",
    "BANK OF NEW YORK", "BNY",
    "DEUTSCHE BANK",
    "BARCLAYS",
    "MORGAN STANLEY",
    "GOLDMAN SACHS",
    "MERRILL LYNCH",
    "FIDELITY",
    "VANGUARD",
    "FEDERAL HOME LOAN", "FHLMC", "FREDDIE MAC",
    "FANNIE MAE", "FNMA",
    "GINNIE MAE", "GNMA",
    "FHA", "VA LOAN", "USDA",
    "SERVICING", "SERVICE", "SERVICER"
]

# Preferred document types (seller finance / purchase money)
PREFERRED_DOC_TYPES = [
    "PURCHASE MONEY",
    "SELLER FINANCE", "SELLER FINANCING",
    "VENDOR LIEN",
    "DEED OF TRUST",
    "NOTE",
    "MORTGAGE",
    "PROMISSORY NOTE",
    "INSTALLMENT SALE"
]

# Discard document types
DISCARD_DOC_TYPES = [
    "RELEASE",
    "SATISFACTION",
    "RELEASE OF LIEN",
    "SATISFACTION OF MORTGAGE",
    "CANCELLATION"
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string in various formats."""
    if not date_str or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%y",
        "%Y%m%d"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None


def parse_amount(amount_str: str) -> Optional[float]:
    """Parse loan amount from string."""
    if not amount_str or not amount_str.strip():
        return None
    
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def is_bank_or_servicer(lender_name: str) -> bool:
    """Hard-exclude banks and servicers."""
    if not lender_name:
        return False
    
    lender_upper = lender_name.upper()
    return any(keyword in lender_upper for keyword in BANK_SERVICER_KEYWORDS)


def detect_lender_type(lender_name: str) -> str:
    """
    Detect lender type: BANK, PERSON, LLC, TRUST, or UNKNOWN.
    
    Args:
        lender_name: Lender name
        
    Returns:
        Lender type string
    """
    if not lender_name or not lender_name.strip():
        return "UNKNOWN"
    
    name_upper = lender_name.upper().strip()
    
    # Check for bank/servicer first (hard exclude)
    if is_bank_or_servicer(lender_name):
        return "BANK"
    
    # Check for trust
    if any(keyword in name_upper for keyword in ["TRUST", "TRUSTEE", "ESTATE"]):
        return "TRUST"
    
    # Check for LLC
    if any(name_upper.endswith(f" {suffix}") or name_upper.endswith(f", {suffix}") or 
           f" {suffix}" in name_upper or f", {suffix}" in name_upper
           for suffix in ["LLC", "L.L.C.", "L L C"]):
        return "LLC"
    
    # Check for other corporate entities
    corporate_suffixes = ["INC", "INCORPORATED", "CORP", "CORPORATION", "LTD", "LIMITED", 
                          "LP", "L.P.", "LLP", "L.L.P.", "PC", "P.C.", "PLLC"]
    if any(name_upper.endswith(f" {suffix}") or name_upper.endswith(f", {suffix}") or
           f" {suffix}" in name_upper or f", {suffix}" in name_upper
           for suffix in corporate_suffixes):
        return "LLC"  # Treat other corps as LLC
    
    # If contains comma, likely a person (LAST, FIRST format)
    if "," in lender_name:
        return "PERSON"
    
    # If short and no corporate indicators, likely a person
    if len(lender_name) < 50 and not any(keyword in name_upper for keyword in 
                                         ["COMPANY", "GROUP", "HOLDINGS", "PROPERTIES", "INVESTMENTS"]):
        return "PERSON"
    
    return "UNKNOWN"


def is_preferred_doc_type(doc_type: str) -> bool:
    """Check if document type is preferred (seller finance related)."""
    if not doc_type:
        return False
    
    doc_upper = doc_type.upper()
    return any(keyword in doc_upper for keyword in PREFERRED_DOC_TYPES)


def is_discard_doc_type(doc_type: str) -> bool:
    """Check if document type should be discarded."""
    if not doc_type:
        return False
    
    doc_upper = doc_type.upper()
    return any(keyword in doc_upper for keyword in DISCARD_DOC_TYPES)


def get_recording_age_years(recording_date: Optional[datetime]) -> Optional[float]:
    """Calculate age of recording in years."""
    if not recording_date:
        return None
    
    today = datetime.now()
    delta = today - recording_date
    return delta.days / 365.25


# ============================================================================
# CLASSIFICATION LOGIC
# ============================================================================

def classify_row(row: Dict[str, str], column_mapping: Dict[str, str]) -> Tuple[str, str, str]:
    """
    Classify a row as LEAD, REVIEW, or DISCARD.
    
    Args:
        row: Dictionary of CSV row data (original column names)
        column_mapping: Mapping from canonical fields to CSV column names
        
    Returns:
        Tuple of (classification, lender_type, reason)
    """
    # Extract mapped values
    def get_field(canonical_name: str, default: str = "") -> str:
        if canonical_name in column_mapping:
            csv_col = column_mapping[canonical_name]
            return row.get(csv_col, default)
        return default
    
    lender_name = get_field("lender_name", "").strip()
    loan_amount_str = get_field("loan_amount", "")
    doc_type = get_field("doc_type", "").strip()
    recording_date_str = get_field("recording_date", "")
    
    # Detect lender type
    lender_type = detect_lender_type(lender_name)
    
    # Parse values
    loan_amount = parse_amount(loan_amount_str)
    recording_date = parse_date(recording_date_str)
    recording_age = get_recording_age_years(recording_date)
    
    # ========================================================================
    # DISCARD RULES
    # ========================================================================
    
    # Hard-exclude banks and servicers
    if lender_type == "BANK":
        return ("DISCARD", lender_type, "Lender is a bank or servicer")
    
    # Discard if document type indicates release/satisfaction only
    if is_discard_doc_type(doc_type):
        return ("DISCARD", lender_type, f"Document type indicates release/satisfaction: {doc_type}")
    
    # Discard if no loan amount and no other useful data
    if loan_amount is None and not lender_name and not doc_type:
        return ("DISCARD", lender_type, "Missing all critical fields (loan_amount, lender_name, doc_type)")
    
    # Discard if recording too old (beyond max age)
    if recording_age is not None and recording_age > MAX_AGE_YEARS:
        return ("DISCARD", lender_type, f"Recording too old ({recording_age:.1f} years, max {MAX_AGE_YEARS})")
    
    # ========================================================================
    # LEAD RULES
    # ========================================================================
    
    # Must have loan amount in valid range
    if loan_amount is None:
        return ("REVIEW", lender_type, "Missing loan_amount")
    
    if loan_amount <= 0:
        return ("DISCARD", lender_type, f"Invalid loan amount: {loan_amount}")
    
    if loan_amount >= MAX_LOAN_AMOUNT:
        return ("REVIEW", lender_type, f"Loan amount too large ({loan_amount:,.0f}, max {MAX_LOAN_AMOUNT:,})")
    
    # Check lender type - prefer PERSON and small LLCs
    if not lender_name:
        return ("REVIEW", lender_type, "Missing lender_name")
    
    # Check recording age
    if recording_age is None:
        return ("REVIEW", lender_type, "Missing or unparseable recording_date")
    
    if recording_age < MIN_AGE_YEARS:
        return ("REVIEW", lender_type, f"Recording too recent ({recording_age:.1f} years, min {MIN_AGE_YEARS})")
    
    if recording_age > MAX_AGE_YEARS:
        return ("DISCARD", lender_type, f"Recording too old ({recording_age:.1f} years, max {MAX_AGE_YEARS})")
    
    # Prefer seller finance doc types, but not required
    if not is_preferred_doc_type(doc_type) and lender_type == "LLC":
        return ("REVIEW", lender_type, f"LLC lender without preferred doc type: {doc_type}")
    
    # All checks passed - this is a LEAD
    return ("LEAD", lender_type, "Meets all criteria for private note lead")


def process_recorder_file(
    recorder_path: Path,
    column_mapping: Dict[str, str],
    output_dir: Path
) -> Tuple[Dict[str, int], Counter]:
    """
    Process recorder CSV and classify rows.
    
    Args:
        recorder_path: Path to recorder CSV
        column_mapping: Column mapping from canonical to CSV columns
        output_dir: Directory for output files
        
    Returns:
        Tuple of (counts dictionary, lender_type_counter)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    leads_path = output_dir / "private_note_leads.csv"
    review_path = output_dir / "review_queue.csv"
    discard_path = output_dir / "discarded.csv"
    
    # Open output files
    leads_file = open(leads_path, 'w', newline='', encoding='utf-8')
    review_file = open(review_path, 'w', newline='', encoding='utf-8')
    discard_file = open(discard_path, 'w', newline='', encoding='utf-8')
    
    # Determine output columns (all original columns + classification fields)
    with open(recorder_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        original_columns = list(reader.fieldnames or [])
    
    # Add classification columns
    leads_columns = original_columns + ["lender_type", "classification_reason"]
    review_columns = original_columns + ["lender_type", "classification_reason"]
    discard_columns = original_columns + ["lender_type", "classification_reason"]
    
    leads_writer = csv.DictWriter(leads_file, fieldnames=leads_columns)
    review_writer = csv.DictWriter(review_file, fieldnames=review_columns)
    discard_writer = csv.DictWriter(discard_file, fieldnames=discard_columns)
    
    leads_writer.writeheader()
    review_writer.writeheader()
    discard_writer.writeheader()
    
    # Process rows
    counts = {"leads": 0, "review": 0, "discarded": 0}
    lender_type_counter = Counter()
    
    with open(recorder_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # Classify row
            classification, lender_type, reason = classify_row(row, column_mapping)
            
            # Add classification fields to row
            output_row = dict(row)
            output_row["lender_type"] = lender_type
            output_row["classification_reason"] = reason
            
            # Count lender types
            lender_type_counter[lender_type] += 1
            
            # Write to appropriate file
            if classification == "LEAD":
                leads_writer.writerow(output_row)
                counts["leads"] += 1
            elif classification == "REVIEW":
                review_writer.writerow(output_row)
                counts["review"] += 1
            else:  # DISCARD
                discard_writer.writerow(output_row)
                counts["discarded"] += 1
            
            # Progress update
            if row_num % 10000 == 0:
                print(f"Processed {row_num:,} rows... (leads: {counts['leads']:,}, "
                      f"review: {counts['review']:,}, discarded: {counts['discarded']:,})")
    
    # Close files
    leads_file.close()
    review_file.close()
    discard_file.close()
    
    return counts, lender_type_counter



