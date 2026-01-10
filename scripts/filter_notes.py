"""
Filter Notes Pipeline

Classifies recorder rows into leads, review queue, or discarded based on
business rules for seller-finance note identification.

Produces email-ready and mail-ready CSV outputs for cold outreach campaigns.
"""

import csv
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import re


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

MAX_LOAN_AMOUNT = 500000
MIN_AGE_YEARS = 3
MAX_AGE_YEARS = 20

BANK_KEYWORDS = [
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
    "FHA", "VA LOAN", "USDA"
]

SELLER_FINANCE_KEYWORDS = [
    "PURCHASE MONEY",
    "SELLER FINANCE", "SELLER FINANCING",
    "VENDOR LIEN",
    "DEED OF TRUST",
    "NOTE",
    "MORTGAGE",
    "PROMISSORY NOTE",
    "INSTALLMENT SALE"
]

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
    
    # Common date formats
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
    
    # Remove common formatting
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def parse_float(value_str: str) -> Optional[float]:
    """Parse a float value from string."""
    if not value_str or not value_str.strip():
        return None
    
    cleaned = value_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def is_bank_lender(lender_name: str) -> bool:
    """Check if lender name matches bank keywords."""
    if not lender_name:
        return False
    
    lender_upper = lender_name.upper()
    return any(keyword in lender_upper for keyword in BANK_KEYWORDS)


def detect_owner_type(name: str) -> str:
    """
    Detect owner type: PERSON, LLC, TRUST, BANK, or UNKNOWN.
    
    Args:
        name: Lender/owner name
        
    Returns:
        Owner type string
    """
    if not name or not name.strip():
        return "UNKNOWN"
    
    name_upper = name.upper().strip()
    
    # Check for bank first
    if is_bank_lender(name):
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
        return "LLC"  # Treat other corps as LLC for now
    
    # If contains comma, likely a person (LAST, FIRST format)
    if "," in name:
        return "PERSON"
    
    # If short and no corporate indicators, likely a person
    if len(name) < 50 and not any(keyword in name_upper for keyword in 
                                   ["COMPANY", "GROUP", "HOLDINGS", "PROPERTIES", "INVESTMENTS"]):
        return "PERSON"
    
    return "UNKNOWN"


def parse_name(full_name: str) -> Tuple[str, str]:
    """
    Parse full name into first_name and last_name.
    
    Handles formats like:
    - "LAST, FIRST"
    - "FIRST LAST"
    - "FIRST MIDDLE LAST"
    """
    if not full_name or not full_name.strip():
        return ("", "")
    
    full_name = full_name.strip()
    
    # Handle "LAST, FIRST" format
    if "," in full_name:
        parts = [p.strip() for p in full_name.split(",", 1)]
        if len(parts) == 2:
            return (parts[1], parts[0])  # (first, last)
        elif len(parts) == 1:
            return ("", parts[0])  # Only last name
    
    # Handle "FIRST LAST" format
    parts = full_name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = " ".join(parts[1:])
        return (first, last)
    elif len(parts) == 1:
        return ("", parts[0])
    
    return ("", "")


def extract_company_name(name: str, owner_type: str) -> str:
    """Extract company name if owner_type is LLC."""
    if owner_type != "LLC":
        return ""
    return name.strip()


def is_seller_finance_doc(doc_type: str) -> bool:
    """Check if document type suggests seller financing."""
    if not doc_type:
        return False
    
    doc_upper = doc_type.upper()
    return any(keyword in doc_upper for keyword in SELLER_FINANCE_KEYWORDS)


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


def generate_lead_id(full_name: str, mailing_zip: str, loan_amount: str, recording_date: str) -> str:
    """
    Generate deterministic lead_id using SHA1 hash.
    
    Args:
        full_name: Full name of lender
        mailing_zip: Mailing zip code
        loan_amount: Original loan amount (as string)
        recording_date: Recording date (as string)
        
    Returns:
        SHA1 hash as hex string
    """
    # Normalize inputs
    full_name = (full_name or "").strip().upper()
    mailing_zip = (mailing_zip or "").strip()
    loan_amount = (loan_amount or "").strip()
    recording_date = (recording_date or "").strip()
    
    # Create hash input
    hash_input = f"{full_name}|{mailing_zip}|{loan_amount}|{recording_date}"
    
    # Generate SHA1 hash
    hash_obj = hashlib.sha1(hash_input.encode('utf-8'))
    return hash_obj.hexdigest()


def calculate_lead_score(
    owner_type: str,
    loan_amount: Optional[float],
    doc_type: str,
    recording_age: Optional[float],
    is_seller_finance: bool
) -> Tuple[int, str]:
    """
    Calculate lead score (0-100) and why_flagged summary.
    
    Args:
        owner_type: PERSON, LLC, TRUST, BANK, UNKNOWN
        loan_amount: Loan amount
        doc_type: Document type
        recording_age: Age of recording in years
        is_seller_finance: Whether doc type suggests seller finance
        
    Returns:
        Tuple of (score, why_flagged)
    """
    score = 0
    reasons = []
    
    # Owner type scoring
    if owner_type == "PERSON":
        score += 40
        reasons.append("person lender")
    elif owner_type == "LLC":
        score += 25
        reasons.append("LLC lender")
    elif owner_type == "TRUST":
        score += 20
        reasons.append("trust lender")
    elif owner_type == "BANK":
        score = 0
        return (0, "bank lender")
    else:
        score += 10
        reasons.append("unknown owner type")
    
    # Loan amount scoring
    if loan_amount:
        if 10000 <= loan_amount < 100000:
            score += 25
            reasons.append("small amount")
        elif 100000 <= loan_amount < 300000:
            score += 30
            reasons.append("medium amount")
        elif 300000 <= loan_amount < MAX_LOAN_AMOUNT:
            score += 20
            reasons.append("large amount")
        elif loan_amount < 10000:
            score += 10
            reasons.append("very small amount")
    else:
        score -= 20
        reasons.append("missing loan amount")
    
    # Document type scoring
    if is_seller_finance:
        score += 25
        reasons.append("seller finance doc")
    elif doc_type:
        score += 10
        reasons.append("other doc type")
    else:
        score -= 10
        reasons.append("missing doc type")
    
    # Recording age scoring
    if recording_age:
        if 3 <= recording_age <= 10:
            score += 10
            reasons.append("recent recording")
        elif 10 < recording_age <= 20:
            score += 5
            reasons.append("older recording")
        elif recording_age < 3:
            score -= 5
            reasons.append("very recent")
        else:
            score -= 10
            reasons.append("very old")
    else:
        score -= 5
        reasons.append("missing date")
    
    # Cap score at 100
    score = max(0, min(100, score))
    
    # Create why_flagged summary
    why_flagged = " + ".join(reasons[:5])  # Limit to first 5 reasons
    
    return (score, why_flagged)


# ============================================================================
# CLASSIFICATION LOGIC
# ============================================================================

def classify_row(row: Dict[str, str], column_mapping: Dict[str, str]) -> Tuple[str, str]:
    """
    Classify a row as LEAD, REVIEW, or DISCARD.
    
    Args:
        row: Dictionary of CSV row data (original column names)
        column_mapping: Mapping from canonical fields to CSV column names
        
    Returns:
        Tuple of (classification, reason)
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
    
    # Parse values
    loan_amount = parse_amount(loan_amount_str)
    recording_date = parse_date(recording_date_str)
    recording_age = get_recording_age_years(recording_date)
    
    # ========================================================================
    # DISCARD RULES
    # ========================================================================
    
    # Discard if lender is a bank
    if is_bank_lender(lender_name):
        return ("DISCARD", "Lender is a bank or large financial institution")
    
    # Discard if document type indicates release/satisfaction only
    if is_discard_doc_type(doc_type):
        return ("DISCARD", f"Document type indicates release/satisfaction: {doc_type}")
    
    # Discard if no loan amount and no other useful data
    if loan_amount is None and not lender_name and not doc_type:
        return ("DISCARD", "Missing all critical fields (loan_amount, lender_name, doc_type)")
    
    # Discard if recording too old (beyond max age)
    if recording_age is not None and recording_age > MAX_AGE_YEARS:
        return ("DISCARD", f"Recording too old ({recording_age:.1f} years, max {MAX_AGE_YEARS})")
    
    # ========================================================================
    # LEAD RULES
    # ========================================================================
    
    # Must have loan amount in valid range
    if loan_amount is None:
        return ("REVIEW", "Missing loan_amount")
    
    if loan_amount <= 0:
        return ("DISCARD", f"Invalid loan amount: {loan_amount}")
    
    if loan_amount >= MAX_LOAN_AMOUNT:
        return ("REVIEW", f"Loan amount too large ({loan_amount:,.0f}, max {MAX_LOAN_AMOUNT:,})")
    
    # Check lender type
    if not lender_name:
        return ("REVIEW", "Missing lender_name")
    
    # Check recording age
    if recording_age is None:
        return ("REVIEW", "Missing or unparseable recording_date")
    
    if recording_age < MIN_AGE_YEARS:
        return ("REVIEW", f"Recording too recent ({recording_age:.1f} years, min {MIN_AGE_YEARS})")
    
    if recording_age > MAX_AGE_YEARS:
        return ("DISCARD", f"Recording too old ({recording_age:.1f} years, max {MAX_AGE_YEARS})")
    
    # All checks passed - this is a LEAD
    return ("LEAD", "Meets all criteria for seller-finance note lead")


def build_email_ready_row(
    row: Dict[str, str],
    column_mapping: Dict[str, str],
    tcad_row: Optional[Dict[str, str]],
    source_file: str
) -> Dict[str, str]:
    """
    Build an email-ready row with all required columns.
    
    Args:
        row: Original recorder row
        column_mapping: Column mapping
        tcad_row: Optional TCAD enrichment data
        source_file: Source file name
        
    Returns:
        Dictionary with all email-ready columns
    """
    def get_field(canonical_name: str, default: str = "") -> str:
        if canonical_name in column_mapping:
            csv_col = column_mapping[canonical_name]
            return row.get(csv_col, default)
        return default
    
    # Extract base fields
    lender_name = get_field("lender_name", "").strip()
    loan_amount_str = get_field("loan_amount", "")
    recording_date_str = get_field("recording_date", "")
    doc_type = get_field("doc_type", "").strip()
    
    # Parse values
    loan_amount = parse_amount(loan_amount_str)
    recording_date = parse_date(recording_date_str)
    recording_age = get_recording_age_years(recording_date)
    
    # Detect owner type
    owner_type = detect_owner_type(lender_name)
    
    # Parse name
    first_name, last_name = parse_name(lender_name)
    full_name = lender_name.strip()
    
    # Extract company name
    company_name = extract_company_name(lender_name, owner_type)
    
    # Get mailing address (prefer TCAD, fallback to recorder)
    mailing_address_1 = ""
    mailing_city = ""
    mailing_state = ""
    mailing_zip = ""
    
    if tcad_row:
        mailing_address_1 = tcad_row.get("mailing_address", "").strip()
        mailing_city = tcad_row.get("mailing_city", "").strip()
        mailing_state = tcad_row.get("mailing_state", "").strip()
        mailing_zip = tcad_row.get("mailing_zip", "").strip()
    
    # If TCAD doesn't have it, try recorder fields (if they exist)
    if not mailing_address_1:
        # Could add logic here to extract from recorder if available
        pass
    
    # Get property address (prefer TCAD situs, fallback to recorder)
    property_address_1 = ""
    property_city = ""
    property_state = ""
    property_zip = ""
    
    if tcad_row:
        property_address_1 = tcad_row.get("situs_address", "").strip()
        property_city = tcad_row.get("situs_city", "").strip()
        property_state = tcad_row.get("situs_state", "").strip()
        property_zip = tcad_row.get("situs_zip", "").strip()
    
    # Fallback to recorder fields
    if not property_address_1:
        property_address_1 = get_field("property_address", "").strip()
        property_city = get_field("property_city", "").strip()
        property_state = get_field("property_state", "").strip()
        property_zip = get_field("property_zip", "").strip()
    
    # County (extract from state or leave blank - would need county field in data)
    county = ""  # Would need county mapping or field
    
    # Optional fields
    interest_rate_str = get_field("interest_rate", "")
    interest_rate = parse_float(interest_rate_str)
    interest_rate_display = f"{interest_rate:.2f}" if interest_rate else ""
    
    loan_term_str = get_field("loan_term", "")
    loan_term_months = ""  # Would need to parse from loan_term field
    
    lien_position = ""  # Would need to infer or extract from data
    
    # TCAD account ID
    tcad_account_id = ""
    apn_field = column_mapping.get("apn", "")
    if apn_field and apn_field in row:
        tcad_account_id = row[apn_field].strip()
    elif tcad_row:
        tcad_account_id = tcad_row.get("account_id", "").strip()
    
    # Generate lead_id
    lead_id = generate_lead_id(full_name, mailing_zip, loan_amount_str, recording_date_str)
    
    # Calculate lead score
    is_seller_finance = is_seller_finance_doc(doc_type)
    lead_score, why_flagged = calculate_lead_score(
        owner_type, loan_amount, doc_type, recording_age, is_seller_finance
    )
    
    # Build output row
    output_row = {
        "lead_id": lead_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": "",  # Blank - to be filled by enrichment
        "company_name": company_name,
        "owner_type": owner_type,
        "mailing_address_1": mailing_address_1,
        "mailing_city": mailing_city,
        "mailing_state": mailing_state,
        "mailing_zip": mailing_zip,
        "property_address_1": property_address_1,
        "property_city": property_city,
        "property_state": property_state,
        "property_zip": property_zip,
        "county": county,
        "doc_type": doc_type,
        "recording_date": recording_date_str,
        "original_loan_amount": loan_amount_str if loan_amount_str else "",
        "interest_rate": interest_rate_display,
        "loan_term_months": loan_term_months,
        "lien_position": lien_position,
        "tcad_account_id": tcad_account_id,
        "source_file": source_file,
        "lead_score": str(lead_score),
        "why_flagged": why_flagged
    }
    
    return output_row


def build_mail_ready_row(email_row: Dict[str, str], tcad_row: Optional[Dict[str, str]]) -> Dict[str, str]:
    """
    Build a mail-ready row from email-ready row, adding mail-specific fields.
    
    Args:
        email_row: Email-ready row
        tcad_row: Optional TCAD data
        
    Returns:
        Dictionary with mail-ready columns
    """
    mail_row = dict(email_row)
    
    # Add mail-specific fields
    mail_row["mailing_address_2"] = ""  # Would extract if available
    
    # Owner mailing name line (exact formatting for labels)
    owner_mailing_name_line = email_row.get("full_name", "")
    if email_row.get("company_name"):
        owner_mailing_name_line = email_row.get("company_name", "")
    mail_row["owner_mailing_name_line"] = owner_mailing_name_line
    
    # Property owner occupied guess (would need logic to determine)
    mail_row["property_owner_occupied_guess"] = "UNKNOWN"
    
    # Equity estimate (optional, blank for now)
    mail_row["equity_estimate"] = ""
    
    return mail_row


def process_recorder_file(
    recorder_path: Path,
    column_mapping: Dict[str, str],
    output_dir: Path,
    tcad_path: Optional[Path] = None
) -> Dict[str, int]:
    """
    Process recorder CSV and classify rows into email-ready and mail-ready formats.
    
    Args:
        recorder_path: Path to recorder CSV
        column_mapping: Column mapping from canonical to CSV columns
        output_dir: Directory for output files
        tcad_path: Optional path to TCAD prop_clean.csv for enrichment
        
    Returns:
        Dictionary with counts: {"leads": X, "review": Y, "discarded": Z}
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    email_path = output_dir / "note_leads_email_ready.csv"
    mail_path = output_dir / "note_leads_mail_ready.csv"
    review_path = output_dir / "review_queue.csv"
    discard_path = output_dir / "discarded.csv"
    
    source_file = recorder_path.name
    
    # Load TCAD data if provided (for enrichment)
    tcad_lookup = {}
    if tcad_path and tcad_path.exists():
        print(f"Loading TCAD data from {tcad_path}...")
        with open(tcad_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                account_id = row.get("account_id", "").strip()
                if account_id:
                    tcad_lookup[account_id] = row
        print(f"Loaded {len(tcad_lookup):,} TCAD records")
    
    # Define email-ready columns
    email_columns = [
        "lead_id", "first_name", "last_name", "full_name", "email", "company_name",
        "owner_type", "mailing_address_1", "mailing_city", "mailing_state", "mailing_zip",
        "property_address_1", "property_city", "property_state", "property_zip",
        "county", "doc_type", "recording_date", "original_loan_amount",
        "interest_rate", "loan_term_months", "lien_position", "tcad_account_id",
        "source_file", "lead_score", "why_flagged"
    ]
    
    # Define mail-ready columns (email columns + mail-specific)
    mail_columns = email_columns + [
        "mailing_address_2", "owner_mailing_name_line",
        "property_owner_occupied_guess", "equity_estimate"
    ]
    
    # Review/discard columns (original + reason)
    with open(recorder_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        original_columns = list(reader.fieldnames or [])
    
    review_columns = original_columns + ["classification_reason"]
    discard_columns = original_columns + ["classification_reason"]
    
    # Open output files
    email_file = open(email_path, 'w', newline='', encoding='utf-8')
    mail_file = open(mail_path, 'w', newline='', encoding='utf-8')
    review_file = open(review_path, 'w', newline='', encoding='utf-8')
    discard_file = open(discard_path, 'w', newline='', encoding='utf-8')
    
    email_writer = csv.DictWriter(email_file, fieldnames=email_columns)
    mail_writer = csv.DictWriter(mail_file, fieldnames=mail_columns)
    review_writer = csv.DictWriter(review_file, fieldnames=review_columns)
    discard_writer = csv.DictWriter(discard_file, fieldnames=discard_columns)
    
    email_writer.writeheader()
    mail_writer.writeheader()
    review_writer.writeheader()
    discard_writer.writeheader()
    
    # Process rows
    counts = {"leads": 0, "review": 0, "discarded": 0}
    
    with open(recorder_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # Classify row
            classification, reason = classify_row(row, column_mapping)
            
            if classification == "LEAD":
                # Get TCAD data if available
                tcad_row = None
                apn_field = column_mapping.get("apn", "")
                if apn_field and apn_field in row:
                    account_id = row[apn_field].strip()
                    if account_id in tcad_lookup:
                        tcad_row = tcad_lookup[account_id]
                
                # Build email-ready row
                email_row = build_email_ready_row(row, column_mapping, tcad_row, source_file)
                email_writer.writerow(email_row)
                
                # Build mail-ready row
                mail_row = build_mail_ready_row(email_row, tcad_row)
                mail_writer.writerow(mail_row)
                
                counts["leads"] += 1
                
            elif classification == "REVIEW":
                output_row = dict(row)
                output_row["classification_reason"] = reason
                review_writer.writerow(output_row)
                counts["review"] += 1
                
            else:  # DISCARD
                output_row = dict(row)
                output_row["classification_reason"] = reason
                discard_writer.writerow(output_row)
                counts["discarded"] += 1
            
            # Progress update
            if row_num % 10000 == 0:
                print(f"Processed {row_num:,} rows... (leads: {counts['leads']:,}, "
                      f"review: {counts['review']:,}, discarded: {counts['discarded']:,})")
    
    # Close files
    email_file.close()
    mail_file.close()
    review_file.close()
    discard_file.close()
    
    return counts
