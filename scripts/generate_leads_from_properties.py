"""
Generate Note Leads from Property Data

Alternative approach: Uses property owners as potential note holders.
This is speculative - actual recorder data is preferred.
"""

import csv
import hashlib
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import sys

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from filter_notes import (
    detect_owner_type, parse_name, extract_company_name,
    generate_lead_id, calculate_lead_score
)


def parse_amount(amount_str: str) -> Optional[float]:
    """Parse amount from string."""
    if not amount_str or not amount_str.strip():
        return None
    
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def build_lead_from_property(row: Dict[str, str], source_file: str) -> Optional[Dict[str, str]]:
    """
    Build a lead row from property data.
    
    This is speculative - assumes property owners might be note holders.
    """
    owner_name = row.get("owner_name", "").strip()
    if not owner_name:
        return None
    
    # Detect owner type
    owner_type = detect_owner_type(owner_name)
    
    # Skip banks
    if owner_type == "BANK":
        return None
    
    # Parse name
    first_name, last_name = parse_name(owner_name)
    full_name = owner_name.strip()
    company_name = extract_company_name(owner_name, owner_type)
    
    # Get property value as proxy for potential loan amount
    total_value_str = row.get("total_value", "")
    total_value = parse_amount(total_value_str)
    
    # Use property value as loan amount estimate (divide by 2-3 as rough estimate)
    # This is speculative - actual recorder data would have real loan amounts
    if total_value:
        estimated_loan = total_value / 2.5  # Rough estimate
        if estimated_loan > 500000:
            return None  # Skip if too large
        if estimated_loan < 1000:
            return None  # Skip if too small
        loan_amount_str = f"{int(estimated_loan)}"
    else:
        return None  # Need some value
    
    # Get addresses
    mailing_address_1 = row.get("mailing_address", "").strip()
    mailing_city = row.get("mailing_city", "").strip()
    mailing_state = row.get("mailing_state", "").strip()
    mailing_zip = row.get("mailing_zip", "").strip()
    
    property_address_1 = row.get("situs_address", "").strip()
    property_city = row.get("situs_city", "").strip()
    property_state = row.get("situs_state", "").strip()
    property_zip = row.get("situs_zip", "").strip()
    
    # Use assessed year as proxy for recording date (speculative)
    assessed_year = row.get("assessed_year", "").strip()
    if assessed_year and len(assessed_year) >= 4:
        # Convert "02025" to "2025-01-01" as proxy date
        year = int(assessed_year[-4:])
        recording_date_str = f"{year}-01-01"
    else:
        recording_date_str = "2020-01-01"  # Default
    
    # Generate lead_id
    lead_id = generate_lead_id(full_name, mailing_zip, loan_amount_str, recording_date_str)
    
    # Calculate lead score (using estimated values)
    from datetime import datetime
    recording_date = datetime.strptime(recording_date_str, "%Y-%m-%d")
    from filter_notes import get_recording_age_years
    recording_age = get_recording_age_years(recording_date)
    
    # Use generic doc type
    doc_type = "DEED OF TRUST"  # Assumed
    from filter_notes import is_seller_finance_doc
    is_seller_finance = is_seller_finance_doc(doc_type)
    
    lead_score, why_flagged = calculate_lead_score(
        owner_type, estimated_loan, doc_type, recording_age, is_seller_finance
    )
    
    # Only include if score is reasonable
    if lead_score < 30:
        return None
    
    # Build output row
    tcad_account_id = row.get("account_id", "").strip()
    
    output_row = {
        "lead_id": lead_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": "",
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
        "county": "",
        "doc_type": doc_type,
        "recording_date": recording_date_str,
        "original_loan_amount": loan_amount_str,
        "interest_rate": "",
        "loan_term_months": "",
        "lien_position": "",
        "tcad_account_id": tcad_account_id,
        "source_file": source_file,
        "lead_score": str(lead_score),
        "why_flagged": why_flagged + " (estimated from property data)"
    }
    
    return output_row


def process_property_file(
    prop_path: Path,
    output_dir: Path,
    min_score: int = 50
) -> Dict[str, int]:
    """
    Process property CSV and generate potential leads.
    
    Args:
        prop_path: Path to prop_clean.csv
        output_dir: Output directory
        min_score: Minimum lead score to include
        
    Returns:
        Dictionary with counts
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    email_path = output_dir / "note_leads_email_ready.csv"
    mail_path = output_dir / "note_leads_mail_ready.csv"
    
    source_file = prop_path.name
    
    # Define columns
    email_columns = [
        "lead_id", "first_name", "last_name", "full_name", "email", "company_name",
        "owner_type", "mailing_address_1", "mailing_city", "mailing_state", "mailing_zip",
        "property_address_1", "property_city", "property_state", "property_zip",
        "county", "doc_type", "recording_date", "original_loan_amount",
        "interest_rate", "loan_term_months", "lien_position", "tcad_account_id",
        "source_file", "lead_score", "why_flagged"
    ]
    
    mail_columns = email_columns + [
        "mailing_address_2", "owner_mailing_name_line",
        "property_owner_occupied_guess", "equity_estimate"
    ]
    
    # Open output files
    email_file = open(email_path, 'w', newline='', encoding='utf-8')
    mail_file = open(mail_path, 'w', newline='', encoding='utf-8')
    
    email_writer = csv.DictWriter(email_file, fieldnames=email_columns)
    mail_writer = csv.DictWriter(mail_file, fieldnames=mail_columns)
    
    email_writer.writeheader()
    mail_writer.writeheader()
    
    # Process rows
    count = 0
    skipped = 0
    
    print(f"Processing {prop_path}...")
    print(f"Minimum lead score: {min_score}")
    print()
    
    with open(prop_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):
            lead_row = build_lead_from_property(row, source_file)
            
            if lead_row:
                score = int(lead_row.get("lead_score", "0"))
                if score >= min_score:
                    email_writer.writerow(lead_row)
                    
                    # Build mail-ready row
                    mail_row = dict(lead_row)
                    mail_row["mailing_address_2"] = ""
                    mail_row["owner_mailing_name_line"] = lead_row.get("full_name", "")
                    mail_row["property_owner_occupied_guess"] = "UNKNOWN"
                    mail_row["equity_estimate"] = ""
                    mail_writer.writerow(mail_row)
                    
                    count += 1
                else:
                    skipped += 1
            else:
                skipped += 1
            
            # Progress update
            if row_num % 50000 == 0:
                print(f"Processed {row_num:,} rows... (leads: {count:,}, skipped: {skipped:,})")
    
    email_file.close()
    mail_file.close()
    
    return {"leads": count, "skipped": skipped}


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate note leads from property data (speculative approach)"
    )
    parser.add_argument(
        "--prop",
        type=str,
        default="output/prop_clean.csv",
        help="Path to prop_clean.csv (default: output/prop_clean.csv)"
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    parser.add_argument(
        "--min-score",
        type=int,
        default=50,
        help="Minimum lead score to include (default: 50)"
    )
    
    args = parser.parse_args()
    
    prop_path = Path(args.prop)
    if not prop_path.exists():
        print(f"Error: Property file not found: {prop_path}", file=sys.stderr)
        sys.exit(1)
    
    output_dir = Path(args.outdir)
    
    print("=" * 70)
    print("GENERATE LEADS FROM PROPERTY DATA")
    print("=" * 70)
    print(f"WARNING: This is a speculative approach.")
    print(f"Actual recorder data is preferred for accurate results.")
    print()
    print(f"Property file: {prop_path}")
    print(f"Output directory: {output_dir}")
    print(f"Minimum score: {args.min_score}")
    print()
    
    counts = process_property_file(prop_path, output_dir, args.min_score)
    
    print()
    print("=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Total leads generated: {counts['leads']:,}")
    print(f"Skipped: {counts['skipped']:,}")
    print()
    print(f"Output files:")
    print(f"  - output/note_leads_email_ready.csv")
    print(f"  - output/note_leads_mail_ready.csv")
    print()


if __name__ == "__main__":
    main()



