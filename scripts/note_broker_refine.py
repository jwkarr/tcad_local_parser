"""
Note Broker Refinement Filter

Further refines Lee Arnold Favorites (or any property targets) for note broker outreach.

Additional filters:
- Contact information quality (email, phone, address)
- Owner motivation indicators
- Geographic targeting
- Engagement likelihood scoring
- Note-specific criteria

Usage:
    python scripts/note_broker_refine.py --input output/lee_arnold_favorites.csv --outdir output
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
from collections import Counter


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Note buying sweet spot (properties in this range more likely to have notes)
NOTE_SWEET_SPOT_MIN = 150000
NOTE_SWEET_SPOT_MAX = 400000

# High equity indicators (high total value, low improvement = high equity)
MIN_EQUITY_ESTIMATE = 100000

# Geographic filters (example - customize for your market)
TARGET_ZIP_CODES = []  # Empty = no zip filter, add zip codes to filter
EXCLUDE_ZIP_CODES = []  # Add zip codes to exclude

# Owner engagement scoring weights
WEIGHT_ABSENTEE_STRONG = 30  # Different city/state
WEIGHT_ABSENTEE_WEAK = 15    # Same city, different address
WEIGHT_EMAIL = 25
WEIGHT_PHONE = 20
WEIGHT_STREET_ADDRESS = 10
WEIGHT_MULTIPLE_PROPERTIES = 15
WEIGHT_NOTE_SWEET_SPOT = 20
WEIGHT_SIMPLE_NAME = 10


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_amount(amount_str: str) -> Optional[float]:
    """Parse amount from string."""
    if not amount_str or not amount_str.strip():
        return None
    
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def has_email(row: Dict[str, str]) -> bool:
    """Check if row has email address."""
    # Check both 'email' and 'Email' columns (case insensitive)
    email = row.get("email", "") or row.get("Email", "")
    email = email.strip()
    return bool(email and "@" in email)


def has_phone(row: Dict[str, str]) -> bool:
    """Check if row has phone number."""
    # Check both 'phone' and 'Phone' columns (case insensitive)
    phone = row.get("phone", "") or row.get("Phone", "")
    phone = phone.strip()
    return bool(phone and len(phone) >= 10)


def is_street_address(address: str) -> bool:
    """Check if address is a street address (not PO box)."""
    if not address:
        return False
    
    address_upper = address.upper()
    po_box_indicators = ["PO BOX", "P.O. BOX", "P O BOX", "POBOX", "BOX ", "PMB", "SUITE"]
    return not any(indicator in address_upper for indicator in po_box_indicators)


def is_strong_absentee(row: Dict[str, str]) -> bool:
    """Check if owner is strongly absentee (different city/state)."""
    mailing_city = row.get("mailing_city", "").strip().upper()
    mailing_state = row.get("mailing_state", "").strip().upper()
    situs_city = row.get("situs_city", "").strip().upper()
    situs_state = row.get("situs_state", "").strip().upper()
    
    if not mailing_city or not situs_city:
        return False
    
    # Different state = strong absentee
    if mailing_state and situs_state and mailing_state != situs_state:
        return True
    
    # Different city = strong absentee
    if mailing_city != situs_city:
        return True
    
    return False


def is_weak_absentee(row: Dict[str, str]) -> bool:
    """Check if owner is weakly absentee (same city, different address)."""
    mailing_address = row.get("mailing_address", "").strip().upper()
    situs_address = row.get("situs_address", "").strip().upper()
    mailing_city = row.get("mailing_city", "").strip().upper()
    situs_city = row.get("situs_city", "").strip().upper()
    
    if not mailing_address or not situs_address:
        return False
    
    # Same city but different address
    if mailing_city == situs_city and mailing_address != situs_address:
        return True
    
    return False


def is_simple_name(name: str) -> bool:
    """Check if owner name is simple (easy to contact)."""
    if not name:
        return False
    
    # Simple names are short and don't have complex patterns
    if len(name) > 60:
        return False
    
    complex_patterns = ["TRUST", "ESTATE", "HEIRS", "ET AL", "ETC", "UNKNOWN"]
    name_upper = name.upper()
    
    return not any(pattern in name_upper for pattern in complex_patterns)


def estimate_equity(row: Dict[str, str]) -> Optional[float]:
    """
    Rough equity estimate: total_value - improvement_value.
    
    This assumes land value represents equity potential.
    """
    total_value = parse_amount(row.get("total_value", ""))
    improvement_value = parse_amount(row.get("improvement_value", ""))
    
    if total_value is None:
        return None
    
    if improvement_value is None:
        improvement_value = 0
    
    # Rough equity = total - improvement (land value)
    equity = total_value - improvement_value
    
    return equity if equity > 0 else None


def is_in_note_sweet_spot(total_value: Optional[float]) -> bool:
    """Check if property value is in note buying sweet spot."""
    if total_value is None:
        return False
    
    return NOTE_SWEET_SPOT_MIN <= total_value <= NOTE_SWEET_SPOT_MAX


def calculate_engagement_score(row: Dict[str, str]) -> Tuple[int, str]:
    """
    Calculate engagement score (0-100) for note broker outreach.
    
    Higher score = more likely to engage.
    """
    score = 0
    reasons = []
    
    # Email presence
    if has_email(row):
        score += WEIGHT_EMAIL
        reasons.append("has email")
    else:
        reasons.append("no email")
    
    # Phone presence
    if has_phone(row):
        score += WEIGHT_PHONE
        reasons.append("has phone")
    else:
        reasons.append("no phone")
    
    # Mailing address quality
    mailing_address = row.get("mailing_address", "")
    if is_street_address(mailing_address):
        score += WEIGHT_STREET_ADDRESS
        reasons.append("street address")
    else:
        reasons.append("PO box or missing")
    
    # Absentee status
    if is_strong_absentee(row):
        score += WEIGHT_ABSENTEE_STRONG
        reasons.append("strong absentee")
    elif is_weak_absentee(row):
        score += WEIGHT_ABSENTEE_WEAK
        reasons.append("weak absentee")
    else:
        reasons.append("owner occupied")
    
    # Multiple properties
    property_count = int(row.get("property_count", "1") or "1")
    if property_count >= 3:
        score += WEIGHT_MULTIPLE_PROPERTIES
        reasons.append(f"{property_count} properties")
    elif property_count == 2:
        score += WEIGHT_MULTIPLE_PROPERTIES // 2
        reasons.append("2 properties")
    
    # Note sweet spot
    total_value = parse_amount(row.get("total_value", ""))
    if is_in_note_sweet_spot(total_value):
        score += WEIGHT_NOTE_SWEET_SPOT
        reasons.append("note sweet spot value")
    
    # Simple name
    owner_name = row.get("full_name", "") or row.get("company_name", "")
    if is_simple_name(owner_name):
        score += WEIGHT_SIMPLE_NAME
        reasons.append("simple name")
    
    # Cap score at 100
    score = max(0, min(100, score))
    
    why_flagged = " + ".join(reasons[:6])
    
    return (score, why_flagged)


def classify_for_note_broker(row: Dict[str, str]) -> Tuple[str, int, str]:
    """
    Classify row for note broker outreach.
    
    Returns:
        Tuple of (classification, engagement_score, why_flagged)
    """
    # Calculate engagement score
    engagement_score, why_flagged = calculate_engagement_score(row)
    
    # Classification logic
    has_contact_info = has_email(row) or has_phone(row)
    total_value = parse_amount(row.get("total_value", ""))
    equity = estimate_equity(row)
    
    # HIGH PRIORITY: Has contact info + good engagement score
    if has_contact_info and engagement_score >= 60:
        return ("HIGH_PRIORITY", engagement_score, why_flagged)
    
    # MEDIUM PRIORITY: Good engagement score but no contact info (needs enrichment)
    if engagement_score >= 50:
        return ("MEDIUM_PRIORITY", engagement_score, why_flagged)
    
    # LOW PRIORITY: Lower engagement score
    if engagement_score >= 30:
        return ("LOW_PRIORITY", engagement_score, why_flagged)
    
    # REVIEW: Needs manual review
    return ("REVIEW", engagement_score, why_flagged)


def process_targets_file(
    input_path: Path,
    output_dir: Path,
    is_enriched: bool = False
) -> Dict[str, int]:
    """
    Process targets file and add note broker refinement.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    high_priority_path = output_dir / "note_broker_high_priority.csv"
    medium_priority_path = output_dir / "note_broker_medium_priority.csv"
    low_priority_path = output_dir / "note_broker_low_priority.csv"
    review_path = output_dir / "note_broker_review.csv"
    
    # Read input file to get columns
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        input_columns = list(reader.fieldnames or [])
    
    # Add refinement columns
    output_columns = input_columns + ["engagement_score", "engagement_reason", "equity_estimate", "has_email", "has_phone", "contact_quality"]
    
    # Open output files
    high_file = open(high_priority_path, 'w', newline='', encoding='utf-8')
    medium_file = open(medium_priority_path, 'w', newline='', encoding='utf-8')
    low_file = open(low_priority_path, 'w', newline='', encoding='utf-8')
    review_file = open(review_path, 'w', newline='', encoding='utf-8')
    
    high_writer = csv.DictWriter(high_file, fieldnames=output_columns)
    medium_writer = csv.DictWriter(medium_file, fieldnames=output_columns)
    low_writer = csv.DictWriter(low_file, fieldnames=output_columns)
    review_writer = csv.DictWriter(review_file, fieldnames=output_columns)
    
    high_writer.writeheader()
    medium_writer.writeheader()
    low_writer.writeheader()
    review_writer.writeheader()
    
    # Process rows
    counts = {"high": 0, "medium": 0, "low": 0, "review": 0}
    
    print(f"Processing {input_path}...")
    if is_enriched:
        print("Enriched file detected - email/phone scoring enabled")
    print()
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):
            # Classify for note broker
            classification, engagement_score, why_flagged = classify_for_note_broker(row)
            
            # Add refinement fields
            enriched_row = dict(row)
            enriched_row["engagement_score"] = str(engagement_score)
            enriched_row["engagement_reason"] = why_flagged
            
            equity = estimate_equity(row)
            enriched_row["equity_estimate"] = f"{equity:,.0f}" if equity else ""
            
            enriched_row["has_email"] = "Y" if has_email(row) else "N"
            enriched_row["has_phone"] = "Y" if has_phone(row) else "N"
            
            # Contact quality
            contact_quality = []
            if has_email(row):
                contact_quality.append("EMAIL")
            if has_phone(row):
                contact_quality.append("PHONE")
            if is_street_address(row.get("mailing_address", "")):
                contact_quality.append("STREET_ADDRESS")
            enriched_row["contact_quality"] = " | ".join(contact_quality) if contact_quality else "NONE"
            
            # Write to appropriate file
            if classification == "HIGH_PRIORITY":
                high_writer.writerow(enriched_row)
                counts["high"] += 1
            elif classification == "MEDIUM_PRIORITY":
                medium_writer.writerow(enriched_row)
                counts["medium"] += 1
            elif classification == "LOW_PRIORITY":
                low_writer.writerow(enriched_row)
                counts["low"] += 1
            else:  # REVIEW
                review_writer.writerow(enriched_row)
                counts["review"] += 1
            
            # Progress update
            if row_num % 50000 == 0:
                print(f"Processed {row_num:,} rows... (high: {counts['high']:,}, "
                      f"medium: {counts['medium']:,}, low: {counts['low']:,}, review: {counts['review']:,})")
    
    # Close files
    high_file.close()
    medium_file.close()
    low_file.close()
    review_file.close()
    
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Refine property targets for note broker outreach",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to input targets CSV (e.g., lee_arnold_favorites.csv)"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    
    parser.add_argument(
        "--enriched",
        action="store_true",
        help="Input file is already enriched with email/phone columns"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    output_dir = Path(args.outdir)
    
    print("=" * 70)
    print("NOTE BROKER REFINEMENT FILTER")
    print("=" * 70)
    print()
    print("Additional filters applied:")
    print("  - Contact information quality (email, phone, address)")
    print("  - Engagement likelihood scoring")
    print("  - Note buying sweet spot ($150k-$400k)")
    print("  - Equity estimation")
    print("  - Absentee owner strength")
    print("  - Multiple property ownership")
    print()
    print(f"Input file: {input_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    counts = process_targets_file(input_path, output_dir, is_enriched=args.enriched)
    
    print()
    print("=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {sum(counts.values()):,}")
    print(f"  [HIGH]   High Priority:  {counts['high']:,} -> output/note_broker_high_priority.csv")
    print(f"  [MEDIUM] Medium Priority: {counts['medium']:,} -> output/note_broker_medium_priority.csv")
    print(f"  [LOW]    Low Priority:   {counts['low']:,} -> output/note_broker_low_priority.csv")
    print(f"  [?]      Review:          {counts['review']:,} -> output/note_broker_review.csv")
    print()
    print("Next Steps:")
    print("  1. Send HIGH_PRIORITY to enrichment provider for emails/phones")
    print("  2. Review MEDIUM_PRIORITY for potential (needs enrichment)")
    print("  3. Use engagement_score to prioritize outreach")
    print("  4. Focus on properties with equity_estimate > $100k")
    print()


if __name__ == "__main__":
    main()

