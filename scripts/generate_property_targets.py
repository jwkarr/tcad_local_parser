"""
Generate Property Targets from TCAD Data

TCAD-only pipeline to generate a large outreach list of non-bank, private-owner targets.
These are NOT verified note holders - they are property owners who may be potential targets.

Usage:
    python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output --min_value 150000 --max_value 600000 --only_absentee true
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
from collections import Counter


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Default value range
DEFAULT_MIN_VALUE = 150000
DEFAULT_MAX_VALUE = 600000

# Bank-like / institutional owner keywords (exclude these)
INSTITUTIONAL_KEYWORDS = [
    "BANK", "N.A.", "NA", "MORTGAGE", "SERVICING",
    "FNMA", "FANNIE MAE", "FANNIE",
    "FREDDIE MAC", "FREDDIE", "FHLMC",
    "HUD", "VA", "USDA",
    "CREDIT UNION",
    "ASSOCIATION",
    "TRUST COMPANY",
    "WELLS FARGO", "WELLS",
    "JPMORGAN", "JP MORGAN", "CHASE",
    "BANK OF AMERICA", "BANK OF AMER", "BOA",
    "CITIBANK", "CITI",
    "U.S. BANK", "US BANK", "USBANK",
    "PNC", "TRUIST",
    "CAPITAL ONE", "CAPITALONE",
    "REGIONS BANK", "REGIONS",
    "SUNTRUST",
    "BB&T", "BBT",
    "TD BANK", "HSBC",
    "MORGAN STANLEY", "GOLDMAN SACHS",
    "MERRILL LYNCH", "FIDELITY", "VANGUARD"
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def normalize_address(address: str) -> str:
    """Normalize address for comparison."""
    if not address:
        return ""
    # Remove common variations
    normalized = address.upper().strip()
    normalized = normalized.replace("STREET", "ST")
    normalized = normalized.replace("AVENUE", "AVE")
    normalized = normalized.replace("ROAD", "RD")
    normalized = normalized.replace("DRIVE", "DR")
    normalized = normalized.replace("BOULEVARD", "BLVD")
    normalized = normalized.replace(".", "")
    normalized = normalized.replace(",", "")
    normalized = " ".join(normalized.split())  # Normalize whitespace
    return normalized


def is_institutional_owner(owner_name: str) -> bool:
    """Check if owner name matches institutional/bank keywords."""
    if not owner_name:
        return False
    
    owner_upper = owner_name.upper()
    return any(keyword in owner_upper for keyword in INSTITUTIONAL_KEYWORDS)


def detect_owner_type(owner_name: str) -> str:
    """
    Detect owner type: PERSON, TRUST, LLC, or UNKNOWN.
    
    Args:
        owner_name: Owner name
        
    Returns:
        Owner type string
    """
    if not owner_name or not owner_name.strip():
        return "UNKNOWN"
    
    name_upper = owner_name.upper().strip()
    
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
    if "," in owner_name:
        return "PERSON"
    
    # If short and no corporate indicators, likely a person
    if len(owner_name) < 50 and not any(keyword in name_upper for keyword in 
                                       ["COMPANY", "GROUP", "HOLDINGS", "PROPERTIES", "INVESTMENTS"]):
        return "PERSON"
    
    return "UNKNOWN"


def parse_amount(amount_str: str) -> Optional[float]:
    """Parse amount from string."""
    if not amount_str or not amount_str.strip():
        return None
    
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def is_absentee_owner(row: Dict[str, str]) -> bool:
    """Check if owner is absentee (mailing address != situs address)."""
    mailing = normalize_address(row.get("mailing_address", ""))
    situs = normalize_address(row.get("situs_address", ""))
    
    if not mailing or not situs:
        return False
    
    return mailing != situs


def calculate_lead_score(
    owner_type: str,
    total_value: Optional[float],
    is_absentee: bool,
    min_value: float,
    max_value: float
) -> Tuple[int, str]:
    """
    Calculate lead score (0-100) and why_flagged summary.
    
    Args:
        owner_type: PERSON, TRUST, LLC, UNKNOWN
        total_value: Property total value
        is_absentee: Whether owner is absentee
        min_value: Minimum value threshold
        max_value: Maximum value threshold
        
    Returns:
        Tuple of (score, why_flagged)
    """
    score = 0
    reasons = []
    
    # Owner type scoring
    if owner_type == "PERSON":
        score += 40
        reasons.append("person owner")
    elif owner_type == "LLC":
        score += 30
        reasons.append("LLC owner")
    elif owner_type == "TRUST":
        score += 25
        reasons.append("trust owner")
    else:
        score += 10
        reasons.append("unknown owner type")
    
    # Absentee owner scoring
    if is_absentee:
        score += 30
        reasons.append("absentee owner")
    else:
        score += 10
        reasons.append("owner occupied")
    
    # Value scoring
    if total_value:
        if min_value <= total_value <= max_value:
            score += 30
            reasons.append("value in range")
        elif total_value < min_value:
            score += 10
            reasons.append("below min value")
        else:
            score += 15
            reasons.append("above max value")
    else:
        score -= 10
        reasons.append("missing value")
    
    # Cap score at 100
    score = max(0, min(100, score))
    
    # Create why_flagged summary
    why_flagged = " + ".join(reasons[:4])  # Limit to first 4 reasons
    
    return (score, why_flagged)


def generate_lead_id(owner_name: str, account_id: str, mailing_zip: str) -> str:
    """
    Generate deterministic lead_id using SHA1 hash.
    
    Args:
        owner_name: Owner name
        account_id: TCAD account ID
        mailing_zip: Mailing zip code
        
    Returns:
        SHA1 hash as hex string
    """
    # Normalize inputs
    owner_name = (owner_name or "").strip().upper()
    account_id = (account_id or "").strip()
    mailing_zip = (mailing_zip or "").strip()
    
    # Create hash input
    hash_input = f"{owner_name}|{account_id}|{mailing_zip}"
    
    # Generate SHA1 hash
    hash_obj = hashlib.sha1(hash_input.encode('utf-8'))
    return hash_obj.hexdigest()


def classify_property_row(
    row: Dict[str, str],
    min_value: float,
    max_value: float,
    only_absentee: bool
) -> Tuple[str, str, int, str]:
    """
    Classify a property row as TARGET, REVIEW, or DISCARD.
    
    Args:
        row: Property row data
        min_value: Minimum property value
        max_value: Maximum property value
        only_absentee: Only include absentee owners
        
    Returns:
        Tuple of (classification, owner_type, lead_score, why_flagged)
    """
    owner_name = row.get("owner_name", "").strip()
    
    # Hard-exclude institutional owners
    if is_institutional_owner(owner_name):
        return ("DISCARD", "INSTITUTIONAL", 0, "Institutional/bank owner")
    
    # Detect owner type
    owner_type = detect_owner_type(owner_name)
    
    # If UNKNOWN and no other indicators, review
    if owner_type == "UNKNOWN" and not owner_name:
        return ("DISCARD", owner_type, 0, "Missing owner name")
    
    # Check absentee status
    is_absentee = is_absentee_owner(row)
    
    if only_absentee and not is_absentee:
        return ("DISCARD", owner_type, 0, "Not absentee owner (only_absentee=true)")
    
    # Check property value
    total_value_str = row.get("total_value", "")
    total_value = parse_amount(total_value_str)
    
    if total_value is None:
        return ("REVIEW", owner_type, 0, "Missing total_value")
    
    if total_value <= 0:
        return ("DISCARD", owner_type, 0, f"Invalid total_value: {total_value}")
    
    # Value range check
    if total_value < min_value:
        return ("REVIEW", owner_type, 0, f"Value below minimum ({total_value:,.0f} < {min_value:,.0f})")
    
    if total_value > max_value:
        return ("REVIEW", owner_type, 0, f"Value above maximum ({total_value:,.0f} > {max_value:,.0f})")
    
    # Calculate lead score
    lead_score, why_flagged = calculate_lead_score(
        owner_type, total_value, is_absentee, min_value, max_value
    )
    
    # All checks passed - this is a TARGET
    return ("TARGET", owner_type, lead_score, why_flagged)


def build_target_row(row: Dict[str, str], owner_type: str, lead_score: int, why_flagged: str) -> Dict[str, str]:
    """Build output row for target."""
    owner_name = row.get("owner_name", "").strip()
    account_id = row.get("account_id", "").strip()
    mailing_zip = row.get("mailing_zip", "").strip()
    
    # Generate lead_id
    lead_id = generate_lead_id(owner_name, account_id, mailing_zip)
    
    # Determine full_name vs company_name
    full_name = owner_name if owner_type == "PERSON" else ""
    company_name = owner_name if owner_type in ["LLC", "TRUST"] else ""
    
    # Owner occupied guess
    is_absentee = is_absentee_owner(row)
    owner_occupied_guess = "N" if is_absentee else "Y"
    
    output_row = {
        "lead_id": lead_id,
        "full_name": full_name,
        "company_name": company_name,
        "owner_type": owner_type,
        "mailing_address": row.get("mailing_address", "").strip(),
        "mailing_city": row.get("mailing_city", "").strip(),
        "mailing_state": row.get("mailing_state", "").strip(),
        "mailing_zip": mailing_zip,
        "situs_address": row.get("situs_address", "").strip(),
        "situs_city": row.get("situs_city", "").strip(),
        "situs_state": row.get("situs_state", "").strip(),
        "situs_zip": row.get("situs_zip", "").strip(),
        "tcad_account_id": account_id,
        "owner_occupied_guess": owner_occupied_guess,
        "total_value": row.get("total_value", "").strip(),
        "property_type": row.get("property_type", "").strip(),
        "lead_score": str(lead_score),
        "why_flagged": why_flagged
    }
    
    return output_row


def get_value_bucket(total_value: Optional[float]) -> str:
    """Get value bucket (100k steps)."""
    if total_value is None:
        return "unknown"
    
    bucket = int(total_value // 100000) * 100000
    return f"{bucket}k-{bucket+100000}k"


def process_tcad_file(
    tcad_path: Path,
    output_dir: Path,
    min_value: float,
    max_value: float,
    only_absentee: bool,
    enable_bucketing: bool = False
) -> Dict[str, int]:
    """
    Process TCAD file and generate property targets.
    
    Args:
        tcad_path: Path to prop_clean.csv
        output_dir: Output directory
        min_value: Minimum property value
        max_value: Maximum property value
        only_absentee: Only include absentee owners
        enable_bucketing: Enable value bucketing
        
    Returns:
        Dictionary with counts
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    targets_path = output_dir / "property_targets_email_ready.csv"
    review_path = output_dir / "property_targets_review.csv"
    discard_path = output_dir / "property_targets_discarded.csv"
    
    # Define output columns
    output_columns = [
        "lead_id", "full_name", "company_name", "owner_type",
        "mailing_address", "mailing_city", "mailing_state", "mailing_zip",
        "situs_address", "situs_city", "situs_state", "situs_zip",
        "tcad_account_id", "owner_occupied_guess", "total_value",
        "property_type", "lead_score", "why_flagged"
    ]
    
    # Open output files
    targets_file = open(targets_path, 'w', newline='', encoding='utf-8')
    review_file = open(review_path, 'w', newline='', encoding='utf-8')
    discard_file = open(discard_path, 'w', newline='', encoding='utf-8')
    
    targets_writer = csv.DictWriter(targets_file, fieldnames=output_columns)
    review_writer = csv.DictWriter(review_file, fieldnames=output_columns)
    discard_writer = csv.DictWriter(discard_file, fieldnames=output_columns)
    
    targets_writer.writeheader()
    review_writer.writeheader()
    discard_writer.writeheader()
    
    # Bucketing files (if enabled)
    bucket_files = {}
    bucket_writers = {}
    if enable_bucketing:
        for bucket_start in range(0, int(max_value), 100000):
            bucket_end = bucket_start + 100000
            if bucket_start < min_value:
                continue
            bucket_name = f"{bucket_start}k-{bucket_end}k"
            bucket_path = output_dir / f"property_targets_{bucket_name}.csv"
            bucket_file = open(bucket_path, 'w', newline='', encoding='utf-8')
            bucket_writer = csv.DictWriter(bucket_file, fieldnames=output_columns)
            bucket_writer.writeheader()
            bucket_files[bucket_name] = bucket_file
            bucket_writers[bucket_name] = bucket_writer
    
    # Process rows
    counts = {"targets": 0, "review": 0, "discarded": 0}
    owner_type_counter = Counter()
    
    print(f"Processing {tcad_path}...")
    print(f"Filters: min_value={min_value:,.0f}, max_value={max_value:,.0f}, only_absentee={only_absentee}")
    print()
    
    with open(tcad_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # Classify row
            classification, owner_type, lead_score, why_flagged = classify_property_row(
                row, min_value, max_value, only_absentee
            )
            
            # Count owner types
            owner_type_counter[owner_type] += 1
            
            # Build output row
            if classification == "TARGET":
                target_row = build_target_row(row, owner_type, lead_score, why_flagged)
                targets_writer.writerow(target_row)
                counts["targets"] += 1
                
                # Write to bucket file if enabled
                if enable_bucketing:
                    total_value = parse_amount(row.get("total_value", ""))
                    bucket = get_value_bucket(total_value)
                    if bucket in bucket_writers:
                        bucket_writers[bucket].writerow(target_row)
                
            elif classification == "REVIEW":
                target_row = build_target_row(row, owner_type, lead_score, why_flagged)
                review_writer.writerow(target_row)
                counts["review"] += 1
            else:  # DISCARD
                target_row = build_target_row(row, owner_type, lead_score, why_flagged)
                discard_writer.writerow(target_row)
                counts["discarded"] += 1
            
            # Progress update
            if row_num % 100000 == 0:
                print(f"Processed {row_num:,} rows... (targets: {counts['targets']:,}, "
                      f"review: {counts['review']:,}, discarded: {counts['discarded']:,})")
    
    # Close files
    targets_file.close()
    review_file.close()
    discard_file.close()
    
    if enable_bucketing:
        for bucket_file in bucket_files.values():
            bucket_file.close()
    
    return counts, owner_type_counter


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate property targets from TCAD data (NOT verified note holders)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output
  
  # With custom value range and absentee-only
  python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output --min_value 150000 --max_value 600000 --only_absentee true
  
  # With value bucketing
  python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output --enable_bucketing true
        """
    )
    
    parser.add_argument(
        "--tcad",
        type=str,
        required=True,
        help="Path to TCAD prop_clean.csv file"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    
    parser.add_argument(
        "--min_value",
        type=float,
        default=DEFAULT_MIN_VALUE,
        help=f"Minimum property value (default: {DEFAULT_MIN_VALUE:,.0f})"
    )
    
    parser.add_argument(
        "--max_value",
        type=float,
        default=DEFAULT_MAX_VALUE,
        help=f"Maximum property value (default: {DEFAULT_MAX_VALUE:,.0f})"
    )
    
    parser.add_argument(
        "--only_absentee",
        type=str,
        default="false",
        help="Only include absentee owners (true/false, default: false)"
    )
    
    parser.add_argument(
        "--enable_bucketing",
        type=str,
        default="false",
        help="Enable value bucketing into 100k steps (true/false, default: false)"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    tcad_path = Path(args.tcad)
    if not tcad_path.exists():
        print(f"Error: TCAD file not found: {tcad_path}", file=sys.stderr)
        sys.exit(1)
    
    output_dir = Path(args.outdir)
    
    # Parse boolean flags
    only_absentee = args.only_absentee.lower() in ["true", "1", "yes"]
    enable_bucketing = args.enable_bucketing.lower() in ["true", "1", "yes"]
    
    print("=" * 70)
    print("PROPERTY TARGETS GENERATOR (TCAD-ONLY)")
    print("=" * 70)
    print("NOTE: These are property owner targets, NOT verified note holders.")
    print()
    print(f"TCAD file: {tcad_path}")
    print(f"Output directory: {output_dir}")
    print(f"Min value: ${args.min_value:,.0f}")
    print(f"Max value: ${args.max_value:,.0f}")
    print(f"Only absentee: {only_absentee}")
    print(f"Value bucketing: {enable_bucketing}")
    print()
    
    counts, owner_type_counter = process_tcad_file(
        tcad_path=tcad_path,
        output_dir=output_dir,
        min_value=args.min_value,
        max_value=args.max_value,
        only_absentee=only_absentee,
        enable_bucketing=enable_bucketing
    )
    
    print()
    print("=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {sum(counts.values()):,}")
    print(f"  [OK] Targets:     {counts['targets']:,} -> output/property_targets_email_ready.csv")
    print(f"  [?]  Review:       {counts['review']:,} -> output/property_targets_review.csv")
    print(f"  [X]  Discarded:    {counts['discarded']:,} -> output/property_targets_discarded.csv")
    print()
    
    # Owner type breakdown
    print("Owner Type Distribution:")
    total_classified = sum(owner_type_counter.values())
    for owner_type in ["PERSON", "LLC", "TRUST", "UNKNOWN", "INSTITUTIONAL"]:
        count = owner_type_counter.get(owner_type, 0)
        pct = (count / total_classified * 100) if total_classified > 0 else 0
        print(f"  {owner_type:15s}: {count:8,} ({pct:5.1f}%)")
    print()
    
    if enable_bucketing:
        print("Value bucket files created in output/ directory")
    print()


if __name__ == "__main__":
    main()



