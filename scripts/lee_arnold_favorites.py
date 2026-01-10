"""
Lee Arnold Favorites Filter

Filters property targets for Lee Arnold's specific criteria:
- Single family homes, duplex, triplex, quadplex
- Max sq ft 2800 (NOTE: sq ft not in data, will need manual review)
- Price ARV not to exceed FHA cap (default: $498,257 for Travis County, TX)
- Max 5 bedroom (NOTE: bedrooms not in data, will need manual review)
- Max 3 bathroom (NOTE: bathrooms not in data, will need manual review)
- No more than 1/2 acre (NOTE: lot size not in data, estimated from land_value)
- Exclude complex owner names (#8)
- Exclude vacant land (#2 - zero improvement value)
- Highlight multiple property owners (#5) but don't exclude 1-2 properties owned

Usage:
    python scripts/lee_arnold_favorites.py --tcad output/prop_clean.csv --outdir output
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple, Set
from collections import Counter, defaultdict


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# FHA loan limits by county (2024-2025)
# Default to Travis County, TX: $498,257
DEFAULT_FHA_CAP = 498257

# Max square footage (for manual review - not in data)
MAX_SQ_FT = 2800

# Max bedrooms (for manual review - not in data)
MAX_BEDROOMS = 5

# Max bathrooms (for manual review - not in data)
MAX_BATHROOMS = 3

# Max lot size in acres (estimated from land value)
# Rough estimate: $50k land value ≈ 0.1 acres in urban areas
# $250k land value ≈ 0.5 acres
MAX_LAND_VALUE_FOR_HALF_ACRE = 250000  # Rough estimate

# Institutional owner keywords (exclude)
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

# Property type codes for residential (single family, duplex, triplex, quadplex)
# R = Residential (general)
# Need to check actual codes, but typically:
# R1, R2, R3 = Single family
# R4, R5 = Duplex/Triplex/Quadplex
RESIDENTIAL_PROPERTY_TYPES = ["R", "R1", "R2", "R3", "R4", "R5"]


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


def is_institutional_owner(owner_name: str) -> bool:
    """Check if owner name matches institutional/bank keywords."""
    if not owner_name:
        return False
    
    owner_upper = owner_name.upper()
    return any(keyword in owner_upper for keyword in INSTITUTIONAL_KEYWORDS)


def is_complex_owner_name(owner_name: str) -> bool:
    """
    Check if owner name is too complex (#8 - exclude these).
    
    Complex indicators:
    - Very long (> 80 chars)
    - Multiple special patterns
    - Complex trust structures
    """
    if not owner_name:
        return False
    
    # Very long names are complex
    if len(owner_name) > 80:
        return True
    
    name_upper = owner_name.upper()
    
    # Complex trust patterns
    complex_patterns = [
        "TRUST DATED",
        "REVOCABLE TRUST",
        "IRREVOCABLE TRUST",
        "FAMILY TRUST DATED",
        "ESTATE OF",
        "HEIRS OF",
        "UNKNOWN",
        "ET AL",
        "ETAL",
        "ETC",
        "ETC.",
        "ET CETERA"
    ]
    
    if any(pattern in name_upper for pattern in complex_patterns):
        return True
    
    # Multiple commas or special characters suggest complexity
    if owner_name.count(",") > 2:
        return True
    
    return False


def is_vacant_land(row: Dict[str, str]) -> bool:
    """
    Check if property is vacant land (#2 - exclude).
    
    Vacant land indicators:
    - Zero or very low improvement value
    """
    improvement_value = parse_amount(row.get("improvement_value", ""))
    
    if improvement_value is None or improvement_value == 0:
        return True
    
    # Very low improvement value (< $10k) likely vacant or minimal structure
    if improvement_value < 10000:
        return True
    
    return False


def is_residential_property(property_type: str) -> bool:
    """Check if property type is residential (single family, duplex, triplex, quadplex)."""
    if not property_type:
        return False
    
    property_type_upper = property_type.strip().upper()
    
    # Check if it's in our residential list
    if property_type_upper in RESIDENTIAL_PROPERTY_TYPES:
        return True
    
    # Also check if it starts with R (residential)
    if property_type_upper.startswith("R"):
        return True
    
    return False


def estimate_lot_size_from_land_value(land_value: Optional[float]) -> Optional[float]:
    """
    Rough estimate of lot size from land value.
    
    This is a very rough estimate and varies by location.
    Assumes: $50k land value ≈ 0.1 acres in urban areas
    """
    if land_value is None:
        return None
    
    # Rough estimate: $500k land value ≈ 1 acre
    # Adjust based on your market
    estimated_acres = land_value / 500000
    
    return estimated_acres


def generate_lead_id(owner_name: str, account_id: str, mailing_zip: str) -> str:
    """Generate deterministic lead_id using SHA1 hash."""
    owner_name = (owner_name or "").strip().upper()
    account_id = (account_id or "").strip()
    mailing_zip = (mailing_zip or "").strip()
    
    hash_input = f"{owner_name}|{account_id}|{mailing_zip}"
    hash_obj = hashlib.sha1(hash_input.encode('utf-8'))
    return hash_obj.hexdigest()


def calculate_lead_score(
    total_value: Optional[float],
    fha_cap: float,
    is_absentee: bool,
    property_count: int
) -> Tuple[int, str]:
    """
    Calculate lead score (0-100) and why_flagged summary.
    """
    score = 0
    reasons = []
    
    # Value scoring (prefer properties under FHA cap)
    if total_value:
        if total_value <= fha_cap:
            score += 40
            reasons.append("under FHA cap")
        else:
            score += 20
            reasons.append("over FHA cap")
    else:
        score -= 10
        reasons.append("missing value")
    
    # Absentee owner scoring
    if is_absentee:
        score += 30
        reasons.append("absentee owner")
    else:
        score += 15
        reasons.append("owner occupied")
    
    # Multiple property ownership (highlight but don't exclude)
    if property_count >= 3:
        score += 20
        reasons.append(f"{property_count} properties")
    elif property_count == 2:
        score += 10
        reasons.append("2 properties")
    else:
        score += 5
        reasons.append("1 property")
    
    # Cap score at 100
    score = max(0, min(100, score))
    
    why_flagged = " + ".join(reasons[:4])
    
    return (score, why_flagged)


def normalize_address(address: str) -> str:
    """Normalize address for comparison."""
    if not address:
        return ""
    normalized = address.upper().strip()
    normalized = normalized.replace("STREET", "ST")
    normalized = normalized.replace("AVENUE", "AVE")
    normalized = normalized.replace("ROAD", "RD")
    normalized = normalized.replace("DRIVE", "DR")
    normalized = normalized.replace("BOULEVARD", "BLVD")
    normalized = normalized.replace(".", "")
    normalized = normalized.replace(",", "")
    normalized = " ".join(normalized.split())
    return normalized


def is_absentee_owner(row: Dict[str, str]) -> bool:
    """Check if owner is absentee (mailing address != situs address)."""
    mailing = normalize_address(row.get("mailing_address", ""))
    situs = normalize_address(row.get("situs_address", ""))
    
    if not mailing or not situs:
        return False
    
    return mailing != situs


def classify_property_row(
    row: Dict[str, str],
    fha_cap: float,
    property_count: int
) -> Tuple[str, str, int, str]:
    """
    Classify a property row for Lee Arnold Favorites.
    
    Returns:
        Tuple of (classification, owner_type, lead_score, why_flagged)
    """
    owner_name = row.get("owner_name", "").strip()
    property_type = row.get("property_type", "").strip()
    
    # Hard-exclude institutional owners
    if is_institutional_owner(owner_name):
        return ("DISCARD", "INSTITUTIONAL", 0, "Institutional/bank owner")
    
    # Exclude complex owner names (#8)
    if is_complex_owner_name(owner_name):
        return ("DISCARD", "COMPLEX", 0, "Complex owner name")
    
    # Exclude vacant land (#2)
    if is_vacant_land(row):
        return ("DISCARD", "VACANT", 0, "Vacant land or minimal improvement")
    
    # Must be residential property type
    if not is_residential_property(property_type):
        return ("DISCARD", "NON_RESIDENTIAL", 0, f"Non-residential property type: {property_type}")
    
    # Check property value (FHA cap)
    total_value = parse_amount(row.get("total_value", ""))
    
    if total_value is None or total_value <= 0:
        return ("REVIEW", "UNKNOWN", 0, "Missing or invalid total_value")
    
    if total_value > fha_cap * 1.1:  # Allow 10% buffer for review
        return ("REVIEW", "VALUE", 0, f"Value exceeds FHA cap ({total_value:,.0f} > {fha_cap:,.0f})")
    
    # Check land value (estimate lot size)
    land_value = parse_amount(row.get("land_value", ""))
    if land_value and land_value > MAX_LAND_VALUE_FOR_HALF_ACRE:
        estimated_acres = estimate_lot_size_from_land_value(land_value)
        if estimated_acres and estimated_acres > 0.6:  # Slightly over 0.5 for buffer
            return ("REVIEW", "LOT_SIZE", 0, f"Lot size may exceed 0.5 acres (estimated {estimated_acres:.2f} acres)")
    
    # Calculate lead score
    is_absentee = is_absentee_owner(row)
    lead_score, why_flagged = calculate_lead_score(
        total_value, fha_cap, is_absentee, property_count
    )
    
    # Determine owner type
    owner_type = "PERSON"
    if "LLC" in owner_name.upper():
        owner_type = "LLC"
    elif "TRUST" in owner_name.upper():
        owner_type = "TRUST"
    
    # All checks passed - this is a TARGET
    return ("TARGET", owner_type, lead_score, why_flagged)


def build_target_row(row: Dict[str, str], owner_type: str, lead_score: int, why_flagged: str, property_count: int) -> Dict[str, str]:
    """Build output row for target."""
    owner_name = row.get("owner_name", "").strip()
    account_id = row.get("account_id", "").strip()
    mailing_zip = row.get("mailing_zip", "").strip()
    
    lead_id = generate_lead_id(owner_name, account_id, mailing_zip)
    
    full_name = owner_name if owner_type == "PERSON" else ""
    company_name = owner_name if owner_type in ["LLC", "TRUST"] else ""
    
    is_absentee = is_absentee_owner(row)
    owner_occupied_guess = "N" if is_absentee else "Y"
    
    # Add notes about missing data
    notes = []
    notes.append("SQ_FT_REVIEW")  # Square footage not in data
    notes.append("BED_BATH_REVIEW")  # Bedrooms/bathrooms not in data
    
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
        "property_count": str(property_count),  # Highlight multiple property owners
        "lead_score": str(lead_score),
        "why_flagged": why_flagged,
        "data_limitations": " | ".join(notes)  # Note what data is missing
    }
    
    return output_row


def count_properties_per_owner(tcad_path: Path) -> Dict[str, int]:
    """
    First pass: Count properties per owner to highlight multiple property owners (#5).
    
    Returns:
        Dictionary mapping normalized owner name -> property count
    """
    print("Counting properties per owner...")
    owner_counts = defaultdict(int)
    
    with open(tcad_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            owner_name = row.get("owner_name", "").strip().upper()
            if owner_name:
                owner_counts[owner_name] += 1
    
    print(f"Found {len(owner_counts):,} unique owners")
    return owner_counts


def process_tcad_file(
    tcad_path: Path,
    output_dir: Path,
    fha_cap: float
) -> Dict[str, int]:
    """
    Process TCAD file and generate Lee Arnold Favorites.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    targets_path = output_dir / "lee_arnold_favorites.csv"
    review_path = output_dir / "lee_arnold_favorites_review.csv"
    discard_path = output_dir / "lee_arnold_favorites_discarded.csv"
    
    # First pass: Count properties per owner
    owner_counts = count_properties_per_owner(tcad_path)
    print()
    
    # Define output columns
    output_columns = [
        "lead_id", "full_name", "company_name", "owner_type",
        "mailing_address", "mailing_city", "mailing_state", "mailing_zip",
        "situs_address", "situs_city", "situs_state", "situs_zip",
        "tcad_account_id", "owner_occupied_guess", "total_value",
        "property_type", "property_count", "lead_score", "why_flagged",
        "data_limitations"
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
    
    # Process rows
    counts = {"targets": 0, "review": 0, "discarded": 0}
    owner_type_counter = Counter()
    
    print(f"Processing {tcad_path}...")
    print(f"FHA Cap: ${fha_cap:,.0f}")
    print(f"Max SQ FT: {MAX_SQ_FT} (NOTE: Not in data, requires manual review)")
    print(f"Max Bedrooms: {MAX_BEDROOMS} (NOTE: Not in data, requires manual review)")
    print(f"Max Bathrooms: {MAX_BATHROOMS} (NOTE: Not in data, requires manual review)")
    print()
    
    with open(tcad_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):
            owner_name = row.get("owner_name", "").strip().upper()
            property_count = owner_counts.get(owner_name, 1)
            
            # Classify row
            classification, owner_type, lead_score, why_flagged = classify_property_row(
                row, fha_cap, property_count
            )
            
            owner_type_counter[owner_type] += 1
            
            # Build output row
            target_row = build_target_row(row, owner_type, lead_score, why_flagged, property_count)
            
            if classification == "TARGET":
                targets_writer.writerow(target_row)
                counts["targets"] += 1
            elif classification == "REVIEW":
                review_writer.writerow(target_row)
                counts["review"] += 1
            else:  # DISCARD
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
    
    return counts, owner_type_counter


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Lee Arnold Favorites - Filter properties for specific investment criteria",
        formatter_class=argparse.RawDescriptionHelpFormatter
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
        "--fha_cap",
        type=float,
        default=DEFAULT_FHA_CAP,
        help=f"FHA loan limit cap (default: {DEFAULT_FHA_CAP:,.0f} for Travis County, TX)"
    )
    
    args = parser.parse_args()
    
    tcad_path = Path(args.tcad)
    if not tcad_path.exists():
        print(f"Error: TCAD file not found: {tcad_path}", file=sys.stderr)
        sys.exit(1)
    
    output_dir = Path(args.outdir)
    
    print("=" * 70)
    print("LEE ARNOLD FAVORITES FILTER")
    print("=" * 70)
    print()
    print("Criteria:")
    print(f"  - Single family, duplex, triplex, quadplex")
    print(f"  - Max sq ft: {MAX_SQ_FT} (NOTE: Not in data, requires manual review)")
    print(f"  - Max ARV: ${args.fha_cap:,.0f} (FHA cap)")
    print(f"  - Max bedrooms: {MAX_BEDROOMS} (NOTE: Not in data, requires manual review)")
    print(f"  - Max bathrooms: {MAX_BATHROOMS} (NOTE: Not in data, requires manual review)")
    print(f"  - Max lot size: 0.5 acres (estimated from land value)")
    print(f"  - Exclude complex owner names")
    print(f"  - Exclude vacant land")
    print(f"  - Highlight multiple property owners (but don't exclude 1-2 properties)")
    print()
    print(f"TCAD file: {tcad_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    counts, owner_type_counter = process_tcad_file(
        tcad_path=tcad_path,
        output_dir=output_dir,
        fha_cap=args.fha_cap
    )
    
    print()
    print("=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {sum(counts.values()):,}")
    print(f"  [OK] Targets:     {counts['targets']:,} -> output/lee_arnold_favorites.csv")
    print(f"  [?]  Review:       {counts['review']:,} -> output/lee_arnold_favorites_review.csv")
    print(f"  [X]  Discarded:    {counts['discarded']:,} -> output/lee_arnold_favorites_discarded.csv")
    print()
    print("Owner Type Distribution:")
    total_classified = sum(owner_type_counter.values())
    for owner_type in ["PERSON", "LLC", "TRUST", "INSTITUTIONAL", "VACANT", "COMPLEX", "NON_RESIDENTIAL"]:
        count = owner_type_counter.get(owner_type, 0)
        if count > 0:
            pct = (count / total_classified * 100) if total_classified > 0 else 0
            print(f"  {owner_type:20s}: {count:8,} ({pct:5.1f}%)")
    print()
    print("IMPORTANT NOTES:")
    print("  - Square footage, bedrooms, and bathrooms are NOT in the data")
    print("  - These require manual review of the targets")
    print("  - Property count is included to highlight multiple property owners")
    print()


if __name__ == "__main__":
    main()



