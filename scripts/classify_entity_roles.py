"""
Entity Role Classification for Note Broker Pipeline

Extends the note broker refinement pipeline to add role-based and entity-based
segmentation using owner_name / full_name heuristics.

Classifies entities into:
- INVESTOR_ENTITY
- TRUST_ENTITY
- INDIVIDUAL_PERSON
- LEGAL_ENTITY
- BANK_INSTITUTION
- GOVERNMENT_ENTITY
- UTILITY_OR_NONPROFIT
- UNKNOWN

Applies role-based score modifications and routes to appropriate output files.

Usage:
    python scripts/classify_entity_roles.py
"""

import csv
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
from collections import Counter


# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Role-based score modifiers
SCORE_MODIFIER_INVESTOR = 20
SCORE_MODIFIER_TRUST = 15
SCORE_MODIFIER_INDIVIDUAL = 10
SCORE_MODIFIER_LEGAL = 5
SCORE_MODIFIER_BANK = None  # Force discard
SCORE_MODIFIER_GOV = None   # Force discard
SCORE_MODIFIER_UTILITY = None  # Force discard
SCORE_MODIFIER_UNKNOWN = 0

# Keyword heuristics (case-insensitive)
INVESTOR_KEYWORDS = [
    "INVEST", "CAPITAL", "HOLDINGS", "EQUITY", "PARTNERS", "GROUP",
    "VENTURES", "PROPERTIES", "REAL ESTATE", "DEVELOPMENT"
]

TRUST_KEYWORDS = [
    "TRUST", "ESTATE", "FAMILY TRUST", "LIVING TRUST"
]

LEGAL_KEYWORDS = [
    "LAW", "ATTORNEY", "ESQ", "LEGAL", "COUNSEL", "PLLC"
]

BANK_KEYWORDS = [
    "BANK", "N.A.", "CREDIT UNION", "MORTGAGE", "SERVICING",
    "FNMA", "FREDDIE", "FHA", "VA", "USDA"
]

GOV_KEYWORDS = [
    "CITY OF", "COUNTY OF", "STATE OF", "AUTHORITY", "DISTRICT"
]

UTILITY_KEYWORDS = [
    "UTILITY", "ELECTRIC", "WATER", "SEWER", "FOUNDATION", "CHURCH"
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_entity_name(row: Dict[str, str]) -> str:
    """Get entity name from full_name or company_name."""
    full_name = row.get("full_name", "").strip()
    company_name = row.get("company_name", "").strip()
    
    # Prefer company_name if available, otherwise full_name
    return company_name if company_name else full_name


def contains_keyword(text: str, keywords: list) -> bool:
    """Check if text contains any keyword (case-insensitive)."""
    if not text:
        return False
    
    text_upper = text.upper()
    for keyword in keywords:
        if keyword in text_upper:
            return True
    
    return False


def classify_entity_role(row: Dict[str, str]) -> Tuple[str, str]:
    """
    Classify entity role based on name heuristics.
    
    Returns:
        Tuple of (entity_role, role_reason)
    """
    entity_name = get_entity_name(row)
    
    if not entity_name:
        return ("UNKNOWN", "no name provided")
    
    entity_name_upper = entity_name.upper()
    
    # Check in priority order (most specific first)
    
    # BANK - check early as it's a strong exclusion
    if contains_keyword(entity_name, BANK_KEYWORDS):
        matched = [kw for kw in BANK_KEYWORDS if kw in entity_name_upper]
        return ("BANK_INSTITUTION", f"bank keyword: {matched[0]}")
    
    # GOVERNMENT - check early as it's a strong exclusion
    if contains_keyword(entity_name, GOV_KEYWORDS):
        matched = [kw for kw in GOV_KEYWORDS if kw in entity_name_upper]
        return ("GOVERNMENT_ENTITY", f"government keyword: {matched[0]}")
    
    # UTILITY - check early as it's a strong exclusion
    if contains_keyword(entity_name, UTILITY_KEYWORDS):
        matched = [kw for kw in UTILITY_KEYWORDS if kw in entity_name_upper]
        return ("UTILITY_OR_NONPROFIT", f"utility keyword: {matched[0]}")
    
    # TRUST - check before INVESTOR as trusts might contain "INVEST"
    if contains_keyword(entity_name, TRUST_KEYWORDS):
        matched = [kw for kw in TRUST_KEYWORDS if kw in entity_name_upper]
        return ("TRUST_ENTITY", f"trust keyword: {matched[0]}")
    
    # INVESTOR
    if contains_keyword(entity_name, INVESTOR_KEYWORDS):
        matched = [kw for kw in INVESTOR_KEYWORDS if kw in entity_name_upper]
        return ("INVESTOR_ENTITY", f"investor keyword: {matched[0]}")
    
    # LEGAL
    if contains_keyword(entity_name, LEGAL_KEYWORDS):
        matched = [kw for kw in LEGAL_KEYWORDS if kw in entity_name_upper]
        return ("LEGAL_ENTITY", f"legal keyword: {matched[0]}")
    
    # Check if it looks like an individual person
    # Simple heuristic: if it has common person name patterns
    # and doesn't have entity indicators
    owner_type = row.get("owner_type", "").upper()
    
    # Check for common entity indicators
    entity_indicators = ["LLC", "INC", "CORP", "LTD", "LP", "LLP", "PC", "PA", "CO", "COMPANY"]
    has_entity_indicator = any(indicator in entity_name_upper for indicator in entity_indicators)
    
    # If owner_type is PERSON and no entity indicators, likely individual
    if owner_type == "PERSON" and not has_entity_indicator:
        # Double-check: make sure it doesn't have entity keywords
        if not contains_keyword(entity_name, INVESTOR_KEYWORDS + TRUST_KEYWORDS + LEGAL_KEYWORDS):
            return ("INDIVIDUAL_PERSON", "owner_type=PERSON, no entity keywords")
    
    # If no entity indicators and looks like a person name (has common name patterns)
    # Common patterns: "FIRST LAST", "FIRST & FIRST", "LAST, FIRST"
    if not has_entity_indicator:
        # Simple check: if it contains "&" or "," it might be multiple people
        # If it's short and doesn't have entity keywords, might be individual
        name_parts = entity_name.split()
        if len(name_parts) <= 5:  # Reasonable name length
            if not contains_keyword(entity_name, INVESTOR_KEYWORDS + TRUST_KEYWORDS + LEGAL_KEYWORDS):
                if owner_type == "PERSON":
                    return ("INDIVIDUAL_PERSON", "appears to be individual person")
    
    # Default to UNKNOWN if we can't classify
    return ("UNKNOWN", "no matching keywords or patterns")


def get_role_score_modifier(entity_role: str) -> Optional[int]:
    """Get score modifier for entity role."""
    modifiers = {
        "INVESTOR_ENTITY": SCORE_MODIFIER_INVESTOR,
        "TRUST_ENTITY": SCORE_MODIFIER_TRUST,
        "INDIVIDUAL_PERSON": SCORE_MODIFIER_INDIVIDUAL,
        "LEGAL_ENTITY": SCORE_MODIFIER_LEGAL,
        "BANK_INSTITUTION": SCORE_MODIFIER_BANK,
        "GOVERNMENT_ENTITY": SCORE_MODIFIER_GOV,
        "UTILITY_OR_NONPROFIT": SCORE_MODIFIER_UTILITY,
        "UNKNOWN": SCORE_MODIFIER_UNKNOWN,
    }
    return modifiers.get(entity_role, 0)


def is_excluded_role(entity_role: str) -> bool:
    """Check if role should be excluded (BANK/GOV/UTILITY)."""
    return entity_role in ["BANK_INSTITUTION", "GOVERNMENT_ENTITY", "UTILITY_OR_NONPROFIT"]


def parse_engagement_score(row: Dict[str, str]) -> int:
    """Parse engagement_score from row."""
    score_str = row.get("engagement_score", "").strip()
    if not score_str:
        return 0
    
    try:
        return int(float(score_str))
    except (ValueError, TypeError):
        return 0


def apply_role_modifier(base_score: int, modifier: Optional[int]) -> int:
    """
    Apply role modifier to base score.
    
    If modifier is None, return -1 to indicate exclusion.
    """
    if modifier is None:
        return -1  # Excluded
    
    new_score = base_score + modifier
    # Cap at 100
    return min(100, max(0, new_score))


def get_output_file_for_role(entity_role: str) -> str:
    """Get output filename for entity role."""
    role_to_file = {
        "INVESTOR_ENTITY": "note_broker_investor_priority.csv",
        "TRUST_ENTITY": "note_broker_investor_priority.csv",  # Group with investors
        "INDIVIDUAL_PERSON": "note_broker_individual_priority.csv",
        "LEGAL_ENTITY": "note_broker_legal_review.csv",
        "BANK_INSTITUTION": "note_broker_excluded_entities.csv",
        "GOVERNMENT_ENTITY": "note_broker_excluded_entities.csv",
        "UTILITY_OR_NONPROFIT": "note_broker_excluded_entities.csv",
        "UNKNOWN": "note_broker_individual_priority.csv",  # Default to individual
    }
    return role_to_file.get(entity_role, "note_broker_individual_priority.csv")


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_entity_classification(
    input_path: Path,
    output_dir: Path
) -> Dict[str, int]:
    """
    Process CSV file and classify entities by role.
    
    Returns:
        Dictionary with counts per role
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output file paths
    investor_path = output_dir / "note_broker_investor_priority.csv"
    individual_path = output_dir / "note_broker_individual_priority.csv"
    legal_path = output_dir / "note_broker_legal_review.csv"
    excluded_path = output_dir / "note_broker_excluded_entities.csv"
    
    # Read input file to get columns
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        input_columns = list(reader.fieldnames or [])
    
    # Add new columns
    output_columns = input_columns + ["entity_role", "role_score_modifier", "role_reason"]
    
    # Open output files
    investor_file = open(investor_path, 'w', newline='', encoding='utf-8')
    individual_file = open(individual_path, 'w', newline='', encoding='utf-8')
    legal_file = open(legal_path, 'w', newline='', encoding='utf-8')
    excluded_file = open(excluded_path, 'w', newline='', encoding='utf-8')
    
    investor_writer = csv.DictWriter(investor_file, fieldnames=output_columns)
    individual_writer = csv.DictWriter(individual_file, fieldnames=output_columns)
    legal_writer = csv.DictWriter(legal_file, fieldnames=output_columns)
    excluded_writer = csv.DictWriter(excluded_file, fieldnames=output_columns)
    
    investor_writer.writeheader()
    individual_writer.writeheader()
    legal_writer.writeheader()
    excluded_writer.writeheader()
    
    # Track counts
    role_counts = Counter()
    
    print(f"Processing {input_path}...")
    print()
    
    # Process rows
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):
            # Classify entity role
            entity_role, role_reason = classify_entity_role(row)
            role_counts[entity_role] += 1
            
            # Get score modifier
            modifier = get_role_score_modifier(entity_role)
            
            # Parse base engagement score
            base_score = parse_engagement_score(row)
            
            # Apply modifier
            modified_score = apply_role_modifier(base_score, modifier)
            
            # For excluded entities, set score to 0
            if modified_score == -1:
                modified_score = 0
            
            # Create enriched row
            enriched_row = dict(row)
            enriched_row["entity_role"] = entity_role
            enriched_row["role_score_modifier"] = str(modifier) if modifier is not None else "EXCLUDED"
            enriched_row["role_reason"] = role_reason
            
            # Update engagement_score with modified score
            enriched_row["engagement_score"] = str(modified_score)
            
            # Write to appropriate file
            output_file = get_output_file_for_role(entity_role)
            
            if output_file == "note_broker_investor_priority.csv":
                investor_writer.writerow(enriched_row)
            elif output_file == "note_broker_individual_priority.csv":
                individual_writer.writerow(enriched_row)
            elif output_file == "note_broker_legal_review.csv":
                legal_writer.writerow(enriched_row)
            elif output_file == "note_broker_excluded_entities.csv":
                excluded_writer.writerow(enriched_row)
            
            # Progress update
            if row_num % 10000 == 0:
                print(f"Processed {row_num:,} rows...")
    
    # Close files
    investor_file.close()
    individual_file.close()
    legal_file.close()
    excluded_file.close()
    
    return dict(role_counts)


def main():
    """Main entry point."""
    input_path = Path("output/note_broker_medium_priority.csv")
    output_dir = Path("output")
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    print("=" * 70)
    print("ENTITY ROLE CLASSIFICATION")
    print("=" * 70)
    print()
    print("Classifying entities by role using name heuristics:")
    print("  - INVESTOR_ENTITY: +20 score modifier")
    print("  - TRUST_ENTITY: +15 score modifier")
    print("  - INDIVIDUAL_PERSON: +10 score modifier")
    print("  - LEGAL_ENTITY: +5 score modifier")
    print("  - BANK/GOV/UTILITY: EXCLUDED")
    print()
    print(f"Input file: {input_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    role_counts = process_entity_classification(input_path, output_dir)
    
    print()
    print("=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {sum(role_counts.values()):,}")
    print()
    print("Role classification counts:")
    for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
        output_file = get_output_file_for_role(role)
        print(f"  [{role:20s}] {count:6,} -> {output_file}")
    print()
    print("Output files:")
    print(f"  - output/note_broker_investor_priority.csv (INVESTOR + TRUST)")
    print(f"  - output/note_broker_individual_priority.csv (INDIVIDUAL + UNKNOWN)")
    print(f"  - output/note_broker_legal_review.csv (LEGAL)")
    print(f"  - output/note_broker_excluded_entities.csv (BANK + GOV + UTILITY)")
    print()


if __name__ == "__main__":
    main()

