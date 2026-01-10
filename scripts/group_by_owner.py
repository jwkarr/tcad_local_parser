"""
Group Leads by Owner

Groups multiple properties owned by the same owner into a single lead record.
Consolidates property information and aggregates property counts.

Usage:
    python scripts/group_by_owner.py --input output/note_broker_high_priority.csv --outdir output
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional


def normalize_owner_key(row: Dict[str, str]) -> str:
    """
    Create a normalized key for grouping owners.
    
    Uses: full_name/company_name + mailing_zip
    """
    full_name = row.get("full_name", "").strip().upper()
    company_name = row.get("company_name", "").strip().upper()
    mailing_zip = row.get("mailing_zip", "").strip()
    
    name = full_name if full_name else company_name
    return f"{name}|{mailing_zip}"


def group_properties_by_owner(
    input_path: Path,
    output_path: Path
) -> Dict[str, int]:
    """
    Group properties by owner and create consolidated lead records.
    
    Returns:
        Dictionary with counts
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Group properties by owner
    owner_groups: Dict[str, List[Dict[str, str]]] = {}
    
    print(f"Loading and grouping properties from {input_path}...")
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        input_columns = list(reader.fieldnames or [])
        
        for row in reader:
            owner_key = normalize_owner_key(row)
            if owner_key not in owner_groups:
                owner_groups[owner_key] = []
            owner_groups[owner_key].append(row)
    
    print(f"Found {len(owner_groups):,} unique owners")
    print(f"Total properties: {sum(len(props) for props in owner_groups.values()):,}")
    print()
    
    # Create output columns
    # Keep all original columns, but aggregate some fields
    output_columns = input_columns.copy()
    
    # Add aggregated fields if not present
    if "property_count_actual" not in output_columns:
        output_columns.append("property_count_actual")
    if "property_ids" not in output_columns:
        output_columns.append("property_ids")
    if "total_portfolio_value" not in output_columns:
        output_columns.append("total_portfolio_value")
    
    counts = {
        "total_owners": len(owner_groups),
        "single_property": 0,
        "multiple_properties": 0,
        "total_properties": 0
    }
    
    print("Creating consolidated lead records...")
    print()
    
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        
        for owner_key, properties in owner_groups.items():
            counts["total_properties"] += len(properties)
            
            if len(properties) == 1:
                counts["single_property"] += 1
            else:
                counts["multiple_properties"] += 1
            
            # Use first property as base record
            base_row = dict(properties[0])
            
            # Aggregate information
            property_count = len(properties)
            base_row["property_count_actual"] = str(property_count)
            
            # Collect property IDs
            property_ids = [p.get("tcad_account_id", "").strip() for p in properties if p.get("tcad_account_id", "").strip()]
            base_row["property_ids"] = " | ".join(property_ids[:10])  # Limit to first 10
            
            # Calculate total portfolio value
            total_portfolio_value = 0
            for prop in properties:
                total_value_str = prop.get("total_value", "").strip()
                try:
                    cleaned = total_value_str.replace("$", "").replace(",", "").replace(" ", "")
                    if cleaned:
                        total_portfolio_value += float(cleaned)
                except (ValueError, AttributeError):
                    pass
            
            base_row["total_portfolio_value"] = f"{int(total_portfolio_value):,}" if total_portfolio_value > 0 else ""
            
            # Update engagement score if multiple properties (boost score)
            if property_count > 1:
                engagement_score = int(base_row.get("engagement_score", "0") or "0")
                # Boost score for multiple properties
                engagement_score = min(100, engagement_score + min(10, property_count - 1))
                base_row["engagement_score"] = str(engagement_score)
                
                # Update why_flagged
                why_flagged = base_row.get("why_flagged", "").strip()
                if why_flagged:
                    base_row["why_flagged"] = f"{why_flagged} + {property_count} properties"
                else:
                    base_row["why_flagged"] = f"{property_count} properties"
            
            writer.writerow(base_row)
            
            # Progress update
            if counts["total_owners"] % 10000 == 0:
                print(f"Processed {counts['total_owners']:,} owners...")
    
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Group multiple properties by owner into consolidated leads",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to input CSV (e.g., note_broker_high_priority.csv)"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    output_dir = Path(args.outdir)
    
    # Determine output filename
    input_stem = input_path.stem
    output_path = output_dir / f"{input_stem}_grouped.csv"
    
    print("=" * 70)
    print("GROUP LEADS BY OWNER")
    print("=" * 70)
    print()
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print()
    
    counts = group_properties_by_owner(input_path, output_path)
    
    print()
    print("=" * 70)
    print("GROUPING COMPLETE")
    print("=" * 70)
    print(f"Total unique owners: {counts['total_owners']:,}")
    print(f"Single property owners: {counts['single_property']:,}")
    print(f"Multiple property owners: {counts['multiple_properties']:,}")
    print(f"Total properties: {counts['total_properties']:,}")
    print(f"Average properties per owner: {counts['total_properties']/counts['total_owners'] if counts['total_owners'] > 0 else 0:.2f}")
    print()
    print(f"Output file: {output_path}")
    print()
    print("Consolidated leads ready for outreach!")
    print()


if __name__ == "__main__":
    main()



