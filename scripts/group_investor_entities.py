"""
Group Investor Entities by Name

Consolidates investor/trust entities that appear multiple times by grouping
by company_name or full_name. Aggregates property counts, portfolio values,
and other metrics.

Usage:
    python scripts/group_investor_entities.py
"""

import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional
from collections import Counter


def normalize_entity_name(row: Dict[str, str]) -> str:
    """
    Create a normalized key for grouping entities.
    
    Uses: company_name (preferred) or full_name, normalized to uppercase.
    """
    company_name = row.get("company_name", "").strip().upper()
    full_name = row.get("full_name", "").strip().upper()
    
    # Prefer company_name, fallback to full_name
    entity_name = company_name if company_name else full_name
    
    # Normalize: remove extra spaces, standardize
    entity_name = " ".join(entity_name.split())
    
    return entity_name


def parse_amount(amount_str: str) -> Optional[float]:
    """Parse amount from string."""
    if not amount_str or not amount_str.strip():
        return None
    
    cleaned = amount_str.strip().replace("$", "").replace(",", "").replace(" ", "")
    
    try:
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def group_entities_by_name(
    input_path: Path,
    output_path: Path
) -> Dict[str, int]:
    """
    Group entities by name and create consolidated records.
    
    Returns:
        Dictionary with counts
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Group properties by entity name
    entity_groups: Dict[str, List[Dict[str, str]]] = {}
    
    print(f"Loading and grouping entities from {input_path}...")
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        input_columns = list(reader.fieldnames or [])
        
        for row in reader:
            entity_key = normalize_entity_name(row)
            if not entity_key:
                # Skip rows with no name
                continue
                
            if entity_key not in entity_groups:
                entity_groups[entity_key] = []
            entity_groups[entity_key].append(row)
    
    print(f"Found {len(entity_groups):,} unique entities")
    print(f"Total property records: {sum(len(props) for props in entity_groups.values()):,}")
    print()
    
    # Create output columns
    output_columns = input_columns.copy()
    
    # Add aggregated fields
    aggregated_fields = [
        "properties_count",
        "total_portfolio_value",
        "avg_property_value",
        "property_ids",
        "property_addresses",
        "max_engagement_score",
        "has_any_email",
        "has_any_phone"
    ]
    
    for field in aggregated_fields:
        if field not in output_columns:
            output_columns.append(field)
    
    counts = {
        "total_entities": len(entity_groups),
        "single_property": 0,
        "multiple_properties": 0,
        "total_properties": 0
    }
    
    print("Creating consolidated entity records...")
    print()
    
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        
        for entity_key, properties in entity_groups.items():
            property_count = len(properties)
            counts["total_properties"] += property_count
            
            if property_count == 1:
                counts["single_property"] += 1
            else:
                counts["multiple_properties"] += 1
            
            # Use first property as base record
            base_row = dict(properties[0])
            
            # Aggregate information
            base_row["properties_count"] = str(property_count)
            
            # Collect property IDs and addresses
            property_ids = []
            property_addresses = []
            total_portfolio_value = 0
            max_engagement_score = 0
            has_any_email = False
            has_any_phone = False
            
            for prop in properties:
                # Property IDs
                tcad_id = prop.get("tcad_account_id", "").strip()
                if tcad_id:
                    property_ids.append(tcad_id)
                
                # Property addresses
                situs_addr = prop.get("situs_address", "").strip()
                situs_city = prop.get("situs_city", "").strip()
                if situs_addr:
                    addr_str = f"{situs_addr}, {situs_city}" if situs_city else situs_addr
                    property_addresses.append(addr_str)
                
                # Portfolio value
                total_value = parse_amount(prop.get("total_value", ""))
                if total_value:
                    total_portfolio_value += total_value
                
                # Engagement score
                engagement_score = int(prop.get("engagement_score", "0") or "0")
                max_engagement_score = max(max_engagement_score, engagement_score)
                
                # Contact info
                if prop.get("has_email", "").upper() == "Y":
                    has_any_email = True
                if prop.get("has_phone", "").upper() == "Y":
                    has_any_phone = True
            
            # Set aggregated fields
            base_row["total_portfolio_value"] = f"{int(total_portfolio_value):,}" if total_portfolio_value > 0 else ""
            
            avg_value = total_portfolio_value / property_count if property_count > 0 else 0
            base_row["avg_property_value"] = f"{int(avg_value):,}" if avg_value > 0 else ""
            
            base_row["property_ids"] = " | ".join(property_ids[:20])  # Limit to first 20
            if len(property_ids) > 20:
                base_row["property_ids"] += f" ... (+{len(property_ids) - 20} more)"
            
            base_row["property_addresses"] = " | ".join(property_addresses[:5])  # Limit to first 5
            if len(property_addresses) > 5:
                base_row["property_addresses"] += f" ... (+{len(property_addresses) - 5} more)"
            
            base_row["max_engagement_score"] = str(max_engagement_score)
            base_row["has_any_email"] = "Y" if has_any_email else "N"
            base_row["has_any_phone"] = "Y" if has_any_phone else "N"
            
            # Update engagement_score to max (best property)
            base_row["engagement_score"] = str(max_engagement_score)
            
            # Update property_count field if it exists
            if "property_count" in base_row:
                base_row["property_count"] = str(property_count)
            
            # Update why_flagged to reflect consolidation
            why_flagged = base_row.get("why_flagged", "").strip()
            if property_count > 1:
                if why_flagged:
                    base_row["why_flagged"] = f"{why_flagged} | {property_count} properties consolidated"
                else:
                    base_row["why_flagged"] = f"{property_count} properties consolidated"
            
            writer.writerow(base_row)
            
            # Progress update
            processed = counts["single_property"] + counts["multiple_properties"]
            if len(entity_groups) > 1000 and processed % 1000 == 0:
                print(f"Processed {processed:,} entities...")
    
    return counts


def main():
    """Main entry point."""
    input_path = Path("output/note_broker_investor_priority.csv")
    output_path = Path("output/note_broker_investor_priority_grouped.csv")
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    print("=" * 70)
    print("GROUP INVESTOR ENTITIES BY NAME")
    print("=" * 70)
    print()
    print("Consolidating entities with multiple properties:")
    print("  - Groups by company_name or full_name")
    print("  - Aggregates property counts and portfolio values")
    print("  - Lists property IDs and addresses")
    print("  - Uses maximum engagement score")
    print()
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print()
    
    counts = group_entities_by_name(input_path, output_path)
    
    print()
    print("=" * 70)
    print("GROUPING COMPLETE")
    print("=" * 70)
    print(f"Total unique entities: {counts['total_entities']:,}")
    print(f"Single property entities: {counts['single_property']:,}")
    print(f"Multiple property entities: {counts['multiple_properties']:,}")
    print(f"Total property records: {counts['total_properties']:,}")
    if counts['total_entities'] > 0:
        avg_props = counts['total_properties'] / counts['total_entities']
        print(f"Average properties per entity: {avg_props:.2f}")
    print()
    print(f"Output file: {output_path}")
    print()
    print("Consolidated investor entities ready for outreach!")
    print()


if __name__ == "__main__":
    main()

