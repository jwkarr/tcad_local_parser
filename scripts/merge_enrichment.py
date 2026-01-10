"""
Merge Enrichment Results

Merges enrichment results back into the original leads file.

Usage:
    python scripts/merge_enrichment.py --leads output/note_broker_medium_priority.csv --enrichment input/enrichment_results.csv --outdir output
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Optional


def load_enrichment_results(enrichment_path: Path) -> Dict[str, Dict[str, str]]:
    """
    Load enrichment results into a dictionary keyed by lead_id.
    
    Returns:
        Dictionary mapping lead_id -> enrichment data
    """
    enrichment_lookup = {}
    
    if not enrichment_path.exists():
        print(f"Warning: Enrichment file not found: {enrichment_path}")
        return enrichment_lookup
    
    print(f"Loading enrichment results from {enrichment_path}...")
    
    with open(enrichment_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            lead_id = row.get("lead_id", "").strip()
            if lead_id:
                enrichment_lookup[lead_id] = row
    
    print(f"Loaded {len(enrichment_lookup):,} enrichment records")
    return enrichment_lookup


def merge_enrichment(
    leads_path: Path,
    enrichment_path: Path,
    output_path: Path
) -> Dict[str, int]:
    """
    Merge enrichment results into leads file.
    
    Returns:
        Dictionary with counts
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load enrichment results
    enrichment_lookup = load_enrichment_results(enrichment_path)
    
    # Read leads file to get columns
    with open(leads_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        lead_columns = list(reader.fieldnames or [])
    
    # Add enrichment columns if not present
    enrichment_columns = ["email", "phone"]
    output_columns = lead_columns.copy()
    
    for col in enrichment_columns:
        if col not in output_columns:
            output_columns.append(col)
    
    counts = {
        "total_rows": 0,
        "matched": 0,
        "unmatched": 0,
        "with_email": 0,
        "with_phone": 0
    }
    
    print()
    print(f"Merging enrichment into {leads_path}...")
    print()
    
    with open(leads_path, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        
        for row in reader:
            counts["total_rows"] += 1
            
            lead_id = row.get("lead_id", "").strip()
            
            # Create output row
            output_row = dict(row)
            
            # Merge enrichment data if available
            if lead_id in enrichment_lookup:
                enrichment_row = enrichment_lookup[lead_id]
                counts["matched"] += 1
                
                # Add email if present
                email = enrichment_row.get("email", "").strip()
                if email:
                    output_row["email"] = email
                    counts["with_email"] += 1
                elif "email" not in output_row:
                    output_row["email"] = ""
                
                # Add phone if present
                phone = enrichment_row.get("phone", "").strip()
                if phone:
                    output_row["phone"] = phone
                    counts["with_phone"] += 1
                elif "phone" not in output_row:
                    output_row["phone"] = ""
            else:
                counts["unmatched"] += 1
                # Add empty enrichment columns if not present
                if "email" not in output_row:
                    output_row["email"] = ""
                if "phone" not in output_row:
                    output_row["phone"] = ""
            
            writer.writerow(output_row)
            
            # Progress update
            if counts["total_rows"] % 10000 == 0:
                print(f"Processed {counts['total_rows']:,} rows... (matched: {counts['matched']:,}, "
                      f"with email: {counts['with_email']:,}, with phone: {counts['with_phone']:,})")
    
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Merge enrichment results into leads file",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--leads",
        type=str,
        required=True,
        help="Path to leads CSV (e.g., note_broker_medium_priority.csv)"
    )
    
    parser.add_argument(
        "--enrichment",
        type=str,
        required=True,
        help="Path to enrichment results CSV (e.g., input/enrichment_results.csv)"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    
    args = parser.parse_args()
    
    leads_path = Path(args.leads)
    if not leads_path.exists():
        print(f"Error: Leads file not found: {leads_path}", file=sys.stderr)
        sys.exit(1)
    
    enrichment_path = Path(args.enrichment)
    output_dir = Path(args.outdir)
    
    # Determine output filename
    leads_stem = leads_path.stem
    output_path = output_dir / f"{leads_stem}_enriched.csv"
    
    print("=" * 70)
    print("MERGE ENRICHMENT RESULTS")
    print("=" * 70)
    print()
    print(f"Leads file: {leads_path}")
    print(f"Enrichment file: {enrichment_path}")
    print(f"Output file: {output_path}")
    print()
    
    counts = merge_enrichment(leads_path, enrichment_path, output_path)
    
    print()
    print("=" * 70)
    print("MERGE COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {counts['total_rows']:,}")
    print(f"Matched with enrichment: {counts['matched']:,} ({counts['matched']/counts['total_rows']*100 if counts['total_rows'] > 0 else 0:.1f}%)")
    print(f"Unmatched: {counts['unmatched']:,}")
    print(f"With email: {counts['with_email']:,}")
    print(f"With phone: {counts['with_phone']:,}")
    print()
    print(f"Output file: {output_path}")
    print()
    print("Next step: Re-run note_broker_refine.py with --enriched flag")
    print()


if __name__ == "__main__":
    main()



