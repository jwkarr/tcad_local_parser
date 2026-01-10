"""
Export Enrichment Upload

Exports leads for enrichment provider upload.
Deduplicates by lead_id and by (full_name + mailing_zip).

Usage:
    python scripts/export_enrichment_upload.py --input output/note_broker_medium_priority.csv --outdir output
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Set


def process_enrichment_upload(
    input_path: Path,
    output_path: Path
) -> Dict[str, int]:
    """
    Process input file and create enrichment upload CSV.
    
    Deduplicates by:
    1. lead_id
    2. (full_name + mailing_zip)
    
    Returns:
        Dictionary with counts
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Output columns
    output_columns = [
        "lead_id",
        "full_name",
        "mailing_address_1",
        "mailing_city",
        "mailing_state",
        "mailing_zip"
    ]
    
    # Track seen lead_ids and name+zip combinations
    seen_lead_ids: Set[str] = set()
    seen_name_zip: Set[str] = set()
    
    counts = {
        "total_rows": 0,
        "unique_lead_ids": 0,
        "unique_name_zip": 0,
        "duplicates_skipped": 0
    }
    
    print(f"Processing {input_path}...")
    print("Deduplicating by lead_id and (full_name + mailing_zip)...")
    print()
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        
        for row in reader:
            counts["total_rows"] += 1
            
            lead_id = row.get("lead_id", "").strip()
            full_name = row.get("full_name", "").strip()
            company_name = row.get("company_name", "").strip()
            mailing_zip = row.get("mailing_zip", "").strip()
            
            # Use full_name or company_name
            name = full_name if full_name else company_name
            
            # Skip if no name
            if not name:
                counts["duplicates_skipped"] += 1
                continue
            
            # Check lead_id duplicate
            if lead_id in seen_lead_ids:
                counts["duplicates_skipped"] += 1
                continue
            
            # Check name+zip duplicate
            name_zip_key = f"{name.upper()}|{mailing_zip}"
            if name_zip_key in seen_name_zip:
                counts["duplicates_skipped"] += 1
                continue
            
            # Add to seen sets
            seen_lead_ids.add(lead_id)
            seen_name_zip.add(name_zip_key)
            
            # Build output row
            output_row = {
                "lead_id": lead_id,
                "full_name": name,
                "mailing_address_1": row.get("mailing_address", "").strip(),
                "mailing_city": row.get("mailing_city", "").strip(),
                "mailing_state": row.get("mailing_state", "").strip(),
                "mailing_zip": mailing_zip
            }
            
            writer.writerow(output_row)
            counts["unique_lead_ids"] += 1
            counts["unique_name_zip"] += 1
            
            # Progress update
            if counts["total_rows"] % 10000 == 0:
                print(f"Processed {counts['total_rows']:,} rows... (unique: {counts['unique_lead_ids']:,}, "
                      f"duplicates: {counts['duplicates_skipped']:,})")
    
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export leads for enrichment provider upload",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to input CSV (e.g., note_broker_medium_priority.csv)"
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
    output_path = output_dir / "enrichment_upload.csv"
    
    print("=" * 70)
    print("EXPORT ENRICHMENT UPLOAD")
    print("=" * 70)
    print()
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print()
    
    counts = process_enrichment_upload(input_path, output_path)
    
    print()
    print("=" * 70)
    print("EXPORT COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {counts['total_rows']:,}")
    print(f"Unique lead_ids: {counts['unique_lead_ids']:,}")
    print(f"Unique (name + zip): {counts['unique_name_zip']:,}")
    print(f"Duplicates skipped: {counts['duplicates_skipped']:,}")
    print()
    print(f"Output file: {output_path}")
    print()
    print("Next step: Send enrichment_upload.csv to your enrichment provider")
    print()


if __name__ == "__main__":
    main()



