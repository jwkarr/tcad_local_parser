"""
Export Instantly Upload

Exports high priority leads for Instantly.ai email campaign upload.
Only includes rows with valid email addresses.

Usage:
    python scripts/export_instantly_upload.py --input output/note_broker_high_priority.csv --outdir output
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Optional


def parse_name(full_name: str) -> tuple:
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


def get_value_band(total_value_str: str) -> str:
    """Get value band for property."""
    try:
        cleaned = total_value_str.strip().replace("$", "").replace(",", "").replace(" ", "")
        total_value = float(cleaned)
        
        if total_value < 100000:
            return "<100k"
        elif total_value < 200000:
            return "100k-200k"
        elif total_value < 300000:
            return "200k-300k"
        elif total_value < 400000:
            return "300k-400k"
        elif total_value < 500000:
            return "400k-500k"
        else:
            return "500k+"
    except (ValueError, AttributeError):
        return "unknown"


def process_instantly_upload(
    input_path: Path,
    output_path: Path
) -> Dict[str, int]:
    """
    Process input file and create Instantly upload CSV.
    
    Only includes rows with valid email addresses.
    
    Returns:
        Dictionary with counts
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Output columns for Instantly
    output_columns = [
        "email",
        "first_name",
        "last_name",
        "city",
        "state",
        "zip",
        "engagement_score",
        "value_band",
        "why_flagged"
    ]
    
    counts = {
        "total_rows": 0,
        "with_email": 0,
        "without_email": 0
    }
    
    print(f"Processing {input_path}...")
    print("Filtering for rows with valid email addresses...")
    print()
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=output_columns)
        writer.writeheader()
        
        for row in reader:
            counts["total_rows"] += 1
            
            # Check for email (case insensitive)
            email = row.get("email", "") or row.get("Email", "")
            email = email.strip()
            
            if not email or "@" not in email:
                counts["without_email"] += 1
                continue
            
            counts["with_email"] += 1
            
            # Parse name
            full_name = row.get("full_name", "").strip()
            company_name = row.get("company_name", "").strip()
            name = full_name if full_name else company_name
            
            first_name, last_name = parse_name(name)
            
            # Get location (prefer mailing, fallback to situs)
            city = row.get("mailing_city", "").strip() or row.get("situs_city", "").strip()
            state = row.get("mailing_state", "").strip() or row.get("situs_state", "").strip()
            zip_code = row.get("mailing_zip", "").strip() or row.get("situs_zip", "").strip()
            
            # Get engagement score
            engagement_score = row.get("engagement_score", "").strip() or "0"
            
            # Get value band
            total_value = row.get("total_value", "").strip()
            value_band = get_value_band(total_value)
            
            # Get why_flagged
            why_flagged = row.get("why_flagged", "").strip() or row.get("engagement_reason", "").strip()
            
            # Build output row
            output_row = {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "city": city,
                "state": state,
                "zip": zip_code,
                "engagement_score": engagement_score,
                "value_band": value_band,
                "why_flagged": why_flagged
            }
            
            writer.writerow(output_row)
            
            # Progress update
            if counts["total_rows"] % 10000 == 0:
                print(f"Processed {counts['total_rows']:,} rows... (with email: {counts['with_email']:,}, "
                      f"without: {counts['without_email']:,})")
    
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export high priority leads for Instantly.ai upload",
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
    output_path = output_dir / "instantly_upload.csv"
    
    print("=" * 70)
    print("EXPORT INSTANTLY UPLOAD")
    print("=" * 70)
    print()
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print()
    
    counts = process_instantly_upload(input_path, output_path)
    
    print()
    print("=" * 70)
    print("EXPORT COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {counts['total_rows']:,}")
    print(f"With valid email: {counts['with_email']:,}")
    print(f"Without email (skipped): {counts['without_email']:,}")
    print()
    print(f"Output file: {output_path}")
    print()
    print("Ready to upload to Instantly.ai!")
    print()


if __name__ == "__main__":
    main()



