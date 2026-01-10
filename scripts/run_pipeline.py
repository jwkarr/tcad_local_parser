"""
Private Note Leads Pipeline

Main entry point for processing recorder data and identifying private-held mortgage notes.

Usage:
    python scripts/run_pipeline.py --recorder input/recorder_sample.csv --tcad output/prop_clean.csv --outdir output
"""

import argparse
import sys
from pathlib import Path

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from column_mapper import map_columns, save_mapping
from filter_private_notes import process_recorder_file
from join_tcad import load_tcad_lookup, enrich_leads_with_tcad


def main():
    """Main pipeline entry point."""
    parser = argparse.ArgumentParser(
        description="Filter recorder data to identify private-held mortgage notes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python scripts/run_pipeline.py --recorder input/recorder_sample.csv --outdir output
  
  # With TCAD enrichment
  python scripts/run_pipeline.py --recorder input/recorder_sample.csv --tcad output/prop_clean.csv --outdir output
        """
    )
    
    parser.add_argument(
        "--recorder",
        type=str,
        required=True,
        help="Path to recorder CSV file"
    )
    
    parser.add_argument(
        "--tcad",
        type=str,
        default=None,
        help="Optional path to TCAD prop_clean.csv for enrichment"
    )
    
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory for results (default: output)"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    recorder_path = Path(args.recorder)
    if not recorder_path.exists():
        print(f"Error: Recorder file not found: {recorder_path}", file=sys.stderr)
        sys.exit(1)
    
    tcad_path = None
    if args.tcad:
        tcad_path = Path(args.tcad)
        if not tcad_path.exists():
            print(f"Warning: TCAD file not found: {tcad_path}", file=sys.stderr)
            print("Continuing without TCAD enrichment...", file=sys.stderr)
            tcad_path = None
    
    output_dir = Path(args.outdir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("PRIVATE NOTE LEADS PIPELINE")
    print("=" * 70)
    print(f"Recorder file: {recorder_path}")
    if tcad_path:
        print(f"TCAD file: {tcad_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Step 1: Map columns
    print("STEP 1: Mapping columns...")
    print("-" * 70)
    column_mapping, field_status = map_columns(recorder_path)
    
    # Save mapping
    mapping_path = output_dir / "column_mapping.json"
    save_mapping(column_mapping, mapping_path)
    print()
    
    # Step 2: Process and filter
    print("STEP 2: Processing and filtering rows...")
    print("-" * 70)
    counts, lender_type_counter = process_recorder_file(
        recorder_path=recorder_path,
        column_mapping=column_mapping,
        output_dir=output_dir
    )
    print()
    
    # Step 3: Enrich with TCAD (if provided)
    leads_path = output_dir / "private_note_leads.csv"
    enriched_path = output_dir / "private_note_leads_enriched.csv"
    
    if tcad_path and tcad_path.exists() and leads_path.exists():
        print("STEP 3: Enriching leads with TCAD data...")
        print("-" * 70)
        tcad_lookup = load_tcad_lookup(tcad_path)
        if tcad_lookup:
            enrich_leads_with_tcad(
                leads_path=leads_path,
                tcad_lookup=tcad_lookup,
                column_mapping=column_mapping,
                output_path=enriched_path
            )
            print()
        else:
            print("No TCAD data loaded, skipping enrichment.")
            print()
    else:
        print("STEP 3: Skipping TCAD enrichment (no TCAD file provided or no leads found)")
        print()
    
    # Step 4: Summary
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Total rows processed: {sum(counts.values()):,}")
    print(f"  [OK] Leads:        {counts['leads']:,} -> output/private_note_leads.csv")
    if tcad_path and enriched_path.exists():
        print(f"       Enriched:     -> output/private_note_leads_enriched.csv")
    print(f"  [?]  Review queue: {counts['review']:,} -> output/review_queue.csv")
    print(f"  [X]  Discarded:    {counts['discarded']:,} -> output/discarded.csv")
    print()
    
    # Lender type breakdown
    print("Lender Type Distribution:")
    total_classified = sum(lender_type_counter.values())
    for lender_type in ["BANK", "PERSON", "LLC", "TRUST", "UNKNOWN"]:
        count = lender_type_counter.get(lender_type, 0)
        pct = (count / total_classified * 100) if total_classified > 0 else 0
        print(f"  {lender_type:10s}: {count:8,} ({pct:5.1f}%)")
    print()
    
    print(f"Column mapping saved to: {mapping_path}")
    print()
    
    # Calculate percentages
    total = sum(counts.values())
    if total > 0:
        lead_pct = (counts['leads'] / total) * 100
        review_pct = (counts['review'] / total) * 100
        discard_pct = (counts['discarded'] / total) * 100
        print(f"Classification Distribution:")
        print(f"  Leads:    {lead_pct:.1f}%")
        print(f"  Review:   {review_pct:.1f}%")
        print(f"  Discarded: {discard_pct:.1f}%")


if __name__ == "__main__":
    main()
