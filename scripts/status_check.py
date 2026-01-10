"""
Status Check for Note Leads Pipeline

Validates output files and prints statistics.
"""

import csv
import sys
from pathlib import Path
from collections import Counter
from typing import Dict, List, Optional, Tuple


# Required columns for email-ready CSV
REQUIRED_EMAIL_COLUMNS = [
    "lead_id", "first_name", "last_name", "full_name", "email", "company_name",
    "owner_type", "mailing_address_1", "mailing_city", "mailing_state", "mailing_zip",
    "property_address_1", "property_city", "property_state", "property_zip",
    "county", "doc_type", "recording_date", "original_loan_amount",
    "interest_rate", "loan_term_months", "lien_position", "tcad_account_id",
    "source_file", "lead_score", "why_flagged"
]


def check_file_columns(file_path: Path, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Check if file has all required columns.
    
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    if not file_path.exists():
        return (False, ["FILE_NOT_FOUND"])
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        actual_columns = set(reader.fieldnames or [])
        required_set = set(required_columns)
        
        missing = required_set - actual_columns
        return (len(missing) == 0, list(missing))


def analyze_email_ready(file_path: Path) -> Dict:
    """Analyze email-ready CSV and return statistics."""
    stats = {
        "total_rows": 0,
        "owner_type_counts": Counter(),
        "doc_type_counts": Counter(),
        "lead_score_stats": {"min": 100, "max": 0, "avg": 0},
        "missing_email_count": 0,
        "has_tcad_match_count": 0,
        "sample_lead_ids": []
    }
    
    if not file_path.exists():
        return stats
    
    lead_scores = []
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            stats["total_rows"] += 1
            
            # Owner type
            owner_type = row.get("owner_type", "UNKNOWN")
            stats["owner_type_counts"][owner_type] += 1
            
            # Doc type
            doc_type = row.get("doc_type", "")
            if doc_type:
                stats["doc_type_counts"][doc_type] += 1
            
            # Lead score
            lead_score_str = row.get("lead_score", "")
            if lead_score_str:
                try:
                    score = int(lead_score_str)
                    lead_scores.append(score)
                    stats["lead_score_stats"]["min"] = min(stats["lead_score_stats"]["min"], score)
                    stats["lead_score_stats"]["max"] = max(stats["lead_score_stats"]["max"], score)
                except ValueError:
                    pass
            
            # Email
            email = row.get("email", "").strip()
            if not email:
                stats["missing_email_count"] += 1
            
            # TCAD match
            tcad_id = row.get("tcad_account_id", "").strip()
            if tcad_id:
                stats["has_tcad_match_count"] += 1
            
            # Sample lead IDs
            if len(stats["sample_lead_ids"]) < 5:
                lead_id = row.get("lead_id", "")
                if lead_id:
                    stats["sample_lead_ids"].append(lead_id)
    
    # Calculate average lead score
    if lead_scores:
        stats["lead_score_stats"]["avg"] = sum(lead_scores) / len(lead_scores)
    else:
        stats["lead_score_stats"] = {"min": 0, "max": 0, "avg": 0}
    
    return stats


def print_status(output_dir: Path):
    """Print status check results."""
    print("=" * 70)
    print("NOTE LEADS PIPELINE - STATUS CHECK")
    print("=" * 70)
    print()
    
    # Check email-ready file
    email_path = output_dir / "note_leads_email_ready.csv"
    print(f"Checking: {email_path.name}")
    print("-" * 70)
    
    is_valid, missing = check_file_columns(email_path, REQUIRED_EMAIL_COLUMNS)
    if is_valid:
        print("[OK] All required columns present")
    else:
        print(f"[ERROR] Missing columns: {', '.join(missing)}")
        return
    
    # Analyze email-ready file
    stats = analyze_email_ready(email_path)
    
    print(f"\nTotal leads: {stats['total_rows']:,}")
    print()
    
    # Owner type breakdown
    print("Owner Type Distribution:")
    for owner_type, count in stats["owner_type_counts"].most_common():
        pct = (count / stats["total_rows"] * 100) if stats["total_rows"] > 0 else 0
        print(f"  {owner_type:10s}: {count:6,} ({pct:5.1f}%)")
    print()
    
    # Doc type breakdown (top 10)
    print("Document Type Distribution (top 10):")
    for doc_type, count in stats["doc_type_counts"].most_common(10):
        pct = (count / stats["total_rows"] * 100) if stats["total_rows"] > 0 else 0
        print(f"  {doc_type[:40]:40s}: {count:6,} ({pct:5.1f}%)")
    print()
    
    # Lead score stats
    print("Lead Score Statistics:")
    print(f"  Min:  {stats['lead_score_stats']['min']}")
    print(f"  Max:  {stats['lead_score_stats']['max']}")
    print(f"  Avg:  {stats['lead_score_stats']['avg']:.1f}")
    print()
    
    # Email status
    print("Email Status:")
    has_email = stats["total_rows"] - stats["missing_email_count"]
    print(f"  With email:    {has_email:6,} ({(has_email/stats['total_rows']*100) if stats['total_rows'] > 0 else 0:.1f}%)")
    print(f"  Missing email: {stats['missing_email_count']:6,} ({(stats['missing_email_count']/stats['total_rows']*100) if stats['total_rows'] > 0 else 0:.1f}%)")
    print()
    
    # TCAD match status
    print("TCAD Match Status:")
    print(f"  With TCAD match: {stats['has_tcad_match_count']:6,} ({(stats['has_tcad_match_count']/stats['total_rows']*100) if stats['total_rows'] > 0 else 0:.1f}%)")
    print()
    
    # Sample lead IDs
    if stats["sample_lead_ids"]:
        print("Sample Lead IDs (for enrichment matching):")
        for lead_id in stats["sample_lead_ids"]:
            print(f"  {lead_id}")
        print()
    
    # Check other files
    mail_path = output_dir / "note_leads_mail_ready.csv"
    review_path = output_dir / "review_queue.csv"
    discard_path = output_dir / "discarded.csv"
    
    print("Other Output Files:")
    print(f"  Mail-ready:  {'[OK]' if mail_path.exists() else '[MISSING]'} {mail_path.name}")
    print(f"  Review:      {'[OK]' if review_path.exists() else '[MISSING]'} {review_path.name}")
    print(f"  Discarded:   {'[OK]' if discard_path.exists() else '[MISSING]'} {discard_path.name}")
    print()
    
    print("=" * 70)
    print("Status check complete!")
    print("=" * 70)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Check status of note leads pipeline output"
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
        help="Output directory to check (default: output)"
    )
    
    args = parser.parse_args()
    output_dir = Path(args.outdir)
    
    if not output_dir.exists():
        print(f"Error: Output directory not found: {output_dir}", file=sys.stderr)
        sys.exit(1)
    
    print_status(output_dir)


if __name__ == "__main__":
    main()

