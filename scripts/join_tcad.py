"""
Join TCAD Data with Private Note Leads

Enriches private note leads with TCAD property data when a parcel/account match exists.
"""

import csv
from pathlib import Path
from typing import Dict, Optional


def load_tcad_lookup(tcad_path: Path) -> Dict[str, Dict[str, str]]:
    """
    Load TCAD data into a lookup dictionary keyed by account_id.
    
    Args:
        tcad_path: Path to TCAD prop_clean.csv
        
    Returns:
        Dictionary mapping account_id -> TCAD row data
    """
    tcad_lookup = {}
    
    if not tcad_path.exists():
        return tcad_lookup
    
    print(f"Loading TCAD data from {tcad_path}...")
    
    with open(tcad_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            account_id = row.get("account_id", "").strip()
            if account_id:
                tcad_lookup[account_id] = row
    
    print(f"Loaded {len(tcad_lookup):,} TCAD records")
    return tcad_lookup


def enrich_leads_with_tcad(
    leads_path: Path,
    tcad_lookup: Dict[str, Dict[str, str]],
    column_mapping: Dict[str, str],
    output_path: Path
):
    """
    Enrich leads with TCAD data when parcel/account match exists.
    
    Args:
        leads_path: Path to private_note_leads.csv
        tcad_lookup: TCAD lookup dictionary
        column_mapping: Column mapping from recorder to canonical fields
        output_path: Path for enriched output
    """
    # TCAD enrichment fields to add
    tcad_enrichment_fields = [
        "tcad_owner_name",
        "tcad_situs_address",
        "tcad_situs_city",
        "tcad_situs_state",
        "tcad_situs_zip",
        "tcad_mailing_address",
        "tcad_mailing_city",
        "tcad_mailing_state",
        "tcad_mailing_zip",
        "tcad_property_type",
        "tcad_land_value",
        "tcad_improvement_value",
        "tcad_total_value"
    ]
    
    # Read leads file
    with open(leads_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        original_columns = list(reader.fieldnames or [])
        
        # Add TCAD enrichment columns
        output_columns = original_columns + tcad_enrichment_fields
        
        # Write enriched leads
        with open(output_path, 'w', newline='', encoding='utf-8') as out_f:
            writer = csv.DictWriter(out_f, fieldnames=output_columns)
            writer.writeheader()
            
            matches = 0
            total = 0
            
            for row in reader:
                total += 1
                enriched_row = dict(row)
                
                # Try to find matching TCAD record
                # Look for APN/account_id in recorder data
                apn_field = column_mapping.get("apn", "")
                account_id = ""
                
                if apn_field and apn_field in row:
                    account_id = row[apn_field].strip()
                elif "APN" in row:
                    account_id = row["APN"].strip()
                elif "apn" in row:
                    account_id = row["apn"].strip()
                elif "account_id" in row:
                    account_id = row["account_id"].strip()
                
                # Match with TCAD data
                if account_id and account_id in tcad_lookup:
                    tcad_row = tcad_lookup[account_id]
                    enriched_row["tcad_owner_name"] = tcad_row.get("owner_name", "")
                    enriched_row["tcad_situs_address"] = tcad_row.get("situs_address", "")
                    enriched_row["tcad_situs_city"] = tcad_row.get("situs_city", "")
                    enriched_row["tcad_situs_state"] = tcad_row.get("situs_state", "")
                    enriched_row["tcad_situs_zip"] = tcad_row.get("situs_zip", "")
                    enriched_row["tcad_mailing_address"] = tcad_row.get("mailing_address", "")
                    enriched_row["tcad_mailing_city"] = tcad_row.get("mailing_city", "")
                    enriched_row["tcad_mailing_state"] = tcad_row.get("mailing_state", "")
                    enriched_row["tcad_mailing_zip"] = tcad_row.get("mailing_zip", "")
                    enriched_row["tcad_property_type"] = tcad_row.get("property_type", "")
                    enriched_row["tcad_land_value"] = tcad_row.get("land_value", "")
                    enriched_row["tcad_improvement_value"] = tcad_row.get("improvement_value", "")
                    enriched_row["tcad_total_value"] = tcad_row.get("total_value", "")
                    matches += 1
                else:
                    # Fill with empty strings if no match
                    for field in tcad_enrichment_fields:
                        enriched_row[field] = ""
                
                writer.writerow(enriched_row)
            
            print(f"Enriched {matches:,} of {total:,} leads with TCAD data ({matches/total*100 if total > 0 else 0:.1f}% match rate)")



