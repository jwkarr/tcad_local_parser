# Note Leads Pipeline

Pipeline for filtering county recorder data to identify seller-finance note leads.

**Note:** There are two separate pipelines:
1. **Recorder-based pipeline** - Identifies verified private note holders from recorder data
2. **TCAD-based pipeline** - Generates property owner targets (NOT verified note holders)

## Overview

This pipeline processes recorder export CSVs and classifies each row into one of three categories:

1. **note_leads_email_ready.csv** - Best candidates, formatted for email campaigns (Instantly/Smartlead)
2. **note_leads_mail_ready.csv** - Same leads, formatted for mail campaigns
3. **review_queue.csv** - Rows that need manual review due to ambiguity
4. **discarded.csv** - Rows filtered out (with reason provided)

## Quick Start

```bash
# Basic usage (without TCAD enrichment)
python scripts/run_pipeline.py --recorder input/recorder_sample.csv --outdir output

# With TCAD property data enrichment
python scripts/run_pipeline.py --recorder input/recorder_sample.csv --tcad output/prop_clean.csv --outdir output
```

## Input Files

### Recorder Export

Place your full county recorder export CSV in the `input/` directory:

```
input/
  └── recorder_export.csv  # Your full recorder data
```

The pipeline automatically maps columns using fuzzy matching, so column names don't need to match exactly. It looks for fields like:
- Recording date
- Document type
- Lender/beneficiary name
- Borrower/grantor name
- Property address
- Loan amount
- APN/parcel ID (for TCAD join)

### TCAD Data (Optional)

If you have TCAD property data from `main.py`, you can enrich leads with owner information:

```
output/
  └── prop_clean.csv  # From main.py TCAD parser
```

## Output Files

All outputs are written to the `output/` directory:

### note_leads_email_ready.csv

Best candidates formatted for email campaigns. These rows meet all criteria:
- Loan amount between $1 and $500,000 (configurable)
- Lender appears to be a person or small entity (not a bank)
- Document type suggests seller financing
- Recording date between 3-20 years ago (configurable)
- All required fields present

**Key columns:**
- `lead_id` - Deterministic SHA1 hash for matching enrichment data
- `first_name`, `last_name`, `full_name` - Parsed lender name
- `email` - Blank initially, to be filled by enrichment
- `owner_type` - PERSON, LLC, TRUST, BANK, or UNKNOWN
- `mailing_address_1`, `mailing_city`, `mailing_state`, `mailing_zip` - Mailing address
- `property_address_1`, `property_city`, `property_state`, `property_zip` - Property address
- `original_loan_amount` - Loan amount
- `lead_score` - Quality score (0-100)
- `why_flagged` - Summary of why this is a lead
- `tcad_account_id` - Link back to TCAD data

### note_leads_mail_ready.csv

Same leads as email-ready, with additional fields for mail campaigns:
- `mailing_address_2` - Additional address line
- `owner_mailing_name_line` - Formatted name for labels
- `property_owner_occupied_guess` - Y/N/UNKNOWN
- `equity_estimate` - Optional equity estimate

### review_queue.csv

Rows that need manual review. Common reasons:
- Missing loan amount
- Corporate lender (LLC/Inc) without clear seller finance doc type
- Recording too recent (< 3 years)
- Missing required fields
- Ambiguous classification

### discarded.csv

Rows filtered out with reason provided. Common reasons:
- Lender is a bank or large financial institution
- Document type is release/satisfaction only
- Recording too old (> 20 years)
- Invalid or missing critical data

### column_mapping.json

The column mapping chosen by the auto-mapper. Useful for:
- Verifying the mapping is correct
- Manual adjustments if needed
- Documentation of which columns were used

## Configuration

Edit `scripts/filter_notes.py` to adjust filtering thresholds:

```python
# Loan amount limits
MAX_LOAN_AMOUNT = 500000  # Maximum loan amount for leads

# Recording age limits (years)
MIN_AGE_YEARS = 3   # Minimum age (too recent = review)
MAX_AGE_YEARS = 20  # Maximum age (too old = discard)

# Bank keywords (add more if needed)
BANK_KEYWORDS = [
    "WELLS FARGO", "CHASE", "BANK OF AMERICA",
    # ... see filter_notes.py for full list
]
```

## How It Works

### 1. Column Mapping

The `column_mapper.py` module:
- Reads the CSV header
- Uses fuzzy matching to map columns to canonical field names
- Handles common variations (case, spacing, underscores)
- Prints the mapping for verification
- Saves mapping to JSON

### 2. Classification & Enrichment

The `filter_notes.py` module:
- Classifies each row as LEAD, REVIEW, or DISCARD
- Generates deterministic `lead_id` using SHA1 hash
- Detects `owner_type` (PERSON/LLC/TRUST/BANK/UNKNOWN)
- Calculates `lead_score` (0-100) based on multiple factors
- Creates `why_flagged` summary
- Enriches with TCAD data when available

**LEAD** if:
- Loan amount exists and is in valid range ($1 - $500k)
- Lender looks like person or small entity (not bank)
- Document type suggests seller financing
- Recording age between 3-20 years
- All required fields present

**DISCARD** if:
- Lender is a bank
- Document type is release/satisfaction only
- Recording too old (> 20 years)
- Missing all critical data

**REVIEW** if:
- Missing loan amount
- Corporate lender without clear seller finance doc
- Recording too recent (< 3 years)
- Any ambiguity

### 3. TCAD Enrichment (Optional)

If TCAD data is provided:
- Joins on APN/parcel_id/account_id
- Adds owner name and property address from TCAD
- If join fails, row is still output (just without enrichment)

## Performance

The pipeline is designed for large files:
- **Streaming processing** - doesn't load entire file into memory
- **Incremental writes** - outputs written as rows are processed
- **Progress updates** - shows progress every 10,000 rows

## Troubleshooting

### "Missing required fields" warning

If you see warnings about missing required fields:
- Check that your CSV has columns for `lender_name` and `loan_amount`
- The mapper uses fuzzy matching, so similar names should work
- If mapping fails, you can manually edit `column_mapping.json`

### Too many rows in review_queue

If most rows go to review:
- Check that loan amounts are in the expected format (numbers, not text)
- Verify recording dates are parseable
- Adjust thresholds in `filter_notes.py` if needed

### No leads found

If no rows are classified as leads:
- Check that loan amounts are within range ($1 - $500k)
- Verify lenders aren't all banks
- Check that document types suggest seller financing
- Review a few rows in `review_queue.csv` to see why they didn't qualify

## Example Workflow

1. **Export recorder data** from county system to CSV
2. **Place in input/** directory
3. **Run pipeline**:
   ```bash
   python scripts/run_pipeline.py --recorder input/recorder_export.csv --tcad output/prop_clean.csv --outdir output
   ```
4. **Validate output**:
   ```bash
   python scripts/status_check.py --outdir output
   ```
5. **Review results**:
   - Start with `note_leads_email_ready.csv` for best candidates
   - Check `review_queue.csv` for additional opportunities
   - Use `discarded.csv` to understand what was filtered out
6. **Send to enrichment**:
   - Export `note_leads_email_ready.csv` to enrichment provider
   - Provider returns emails matched by `lead_id`
7. **Merge emails back**:
   - Use `lead_id` to match enrichment results
   - Update `email` column in email-ready CSV
8. **Upload to campaign platform**:
   - Import `note_leads_email_ready.csv` to Instantly/Smartlead
   - Launch email campaign
9. **Tune thresholds** in `filter_notes.py` if needed
10. **Re-run** with adjusted settings

## Requirements

- Python 3.7+
- Standard library only (no external dependencies)
- CSV files with UTF-8 encoding

## Files

```
scripts/
  ├── column_mapper.py    # Auto-maps CSV columns to canonical fields
  ├── filter_notes.py     # Classification logic and filtering rules
  ├── run_pipeline.py      # Main entry point
  └── status_check.py      # Validation and statistics

output/
  ├── note_leads_email_ready.csv  # Email campaign ready (Instantly/Smartlead)
  ├── note_leads_mail_ready.csv   # Mail campaign ready
  ├── review_queue.csv            # Needs manual review
  ├── discarded.csv               # Filtered out
  └── column_mapping.json         # Column mapping used
```

## Lead ID for Enrichment Matching

Each lead has a deterministic `lead_id` generated from:
- Full name
- Mailing zip code
- Original loan amount
- Recording date

This allows you to:
1. Send `note_leads_email_ready.csv` to enrichment provider
2. Provider matches by `lead_id` and returns emails
3. Merge emails back using `lead_id` as the key
4. Upload enriched CSV to campaign platform

The `lead_id` ensures stable matching even if other fields change.

## TCAD-Only Property Targets Pipeline

**IMPORTANT:** This generates property owner targets, NOT verified note holders.

### Usage

```bash
# Basic usage
python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output

# With custom filters
python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output --min_value 150000 --max_value 600000 --only_absentee true

# With value bucketing (splits into 100k value buckets)
python scripts/generate_property_targets.py --tcad output/prop_clean.csv --outdir output --enable_bucketing true
```

### Output Files

- **property_targets_email_ready.csv** - Property owner targets ready for outreach
- **property_targets_review.csv** - Properties needing manual review
- **property_targets_discarded.csv** - Properties filtered out
- **property_targets_XXXk-YYYk.csv** - Value-bucketed files (if bucketing enabled)

### Filters

- **Excludes institutional owners:** Banks, servicers, government entities (BANK, N.A., MORTGAGE, SERVICING, FNMA, FANNIE, FREDDIE, HUD, VA, USDA, etc.)
- **Keeps PERSON/TRUST/LLC:** Uses heuristics to classify owner types
- **Prefer absentee owners:** Mailing address != situs address (configurable)
- **Value range filter:** Total value between min/max (default: $150k-$600k)

### Output Columns

- `lead_id` - Deterministic hash for matching
- `full_name` / `company_name` - Owner name (split by type)
- `owner_type` - PERSON, LLC, TRUST, UNKNOWN
- `mailing_address`, `mailing_city`, `mailing_state`, `mailing_zip` - Mailing address
- `situs_address`, `situs_city`, `situs_state`, `situs_zip` - Property address
- `tcad_account_id` - TCAD account identifier
- `owner_occupied_guess` - Y/N based on address match
- `total_value` - Property total value
- `property_type` - Property type code
- `lead_score` - Quality score (0-100)
- `why_flagged` - Summary of why this is a target

### Configuration

Edit `scripts/generate_property_targets.py` to adjust:
- `DEFAULT_MIN_VALUE` / `DEFAULT_MAX_VALUE` - Value range defaults
- `INSTITUTIONAL_KEYWORDS` - Keywords to exclude
- Scoring weights in `calculate_lead_score()`

