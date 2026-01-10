"""
TCAD Appraisal Export Parser

This script parses TCAD-style appraisal export ZIP files containing fixed-width text files.

Usage:
    python main.py --zip input/tcad_export.zip --out output
    
    Or with defaults (input/tcad_export.zip -> output/):
    python main.py

Requirements:
    - Python 3.11+
    - input/tcad_export.zip (or specify with --zip)
    - The ZIP should contain PROP*.TXT or PROP.TXT file(s)

The script will:
    1. Extract ZIP to a temporary folder
    2. Locate the property file (PROP*.TXT or PROP.TXT)
    3. Parse fixed-width records using FIELD_SPECS configuration
    4. Output clean records to output/prop_clean.csv
    5. Output errors to output/prop_errors.csv

To update field positions:
    Edit the FIELD_SPECS dictionary below with (start_pos, end_pos) tuples.
    Positions are 1-based and inclusive (e.g., (1, 10) means chars 1-10).
"""

import argparse
import csv
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Tuple, Optional

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ============================================================================
# FIELD SPECIFICATION
# ============================================================================
# Update this dictionary with field positions from your PDF layout.
# Format: "field_name": (start_pos, end_pos)
# Positions are 1-based and inclusive (e.g., (1, 10) means characters 1-10).

FIELD_SPECS: Dict[str, Tuple[int, int]] = {
    # Field positions from Legacy8.0.30-AppraisalExportLayout.xlsx
    "account_id": (1, 12),           # prop_id
    "owner_name": (609, 678),        # py_owner_name
    # situs_address is combined from situs_street_prefx + situs_street + situs_street_suffix
    "situs_city": (1110, 1139),      # situs_city
    "situs_state": (924, 925),       # Using py_addr_state first 2 chars (situs state not in layout, using property year state as fallback)
    "situs_zip": (1140, 1149),       # situs_zip
    "mailing_address": (694, 753),   # py_addr_line1 (Property Year Owner Address Line 1)
    "mailing_city": (874, 923),      # py_addr_city (Property Year Owner Address City)
    "mailing_state": (924, 925),     # py_addr_state first 2 chars (Property Year Owner Address State)
    "mailing_zip": (979, 983),       # py_addr_zip (Property Year Owner Address Zip)
    "property_type": (13, 17),       # prop_type_cd
    "land_value": (1796, 1810),      # land_hstd_val (Land Homestead Value)
    "improvement_value": (1826, 1840), # imprv_hstd_val (Improvement Homestead Value)
    "total_value": (1916, 1930),     # appraised_val (Appraised Value)
    "assessed_year": (18, 22),       # prop_val_yr (Appraisal or Tax Year)
    
    # Helper fields for situs_address combination
    "situs_street_prefx": (1040, 1049),
    "situs_street": (1050, 1099),
    "situs_street_suffix": (1100, 1109),
}

# Required output columns (always included, even if blank)
REQUIRED_COLUMNS = [
    "account_id", "owner_name", "situs_address", "situs_city", "situs_state", "situs_zip",
    "mailing_address", "mailing_city", "mailing_state", "mailing_zip", "property_type",
    "land_value", "improvement_value", "total_value", "assessed_year"
]

# Progress reporting interval
PROGRESS_INTERVAL = 50000


def find_property_file(temp_dir: Path) -> Optional[Path]:
    """Find the property file (PROP*.TXT or PROP.TXT) in the extracted directory."""
    # First try files starting with PROP and ending with .TXT
    prop_files = list(temp_dir.glob("PROP*.TXT"))
    if prop_files:
        # Prefer PROP.TXT if it exists, otherwise return first match
        exact_match = temp_dir / "PROP.TXT"
        if exact_match.exists():
            return exact_match
        return prop_files[0]
    
    # Fallback: try case-insensitive search
    for file in temp_dir.iterdir():
        if file.is_file() and file.suffix.upper() == ".TXT":
            name_upper = file.name.upper()
            if name_upper.startswith("PROP"):
                return file
    
    return None


def extract_field(line: str, start_pos: int, end_pos: int) -> str:
    """
    Extract a field from a line using 1-based inclusive positions.
    
    Args:
        line: The line to extract from
        start_pos: 1-based start position (inclusive)
        end_pos: 1-based end position (inclusive)
    
    Returns:
        The extracted field, stripped of whitespace
    """
    # Convert to 0-based indexing
    start_idx = start_pos - 1
    end_idx = end_pos
    
    # Handle lines shorter than expected
    if len(line) < end_idx:
        end_idx = len(line)
    
    if start_idx >= len(line):
        return ""
    
    return line[start_idx:end_idx].strip()


def parse_numeric_field(value: str, field_name: str) -> Optional[float]:
    """
    Parse a numeric field, returning None if conversion fails.
    
    Args:
        value: String value to convert
        field_name: Name of the field (for error messages)
    
    Returns:
        Float value or None if conversion fails
    """
    if not value or not value.strip():
        return None
    
    try:
        # Remove common formatting characters
        cleaned = value.strip().replace(",", "").replace("$", "").replace(" ", "")
        if cleaned:
            return float(cleaned)
    except (ValueError, AttributeError):
        pass
    
    return None


def parse_line(line: str, line_number: int) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Parse a single fixed-width line into a dictionary.
    
    Args:
        line: The line to parse
        line_number: Line number for error reporting
    
    Returns:
        Tuple of (parsed_record, error_message)
        error_message is None if parsing succeeded
    """
    record = {}
    errors = []
    
    # Check if line is shorter than minimum expected length
    max_end_pos = max(end for _, end in FIELD_SPECS.values())
    if len(line) < max_end_pos:
        return (
            {},
            f"Line {line_number}: Line too short (expected at least {max_end_pos} chars, got {len(line)})"
        )
    
    # Special handling for situs_address - combine prefix, street, and suffix
    situs_parts = []
    if "situs_street_prefx" in FIELD_SPECS:
        prefix = extract_field(line, FIELD_SPECS["situs_street_prefx"][0], FIELD_SPECS["situs_street_prefx"][1])
        if prefix:
            situs_parts.append(prefix)
    if "situs_street" in FIELD_SPECS:
        street = extract_field(line, FIELD_SPECS["situs_street"][0], FIELD_SPECS["situs_street"][1])
        if street:
            situs_parts.append(street)
    if "situs_street_suffix" in FIELD_SPECS:
        suffix = extract_field(line, FIELD_SPECS["situs_street_suffix"][0], FIELD_SPECS["situs_street_suffix"][1])
        if suffix:
            situs_parts.append(suffix)
    situs_address = " ".join(situs_parts).strip()
    
    # Extract all fields
    for field_name in REQUIRED_COLUMNS:
        if field_name == "situs_address":
            # Use the combined situs address
            record[field_name] = situs_address
        elif field_name in FIELD_SPECS:
            start_pos, end_pos = FIELD_SPECS[field_name]
            value = extract_field(line, start_pos, end_pos)
            # Special handling for state fields - take only first 2 characters
            if field_name in ["situs_state", "mailing_state"] and len(value) > 2:
                value = value[:2].strip()
            record[field_name] = value
        else:
            # Field not in spec, set to empty
            record[field_name] = ""
    
    # Validate numeric fields
    numeric_fields = ["land_value", "improvement_value", "total_value"]
    for field_name in numeric_fields:
        if field_name in record:
            value = record[field_name]
            if value:  # Only validate if non-empty
                parsed = parse_numeric_field(value, field_name)
                if parsed is None:
                    errors.append(f"Line {line_number}: Failed to parse numeric field '{field_name}': '{value}'")
                else:
                    # Keep original string value for CSV output
                    # (or convert to numeric - user can decide)
                    record[field_name] = value  # Keep as string for now
    
    error_msg = "; ".join(errors) if errors else None
    return record, error_msg


def process_property_file(prop_file: Path, output_clean: Path, output_errors: Path):
    """Process the property file and write clean records and errors to CSV files."""
    print(f"Processing: {prop_file.name}")
    print(f"Output clean records: {output_clean}")
    print(f"Output errors: {output_errors}")
    
    # Ensure output directory exists
    output_clean.parent.mkdir(parents=True, exist_ok=True)
    output_errors.parent.mkdir(parents=True, exist_ok=True)
    
    clean_count = 0
    error_count = 0
    total_lines = 0
    
    # Open output files
    with open(output_clean, 'w', newline='', encoding='utf-8') as clean_file, \
         open(output_errors, 'w', newline='', encoding='utf-8') as error_file:
        
        clean_writer = csv.DictWriter(clean_file, fieldnames=REQUIRED_COLUMNS)
        error_writer = csv.writer(error_file)
        
        # Write headers
        clean_writer.writeheader()
        error_writer.writerow(["line_number", "line_content", "error_reason"])
        
        # Process file line by line (streaming)
        with open(prop_file, 'r', encoding='utf-8', errors='replace') as infile:
            # Use tqdm if available, otherwise manual progress tracking
            if HAS_TQDM:
                # Count lines first for progress bar (optional, or just use iter)
                lines = infile.readlines()
                total_lines_estimate = len(lines)
                infile.seek(0)  # Reset to beginning
                lines = infile
                progress_iter = tqdm(enumerate(lines, start=1), total=total_lines_estimate, desc="Processing lines")
            else:
                progress_iter = enumerate(infile, start=1)
            
            for line_number, line in progress_iter:
                total_lines = line_number
                
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Parse line
                try:
                    record, error_msg = parse_line(line.rstrip('\n\r'), line_number)
                    
                    if error_msg:
                        # Write to errors CSV
                        error_writer.writerow([line_number, line.rstrip('\n\r'), error_msg])
                        error_count += 1
                    else:
                        # Write to clean CSV
                        clean_writer.writerow(record)
                        clean_count += 1
                    
                    # Print progress every N lines (if not using tqdm)
                    if not HAS_TQDM and line_number % PROGRESS_INTERVAL == 0:
                        print(f"Processed {line_number:,} lines (clean: {clean_count:,}, errors: {error_count:,})")
                
                except Exception as e:
                    # Never crash on bad lines
                    error_writer.writerow([line_number, line.rstrip('\n\r'), f"Unexpected error: {str(e)}"])
                    error_count += 1
    
    print(f"\nProcessing complete!")
    print(f"Total lines processed: {total_lines:,}")
    print(f"Clean records: {clean_count:,}")
    print(f"Error records: {error_count:,}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse TCAD appraisal export ZIP files containing fixed-width text files."
    )
    parser.add_argument(
        "--zip",
        type=str,
        default="input/tcad_export.zip",
        help="Path to input ZIP file (default: input/tcad_export.zip)"
    )
    parser.add_argument(
        "--out",
        type=str,
        default="output",
        help="Output directory (default: output)"
    )
    
    args = parser.parse_args()
    
    zip_path = Path(args.zip)
    output_dir = Path(args.out)
    
    # Validate input ZIP exists
    if not zip_path.exists():
        print(f"Error: ZIP file not found: {zip_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Input ZIP: {zip_path}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Create temporary directory for extraction
    with tempfile.TemporaryDirectory(prefix="tcad_parser_") as temp_dir:
        temp_path = Path(temp_dir)
        print(f"Extracting to temporary directory...")
        
        # Extract ZIP (streaming-friendly, doesn't load entire files)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_path)
                print(f"Extracted {len(zip_ref.namelist())} file(s)")
        except zipfile.BadZipFile:
            print(f"Error: {zip_path} is not a valid ZIP file", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error extracting ZIP: {e}", file=sys.stderr)
            sys.exit(1)
        
        # Find property file
        prop_file = find_property_file(temp_path)
        if not prop_file:
            print("Error: Could not find PROP*.TXT or PROP.TXT file in ZIP", file=sys.stderr)
            print(f"Files in ZIP: {list(temp_path.iterdir())}", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found property file: {prop_file.name}")
        print()
        
        # DEBUG: inspect first few lines
        with open(prop_file, "r", encoding="utf-8", errors="replace") as f:
            for i in range(1, 6):
                line = f.readline()
                print(f"\nLINE {i} LEN={len(line.rstrip())}")
                # Show first 500 chars to see field boundaries
                print(repr(line[:500]))
                # Also show a section from middle and end
                if len(line) > 1000:
                    print(f"  MIDDLE (chars 1000-1500): {repr(line[1000:1500])}")
                    print(f"  END (last 200 chars): {repr(line[-200:])}")
        
        print()
        
        # Process the file
        output_clean = output_dir / "prop_clean.csv"
        output_errors = output_dir / "prop_errors.csv"
        
        process_property_file(prop_file, output_clean, output_errors)
        
        print(f"\nOutput files:")
        print(f"  Clean records: {output_clean}")
        print(f"  Errors: {output_errors}")


if __name__ == "__main__":
    main()


