import zipfile
from pathlib import Path
import re

# Extract and analyze PROP.TXT
zip_path = Path("2025 Certified Appraisal Export Supp 0_07202025.zip")
extract_dir = Path("temp_extract")

with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(extract_dir)

prop_file = extract_dir / "PROP.TXT"
lines = prop_file.read_text(encoding='utf-8').splitlines()[:10]

# Analyze first line to find field boundaries
line = lines[0]
print(f"Line length: {len(line)}")
print(f"\nFull first line (first 1500 chars):")
print(repr(line[:1500]))
print(f"\n")

# Look for patterns - find where non-space text starts after spaces
print("Non-empty sections (potential fields):")
i = 0
while i < len(line):
    if line[i] != ' ':
        start = i
        # Find where this section ends
        while i < len(line) and line[i] != ' ':
            i += 1
        end = i - 1
        value = line[start:end+1]
        print(f"  Positions {start+1:4d}-{end+1:4d}: {repr(value[:50])}")
    else:
        i += 1
    if i > 2000:  # Limit output
        break

# Try to identify specific fields by pattern
print(f"\n=== Trying to identify fields by pattern ===")
print(f"Account ID (positions 1-15): {repr(line[0:15])}")

# Look for ZIP codes (5 digits, sometimes with -)
zip_pattern = r'\d{5}(?:-\d{4})?'
for match in re.finditer(zip_pattern, line):
    print(f"Possible ZIP at positions {match.start()+1}-{match.end()}: {match.group()}")

# Look for state codes (2 letters)
state_pattern = r'\b[A-Z]{2}\b'
for match in re.finditer(state_pattern, line):
    print(f"Possible state at positions {match.start()+1}-{match.end()}: {match.group()}")



