import zipfile
from pathlib import Path

# Extract and analyze PROP.TXT
zip_path = Path("2025 Certified Appraisal Export Supp 0_07202025.zip")
extract_dir = Path("temp_extract")

with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(extract_dir)

prop_file = extract_dir / "PROP.TXT"
lines = prop_file.read_text(encoding='utf-8').splitlines()[:5]

print(f"Line length: {len(lines[0])}")
print(f"\nAnalyzing first 3 lines in 100-char chunks:\n")

for i, line in enumerate(lines[:3]):
    print(f"=== Line {i+1} ===")
    for j in range(0, min(1000, len(line)), 100):
        chunk = line[j:j+100]
        print(f"Pos {j:4d}-{j+99:4d}: {repr(chunk)}")



