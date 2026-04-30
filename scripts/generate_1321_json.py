"""
Generate JSON for company 1321 from local XBRL files.
Saves to output/1321_financials.json — the file that the API reads from.
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.services.xbrl_parser import parse_and_merge_xbrl_files
from app.services.xbrl_data_service import save_company

SYMBOL = "1321"
BASE_DIR = Path(__file__).resolve().parent.parent / "data" / "downloads" / SYMBOL

xbrl_files = sorted([
    f for f in BASE_DIR.iterdir()
    if "XBRL" in f.name and f.suffix in ('.xls', '.xlsx')
])

print(f"📄 Found {len(xbrl_files)} XBRL files for {SYMBOL}")
merged = parse_and_merge_xbrl_files(xbrl_files)

sections = merged.get("sections", {})
for sec_name, sec_data in sections.items():
    print(f"  ✅ {sec_name}: {len(sec_data['items'])} items, {len(sec_data['periods'])} periods")

output_path = save_company(SYMBOL, merged)
print(f"\n💾 Saved to: {output_path}")
