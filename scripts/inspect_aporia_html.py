"""Inspect raw Aporia HTML to find the correct CSS class for Weekly % Return."""
import json
from bs4 import BeautifulSoup

for fname in ["raw_largest_market_cap.json", "raw_all_analytics.json", "raw_strongest_uptrends.json"]:
    print(f"\n{'='*60}")
    print(f"FILE: {fname}")
    print('='*60)
    
    with open(f"aporia_out/{fname}", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    html = "".join(data.get("htmlData", []))
    soup = BeautifulSoup(html, "html.parser")
    
    # Get first row
    tr = soup.find("tr", id=True)
    if not tr:
        print("No rows found!")
        continue
    
    tds = tr.find_all("td")
    print(f"Total TDs in first row: {len(tds)}")
    print(f"Row ID: {tr.get('id')}")
    print()
    
    for i, td in enumerate(tds):
        cls = td.get("class", ["-"])
        text = td.get_text(strip=True)[:50]
        print(f"  [{i:2d}] class={cls!s:35s} text={text}")
