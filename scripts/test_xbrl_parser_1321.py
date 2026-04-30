"""
Test XBRL Parser on Company 1321
=================================
Quick test script to parse all XBRL Excel files for company 1321
and display extracted parameters without saving to DB.

Usage:
    python scripts/test_xbrl_parser_1321.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.services.xbrl_parser import parse_xbrl_file, merge_files

SYMBOL = "1321"
BASE_DIR = Path(__file__).resolve().parent.parent / "data" / "downloads" / SYMBOL


def main():
    if not BASE_DIR.exists():
        print(f"❌ Folder not found: {BASE_DIR}")
        return

    # 1. Find all XBRL Excel files
    xbrl_files = sorted([
        f for f in BASE_DIR.iterdir()
        if "XBRL" in f.name and f.suffix in ('.xls', '.xlsx')
    ])

    if not xbrl_files:
        print(f"❌ No XBRL files found in {BASE_DIR}")
        return

    print(f"{'='*70}")
    print(f"📊 XBRL Parser Test — Company {SYMBOL}")
    print(f"📁 Folder: {BASE_DIR}")
    print(f"📄 Found {len(xbrl_files)} XBRL files:")
    for f in xbrl_files:
        print(f"   • {f.name} ({f.stat().st_size / 1024:.0f} KB)")
    print(f"{'='*70}\n")

    # 2. Parse each file individually
    all_results = []
    for filepath in xbrl_files:
        print(f"\n{'─'*60}")
        print(f"📄 Parsing: {filepath.name}")
        print(f"{'─'*60}")
        
        try:
            result = parse_xbrl_file(str(filepath))
            all_results.append(result)
            
            # Display Meta
            meta = result.get("meta", {})
            print(f"\n  📋 META:")
            for k, v in meta.items():
                print(f"     {k}: {v}")
            
            # Display Sections
            sections = result.get("sections", {})
            if not sections:
                print(f"\n  ⚠️  No financial sections found!")
            else:
                for sec_name, sec_data in sections.items():
                    periods = sec_data.get("periods", [])
                    items = sec_data.get("items", [])
                    data_items = [it for it in items if not it.get("is_header")]
                    header_items = [it for it in items if it.get("is_header")]
                    
                    print(f"\n  📊 Section: {sec_name}")
                    print(f"     Periods: {periods}")
                    print(f"     Total items: {len(items)} ({len(header_items)} headers, {len(data_items)} data rows)")
                    
                    # Show first 10 data items as sample
                    print(f"     Sample parameters (first 10):")
                    count = 0
                    for item in items:
                        if item.get("is_header"):
                            print(f"       📁 [HEADER] {item['label']}")
                        else:
                            vals = item.get("values", {})
                            vals_str = " | ".join(f"{p}: {v}" for p, v in vals.items())
                            print(f"       → {item['label']}: {vals_str}")
                            count += 1
                        if count >= 10:
                            remaining = len(data_items) - 10
                            if remaining > 0:
                                print(f"       ... and {remaining} more data rows")
                            break
                            
        except Exception as e:
            print(f"  ❌ Error parsing: {e}")
            import traceback
            traceback.print_exc()

    # 3. Test merge
    print(f"\n\n{'='*70}")
    print(f"🔗 MERGE RESULTS — All {len(all_results)} files merged")
    print(f"{'='*70}")
    
    try:
        merged = merge_files(all_results)
        meta = merged.get("meta", {})
        sections = merged.get("sections", {})
        
        print(f"\n  📋 MERGED META:")
        for k, v in meta.items():
            if k == "source_files":
                print(f"     {k}: ({len(v)} files)")
                for sf in v:
                    print(f"       • {sf}")
            else:
                print(f"     {k}: {v}")
        
        print(f"\n  📊 MERGED SECTIONS SUMMARY:")
        total_params = 0
        for sec_name, sec_data in sections.items():
            periods = sec_data.get("periods", [])
            items = sec_data.get("items", [])
            data_items = [it for it in items if not it.get("is_header")]
            total_params += len(data_items)
            
            print(f"\n     ┌─ {sec_name}")
            print(f"     │  Periods ({len(periods)}): {', '.join(periods)}")
            print(f"     │  Parameters: {len(data_items)} data rows")
            print(f"     │  Headers: {len(items) - len(data_items)}")
            
            # Show all parameters for this section
            print(f"     │  ── All Parameters ──")
            for item in items:
                if item.get("is_header"):
                    print(f"     │  📁 {item['label']}")
                else:
                    vals = item.get("values", {})
                    n_vals = sum(1 for v in vals.values() if isinstance(v, (int, float)))
                    print(f"     │  → {item['label']} ({n_vals} numeric values)")
            print(f"     └─")
        
        print(f"\n  ✅ TOTAL: {len(sections)} sections, {total_params} unique parameters")
        
    except Exception as e:
        print(f"\n  ❌ Merge error: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n{'='*70}")
    print(f"✅ Test Complete!")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
