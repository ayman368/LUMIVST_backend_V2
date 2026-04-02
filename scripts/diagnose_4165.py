"""
Diagnostic script to check why stock 4165 is not appearing in Industry Groups page.
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.models.price import Price
from app.models.industry_group import IndustryGroupHistory
from sqlalchemy import func, text

db = SessionLocal()

print("=" * 70)
print("DIAGNOSING STOCK 4165 IN INDUSTRY GROUPS")
print("=" * 70)

# 1. Check if 4165 exists in prices table at all
stock_records = db.query(Price).filter(Price.symbol == '4165').order_by(Price.date.desc()).limit(5).all()

if not stock_records:
    print("\n[X] Stock 4165 NOT FOUND in prices table at all!")
else:
    print(f"\n[OK] Stock 4165 found in prices table. Latest records:")
    for s in stock_records:
        print(f"   Date: {s.date} | Close: {s.close} | Industry Group: '{s.industry_group}' | Sector: '{s.sector}'")

# 2. Check the latest date in prices
latest_date = db.query(func.max(Price.date)).scalar()
print(f"\nLatest date in prices table: {latest_date}")

# 3. Check if 4165 has a record for the latest date
latest_record = db.query(Price).filter(Price.symbol == '4165', Price.date == latest_date).first()
if latest_record:
    print(f"[OK] Stock 4165 EXISTS for latest date ({latest_date})")
    print(f"   Industry Group: '{latest_record.industry_group}'")
    print(f"   Sector: '{latest_record.sector}'")
    print(f"   Industry: '{latest_record.industry}'")
    print(f"   Sub Industry: '{latest_record.sub_industry}'")
else:
    print(f"[X] Stock 4165 DOES NOT EXIST for latest date ({latest_date})")
    all_dates = db.query(Price.date).filter(Price.symbol == '4165').order_by(Price.date.desc()).limit(10).all()
    print(f"   Latest dates for 4165: {[str(d[0]) for d in all_dates]}")

# 4. Check if the industry group exists in IndustryGroupHistory
if stock_records and stock_records[0].industry_group:
    group_name = stock_records[0].industry_group
    print(f"\nChecking Industry Group: '{group_name}'")
    
    latest_ig_date = db.query(func.max(IndustryGroupHistory.date)).scalar()
    print(f"   Latest date in industry_group_history: {latest_ig_date}")
    
    ig_record = db.query(IndustryGroupHistory).filter(
        IndustryGroupHistory.industry_group == group_name,
        IndustryGroupHistory.date == latest_ig_date
    ).first()
    
    if ig_record:
        print(f"   [OK] Group '{group_name}' EXISTS in industry_group_history")
        print(f"      Rank: {ig_record.rank} | Num Stocks: {ig_record.number_of_stocks}")
    else:
        print(f"   [X] Group '{group_name}' NOT FOUND in industry_group_history for {latest_ig_date}")
        
        all_groups = db.query(IndustryGroupHistory.industry_group).filter(
            IndustryGroupHistory.date == latest_ig_date
        ).distinct().all()
        
        similar = [g[0] for g in all_groups if group_name.lower() in g[0].lower() or g[0].lower() in group_name.lower()]
        if similar:
            print(f"   Similar groups found: {similar}")
        
        count_in_group = db.query(func.count(Price.symbol)).filter(
            Price.date == latest_date,
            Price.industry_group == group_name
        ).scalar()
        print(f"   Stocks in group '{group_name}' on {latest_date}: {count_in_group}")
        
        if count_in_group < 2:
            print(f"   >>> PROBLEM: Group has {count_in_group} stock(s). Script requires >= 2!")
elif stock_records:
    print(f"\n[X] Stock 4165 has NULL/empty industry_group! This is the problem.")

# 5. Check what stocks are in the same group
if stock_records and stock_records[0].industry_group:
    group_name = stock_records[0].industry_group
    stocks_in_group = db.query(Price.symbol, Price.company_name).filter(
        Price.date == latest_date,
        Price.industry_group == group_name
    ).all()
    print(f"\nAll stocks in '{group_name}' on {latest_date}:")
    for s in stocks_in_group:
        print(f"   {s.symbol} - {s.company_name}")

db.close()
print("\n" + "=" * 70)
print("Diagnosis complete")
print("=" * 70)
