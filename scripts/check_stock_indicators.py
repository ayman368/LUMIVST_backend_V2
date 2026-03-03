"""
التحقق من وجود بيانات stock_indicators ليوم 2026-03-02
"""
import sys
from pathlib import Path
from datetime import date
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal

db = SessionLocal()

try:
    target_date = date(2026, 3, 2)
    
    print("=" * 70)
    print(f"🔍 CHECKING STOCK_INDICATORS FOR {target_date}")
    print("=" * 70)
    
    # Check if stock_indicators table exists
    result = db.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'stock_indicators'
        ) as exists;
    """))
    
    table_exists = result.fetchone()[0]
    
    if not table_exists:
        print("❌ ERROR: stock_indicators table DOES NOT EXIST!")
        print("⚠️  This table needs to be created first.")
        sys.exit(1)
    
    print("✅ stock_indicators table exists")
    
    # Check records for this date
    result = db.execute(text("""
        SELECT COUNT(*) FROM stock_indicators WHERE date = :target_date
    """), {"target_date": target_date})
    
    count = result.fetchone()[0]
    print(f"\n📊 Records for {target_date}: {count}")
    
    if count == 0:
        print("❌ NO RECORDS FOUND for this date!")
        print("⚠️  This might be the first run, data will be created.")
    else:
        print(f"✅ {count} records found")
        
        # Show sample record
        result = db.execute(text("""
            SELECT symbol, date, 
                   rsi_14, sma9_rsi, ema20_sma3, 
                   sma4, sma9, sma18, sma4_w, sma9_w, sma18_w,
                   cci, aroon_up
            FROM stock_indicators 
            WHERE date = :target_date
            LIMIT 1
        """), {"target_date": target_date})
        
        row = result.fetchone()
        if row:
            print(f"\n📋 Sample record:")
            print(f"   Symbol: {row[0]}")
            print(f"   Date: {row[1]}")
            print(f"   RSI14: {row[2]}")
            print(f"   SMA9 RSI: {row[3]}")
            print(f"   EMA20 SMA3: {row[4]}")
            print(f"   SMA4: {row[5]}")
            print(f"   SMA9: {row[6]}")
            print(f"   SMA18: {row[7]}")
    
    print("\n" + "=" * 70)
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    db.close()
