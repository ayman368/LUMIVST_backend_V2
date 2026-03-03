"""
التحقق من وجود البيانات الأساسية (open, high, low) لتاريخ محدد
"""
import sys
import argparse
from pathlib import Path
from datetime import date, datetime
from sqlalchemy import func

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.models.price import Price

def verify_base_data(target_date_str=None):
    db = SessionLocal()
    
    try:
        # تحديد التاريخ
        if target_date_str:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        else:
            target_date = date.today()
        
        print("=" * 70)
        print(f"🔍 CHECKING DATA FOR {target_date}")
        print("=" * 70)
        
        # 1. Check if any price data exists for this date
        query = db.query(Price).filter(Price.date == target_date)
        records = query.all()
        
        count = len(records)
        print(f"\n✅ Total price records for {target_date}: {count}")
        
        if count == 0:
            print("❌ ERROR: NO PRICE DATA FOUND FOR THIS DATE!")
            print("⚠️  You MUST scrape the data first before running calculations!")
            return False
        
        # 2. Check if all required fields are populated
        print(f"\n📊 Sample Records (first 5):")
        print("-" * 70)
        
        for i, rec in enumerate(records[:5]):
            print(f"\n{i+1}. Symbol: {rec.symbol}")
            print(f"   Close: {rec.close}")
            print(f"   Open:  {rec.open}")
            print(f"   High:  {rec.high}")
            print(f"   Low:   {rec.low}")
            
        # 3. Check for NULL values in critical fields
        print(f"\n🔎 Checking for NULL values in critical fields...")
        
        null_checks = {
            'open': db.query(func.count()).filter(Price.date == target_date, Price.open.is_(None)).scalar(),
            'high': db.query(func.count()).filter(Price.date == target_date, Price.high.is_(None)).scalar(),
            'low': db.query(func.count()).filter(Price.date == target_date, Price.low.is_(None)).scalar(),
            'close': db.query(func.count()).filter(Price.date == target_date, Price.close.is_(None)).scalar(),
        }
        
        print("\nNULL count per field:")
        all_good = True
        for field, null_count in null_checks.items():
            status = "✅" if null_count == 0 else "⚠️"
            print(f"  {status} {field}: {null_count} NULLs")
            if null_count > 0:
                all_good = False
        
        if not all_good:
            print("\n⚠️  WARNING: Some columns have NULL values!")
            return False
        
        print(f"\n✅ DATA VALIDATION PASSED!")
        print(f"✅ Ready to proceed to calculations phase!")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Verify base price data exists for a specific date',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify_base_data.py                    # Check today's date
  python verify_base_data.py --date 2026-03-02  # Check specific date
        """
    )
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    success = verify_base_data(args.date)
    sys.exit(0 if success else 1)
