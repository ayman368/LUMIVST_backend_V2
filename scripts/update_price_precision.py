"""
تحديث دقة أعمدة الأسعار من NUMERIC(12, 2) إلى NUMERIC(12, 4)
لتصحيح مشكلة فقدان الدقة في حسابات المؤشرات
"""

import sys
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal

def update_price_precision():
    """تحديث دقة أعمدة الأسعار في Database"""
    db = SessionLocal()
    
    try:
        print("=" * 70)
        print("🔧 Updating Price Precision: NUMERIC(12, 2) → NUMERIC(12, 4)")
        print("=" * 70)
        
        # الأعمدة التي نريد تحديثها
        columns = ['open', 'high', 'low', 'close']
        
        for col in columns:
            print(f"\n📝 Updating column 'prices.{col}'...")
            
            # تحديث نوع البيانات
            alter_query = text(f"""
                ALTER TABLE prices
                ALTER COLUMN {col} TYPE NUMERIC(12, 4);
            """)
            
            try:
                db.execute(alter_query)
                db.commit()
                print(f"   ✅ Successfully updated 'prices.{col}' to NUMERIC(12, 4)")
            except Exception as e:
                db.rollback()
                print(f"   ❌ Error updating column: {e}")
                raise
        
        print("\n" + "=" * 70)
        print("✅ Price Precision Update Complete!")
        print("=" * 70)
        print("\n📊 Next Steps:")
        print("   1. ✅ Database schema updated")
        print("   2. ⏳ Clear technical-screener indicators (already done)")
        print("   3. ⏳ Re-scrape price data OR recalculate with current data")
        print("   4. ⏳ Re-run phase6_only.py to recalculate indicators")
        print("   5. ⏳ Verify results match expected values")
        
    finally:
        db.close()

if __name__ == "__main__":
    update_price_precision()
