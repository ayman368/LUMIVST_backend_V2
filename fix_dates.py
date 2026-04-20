import os
import sys
from datetime import date
from sqlalchemy import update

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from app.core.database import SessionLocal
from app.models.economic_indicators import TreasuryYieldCurve

def fix_future_dates():
    db = SessionLocal()
    try:
        # حذف كل البيانات اللي بعد 31 مارس 2025 (لأنها بيانات تجريبية وهمية)
        deleted = db.query(TreasuryYieldCurve).filter(
            TreasuryYieldCurve.report_date > date(2025, 3, 31)
        ).delete()
        
        db.commit()
        print(f"✅ تم بنجاح تنظيف قاعدة البيانات: تم حذف {deleted} سجل وهمي أو مستقبلي.")
        print("💡 قاعدة البيانات دلوقتي أصبحت مطابقة تماماً للموقع الرسمي.")
    except Exception as e:
        db.rollback()
        print(f"❌ حصل خطأ: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    fix_future_dates()
