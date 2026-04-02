import sys
import os
import argparse
from datetime import date
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal

def delete_data_for_date(target_date_str: str):
    """
    يحذف كل البيانات ليوم محدد من قاعدة البيانات
    """
    db = SessionLocal()
    
    try:
        print(f"🗑️ جاري حذف بيانات يوم: {target_date_str}")
        
        # 1. مسح البيانات من جدول prices
        res_prices = db.execute(
            text("DELETE FROM prices WHERE date = :target_date"),
            {"target_date": target_date_str}
        )
        print(f"✅ تم حذف {res_prices.rowcount} سجل من جدول prices")
        
        # 2. مسح البيانات من جدول stock_indicators
        res_indicators = db.execute(
            text("DELETE FROM stock_indicators WHERE date = :target_date"),
            {"target_date": target_date_str}
        )
        print(f"✅ تم حذف {res_indicators.rowcount} سجل من جدول stock_indicators")
        
        # 3. مسح البيانات من جدول rs_daily_v2
        res_rs = db.execute(
            text("DELETE FROM rs_daily_v2 WHERE date = :target_date"),
            {"target_date": target_date_str}
        )
        print(f"✅ تم حذف {res_rs.rowcount} سجل من جدول rs_daily_v2")
        # 4. إعادة تعيين latest_ready_date في update_status (للحفاظ على الـ Atomic Switch)
        res_update = db.execute(text("""
            UPDATE update_status 
            SET latest_ready_date = (SELECT MAX(date) FROM prices)
            WHERE id = 1
        """))
        print(f"✅ تم إرجاع المؤشر (latest_ready_date) لتاريخ اليوم السابق بنجاح")
        
        db.commit()
        print("\n🎉 تم الحذف والتأكيد بنجاح!")
        
    except Exception as e:
        print(f"❌ حدث خطأ: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Delete all calculated data for a specific date')
    parser.add_argument('--date', type=str, required=True, help='Target date in YYYY-MM-DD format (e.g., 2026-03-10)')
    
    args = parser.parse_args()
    delete_data_for_date(args.date)
