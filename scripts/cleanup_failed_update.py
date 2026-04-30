"""
🧹 سكريبت تنظيف التحديث الفاشل
حذف بيانات تاريخ 22-04-2026 من جميع الجداول المتأثرة
لأن الإنترنت قطع أثناء آخر مراحل التحديث
"""

import sys
from pathlib import Path
import logging
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# إضافة المسار
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.config import settings
from app.core.database import SessionLocal

# إعداد Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# التاريخ المراد حذفه
TARGET_DATE = date(2026, 4, 22)

def cleanup_failed_update():
    """
    حذف آمن لبيانات التحديث الفاشل من جميع الجداول
    """
    db = SessionLocal()
    
    try:
        logger.info(f"🧹 بدء تنظيف بيانات تاريخ {TARGET_DATE}...")
        
        # 1️⃣ حذف من rs_daily_v2
        logger.info("🗑️ حذف بيانات RS من rs_daily_v2...")
        result_rs = db.execute(text("""
            DELETE FROM rs_daily_v2 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        db.commit()
        logger.info(f"✅ تم حذف {result_rs.rowcount} صف من rs_daily_v2")
        
        # 2️⃣ حذف من stock_indicators
        logger.info("🗑️ حذف بيانات المؤشرات الفنية من stock_indicators...")
        result_tech = db.execute(text("""
            DELETE FROM stock_indicators 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        db.commit()
        logger.info(f"✅ تم حذف {result_tech.rowcount} صف من stock_indicators")
        
        # 3️⃣ حذف من industry_group_history
        logger.info("🗑️ حذف بيانات مجموعات الصناعة من industry_group_history...")
        result_ig = db.execute(text("""
            DELETE FROM industry_group_history 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        db.commit()
        logger.info(f"✅ تم حذف {result_ig.rowcount} صف من industry_group_history")
        
        # 4️⃣ حذف من market_breadth
        logger.info("🗑️ حذف بيانات اتساع السوق من market_breadth...")
        result_breadth = db.execute(text("""
            DELETE FROM market_breadth 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        db.commit()
        logger.info(f"✅ تم حذف {result_breadth.rowcount} صف من market_breadth")
        
        # 5️⃣ حذف من prices (الأسعار)
        logger.info("🗑️ حذف بيانات الأسعار من prices...")
        result_price = db.execute(text("""
            DELETE FROM prices 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        db.commit()
        logger.info(f"✅ تم حذف {result_price.rowcount} صف من prices")
        
        # 6️⃣ إعادة تعيين حالة التحديث
        logger.info("🔄 إعادة تعيين حالة التحديث...")
        db.execute(text("""
            UPDATE update_status 
            SET is_updating = FALSE, 
                latest_ready_date = CURRENT_DATE - INTERVAL '1 day'
            WHERE id = 1
        """))
        db.commit()
        logger.info("✅ تم إعادة تعيين حالة التحديث")
        
        # ملخص
        total_deleted = (result_rs.rowcount + result_tech.rowcount + 
                        result_ig.rowcount + result_breadth.rowcount + 
                        result_price.rowcount)
        
        logger.info(f"""
        
        ═══════════════════════════════════════
        ✅ تم التنظيف بنجاح!
        ═══════════════════════════════════════
        📅 التاريخ المحذوف: {TARGET_DATE}
        📊 إجمالي الصفوف المحذوفة: {total_deleted}
        
        📋 التفاصيل:
        - rs_daily_v2: {result_rs.rowcount} صف
        - stock_indicators: {result_tech.rowcount} صف
        - industry_group_history: {result_ig.rowcount} صف
        - market_breadth: {result_breadth.rowcount} صف
        - prices: {result_price.rowcount} صف
        
        ✨ جاهز لتشغيل التحديث مجدداً!
        """)

    except Exception as e:
        logger.error(f"❌ خطأ في التنظيف: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    cleanup_failed_update()
