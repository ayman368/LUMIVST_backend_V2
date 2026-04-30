"""
🔍 سكريبت التحقق من البيانات
عرض عدد السجلات لكل تاريخ في قاعدة البيانات
"""

import sys
from pathlib import Path
import logging
from datetime import date
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# إضافة المسار
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal

# إعداد Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TARGET_DATE = date(2026, 4, 22)

def check_data():
    """
    التحقق من عدد السجلات في كل جدول لتاريخ 22-04-2026
    """
    db = SessionLocal()
    
    try:
        logger.info(f"\n📋 التحقق من البيانات المتعلقة بتاريخ {TARGET_DATE}...\n")
        
        # 1️⃣ فحص rs_daily_v2
        logger.info("🔍 جدول rs_daily_v2...")
        result = db.execute(text("""
            SELECT COUNT(*) as count FROM rs_daily_v2 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        count_rs = result.scalar()
        logger.info(f"   ├─ السجلات: {count_rs}\n")
        
        # 2️⃣ فحص stock_indicators
        logger.info("🔍 جدول stock_indicators...")
        result = db.execute(text("""
            SELECT COUNT(*) as count FROM stock_indicators 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        count_tech = result.scalar()
        logger.info(f"   ├─ السجلات: {count_tech}\n")
        
        # 3️⃣ فحص industry_group_history
        logger.info("🔍 جدول industry_group_history...")
        result = db.execute(text("""
            SELECT COUNT(*) as count FROM industry_group_history 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        count_ig = result.scalar()
        logger.info(f"   ├─ السجلات: {count_ig}\n")
        
        # 4️⃣ فحص market_breadth
        logger.info("🔍 جدول market_breadth...")
        result = db.execute(text("""
            SELECT COUNT(*) as count FROM market_breadth 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        count_breadth = result.scalar()
        logger.info(f"   ├─ السجلات: {count_breadth}\n")
        
        # 5️⃣ فحص prices
        logger.info("🔍 جدول prices...")
        result = db.execute(text("""
            SELECT COUNT(*) as count FROM prices 
            WHERE date = :target_date
        """), {"target_date": TARGET_DATE})
        count_price = result.scalar()
        logger.info(f"   ├─ السجلات: {count_price}\n")
        
        # ملخص
        total = count_rs + count_tech + count_ig + count_breadth + count_price
        
        logger.info(f"""
        ═══════════════════════════════════════
        📊 ملخص البيانات
        ═══════════════════════════════════════
        📅 التاريخ: {TARGET_DATE}
        📈 إجمالي السجلات: {total}
        
        📋 التفاصيل:
        ├─ rs_daily_v2: {count_rs}
        ├─ stock_indicators: {count_tech}
        ├─ industry_group_history: {count_ig}
        ├─ market_breadth: {count_breadth}
        └─ prices: {count_price}
        """)
        
        if total == 0:
            logger.warning("⚠️  لا توجد بيانات لحذفها في تاريخ 22-04-2026")
        else:
            logger.info(f"✅ جاهز للحذف! سيتم حذف {total} سجل")

    except Exception as e:
        logger.error(f"❌ خطأ في الفحص: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    check_data()
