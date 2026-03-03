"""
⚡ PHASE 6 ONLY - Stock Technical Indicators
تحسب فقط المؤشرات التقنية للأسهم (بدون الـ phases اللي اتحسبت)
"""
import sys
import logging
import datetime
from pathlib import Path
from datetime import date

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from sqlalchemy import text

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def phase_6_stock_indicators_only(target_date_str=None):
    """
    تشغيل PHASE 6 فقط - Stock Technical Indicators
    """
    db = SessionLocal()
    
    try:
        logger.info("=" * 70)
        logger.info("⚡ PHASE 6 ONLY: STOCK TECHNICAL INDICATORS")
        logger.info("=" * 70)
        
        # تحديد التاريخ
        if target_date_str:
            market_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
            logger.info(f"📅 Target date: {market_date}")
        else:
            market_date = date.today()
            logger.info(f"📅 Using today's date: {market_date}")
        
        # التحقق من وجود البيانات الأساسية
        logger.info(f"\n🔍 Verifying base price data exists for {market_date}...")
        
        result = db.execute(text("""
            SELECT COUNT(*) as cnt FROM prices WHERE date = :target_date
        """), {"target_date": market_date})
        
        price_count = result.fetchone()[0]
        
        if price_count == 0:
            logger.error(f"❌ ERROR: No price data found for {market_date}!")
            return False
        
        logger.info(f"✅ Found {price_count} price records")
        
        # PHASE 6: STOCK TECHNICAL INDICATORS
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("🧮 CALCULATING STOCK TECHNICAL INDICATORS")
        logger.info("=" * 70)
        
        try:
            # Check if stock_indicators table exists first
            logger.info("🔍 Checking if stock_indicators table exists...")
            
            check_table = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'stock_indicators'
                ) as exists;
            """)
            
            result = db.execute(check_table)
            table_exists = result.fetchone()[0]
            
            if not table_exists:
                logger.error("❌ ERROR: stock_indicators table does NOT exist!")
                logger.error("⚠️  Creating table structure...")
                # The migration should create it, but let's warn
                return False
            
            logger.info("✅ stock_indicators table confirmed")
            
            # Delete old records for this date
            logger.info(f"🧹 Deleting old Stock Indicator records for {market_date}...")
            
            delete_si = text("DELETE FROM stock_indicators WHERE date = :target_date")
            result = db.execute(delete_si, {"target_date": market_date})
            deleted_count = result.rowcount
            db.commit()
            
            logger.info(f"✅ Deleted {deleted_count} old records")
            
            # Now calculate new ones
            logger.info("🧮 Calculating and storing Stock Technical Indicators...")
            
            from scripts.calculate_stock_indicators import calculate_and_store_indicators
            
            processed, errors, successful = calculate_and_store_indicators(db, market_date)
            
            logger.info(f"\n" + "=" * 70)
            logger.info(f"✅ STOCK INDICATORS CALCULATION COMPLETE!")
            logger.info(f"=" * 70)
            logger.info(f"   📊 Processed: {processed}")
            logger.info(f"   ✅ Successful: {successful}")
            logger.info(f"   ⚠️  Errors: {errors}")
            logger.info(f"=" * 70)
            
            # Verify what was saved
            result = db.execute(text("""
                SELECT COUNT(*) FROM stock_indicators WHERE date = :target_date
            """), {"target_date": market_date})
            
            final_count = result.fetchone()[0]
            logger.info(f"\n✅ Final verification: {final_count} records saved for {market_date}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Stock Indicators Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    except Exception as e:
        logger.error(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='⚡ PHASE 6 ONLY - Stock Technical Indicators',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quick Mode - Calculate Stock Technical Indicators ONLY

Examples:
  python phase6_only.py                    # Use today's date
  python phase6_only.py --date 2026-03-02  # Use specific date
        """
    )
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    success = phase_6_stock_indicators_only(args.date)
    exit(0 if success else 1)
