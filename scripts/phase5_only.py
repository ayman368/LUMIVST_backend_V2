"""
⚡ PHASE 5 ONLY - Industry Group Metrics
تحسب فقط مؤشرات المجموعات الصناعية (بدون الـ phases اللي اتحسبت)
"""
import sys
import logging
import datetime
from pathlib import Path
from datetime import date
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def phase_5_industry_group_only(target_date_str=None):
    """
    تشغيل PHASE 5 فقط - Industry Group Metrics
    """
    db = SessionLocal()
    
    try:
        logger.info("=" * 70)
        logger.info("⚡ PHASE 5 ONLY: INDUSTRY GROUP METRICS")
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
        
        # PHASE 5: INDUSTRY GROUP METRICS
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("🧮 CALCULATING INDUSTRY GROUP METRICS")
        logger.info("=" * 70)
        
        try:
            # Check if industry_group_history table exists
            logger.info("🔍 Checking if industry_group_history table exists...")
            
            check_table = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'industry_group_history'
                ) as exists;
            """)
            
            result = db.execute(check_table)
            table_exists = result.fetchone()[0]
            
            if not table_exists:
                logger.error("❌ ERROR: industry_group_history table does NOT exist!")
                return False
            
            logger.info("✅ industry_group_history table confirmed")
            
            # Delete old records for this date
            logger.info(f"🧹 Deleting old Industry Group records for {market_date}...")
            
            delete_ig = text("DELETE FROM industry_group_history WHERE date = :target_date")
            result = db.execute(delete_ig, {"target_date": market_date})
            deleted_count = result.rowcount
            db.commit()
            
            logger.info(f"✅ Deleted {deleted_count} old records")
            
            # Now calculate new ones
            logger.info("🧮 Calculating Industry Group Metrics...")
            
            from scripts.calculate_industry_groups import IndustryGroupCalculator
            
            ig_calc = IndustryGroupCalculator(db)
            
            # Step 1: Calculate group index prices
            logger.info("📈 Step 1: Calculating group index prices...")
            group_indices = ig_calc.calculate_group_index_prices(market_date)
            
            if not group_indices:
                logger.warning("⚠️ No group indices found")
                return False
            
            logger.info(f"✅ Calculated {len(group_indices)} group indices")
            
            # Step 2: Calculate IBD scores
            logger.info("📊 Step 2: Calculating IBD scores...")
            group_df = ig_calc.calculate_ibd_group_score(group_indices)
            
            if group_df.empty:
                logger.warning("⚠️ No IBD scores calculated")
                return False
            
            logger.info(f"✅ Calculated IBD scores for {len(group_df)} groups")
            
            # Step 3: Prepare summary data
            logger.info("📋 Step 3: Preparing summary data...")
            summary_ig = ig_calc.prepare_summary_data(group_df, market_date)
            
            if summary_ig.empty:
                logger.warning("⚠️ No summary data generated")
                return False
            
            logger.info(f"✅ Prepared summary data for {len(summary_ig)} groups")
            
            # Step 4: Save to database
            logger.info("💾 Step 4: Saving to database...")
            ig_calc.save(summary_ig, market_date)
            
            logger.info(f"\n" + "=" * 70)
            logger.info(f"✅ INDUSTRY GROUP METRICS CALCULATION COMPLETE!")
            logger.info(f"=" * 70)
            logger.info(f"   📊 Groups Processed: {len(summary_ig)}")
            logger.info(f"   📅 Date: {market_date}")
            logger.info(f"=" * 70)
            
            # Verify what was saved
            result = db.execute(text("""
                SELECT COUNT(*) FROM industry_group_history WHERE date = :target_date
            """), {"target_date": market_date})
            
            final_count = result.fetchone()[0]
            logger.info(f"\n✅ Final verification: {final_count} records saved for {market_date}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Industry Group Error: {e}")
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
        description='⚡ PHASE 5 ONLY - Industry Group Metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quick Mode - Calculate Industry Group Metrics ONLY

Examples:
  python phase5_only.py                    # Use today's date
  python phase5_only.py --date 2026-03-02  # Use specific date
        """
    )
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    success = phase_5_industry_group_only(args.date)
    exit(0 if success else 1)
