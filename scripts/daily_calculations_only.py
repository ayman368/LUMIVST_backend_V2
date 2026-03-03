"""
Daily Market Update - CALCULATIONS ONLY (NO SCRAPING)
تشغيل جميع الحسابات بدون سحب البيانات (السكرابينج)
"""
import sys
import logging
import datetime
from pathlib import Path
from datetime import date
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.config import settings
from app.core.database import SessionLocal
from sqlalchemy import text

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_calculations_only(target_date_str=None):
    """
    تشغيل جميع الحسابات للتاريخ المحدد بدون سحب البيانات
    - RS Calculation
    - Technical Indicators
    - IBD Metrics
    - Industry Group Metrics
    - Stock Technical Indicators
    """
    db = SessionLocal()
    
    try:
        logger.info("=" * 70)
        logger.info("🚀 STARTING CALCULATIONS-ONLY MODE (NO SCRAPING)")
        logger.info("=" * 70)
        
        # تحديد التاريخ
        if target_date_str:
            market_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
            logger.info(f"📅 Target date: {market_date}")
        else:
            market_date = date.today()
            logger.info(f"📅 Using today's date: {market_date}")
        
        # التحقق من وجود البيانات الأساسية
        logger.info(f"\n🔍 VERIFYING base price data exists for {market_date}...")
        
        result = db.execute(text("""
            SELECT COUNT(*) as cnt FROM prices WHERE date = :target_date
        """), {"target_date": market_date})
        
        price_count = result.fetchone()[0]
        
        if price_count == 0:
            logger.error(f"❌ ERROR: No price data found for {market_date}!")
            logger.error("⚠️  You MUST run scraping first!")
            return False
        
        logger.info(f"✅ Found {price_count} price records for {market_date}")
        
        # PHASE 2: RS CALCULATION
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2/5: RS CALCULATION")
        logger.info("=" * 70)
        
        try:
            # Delete old RS records for this date FIRST
            logger.info(f"🧹 Deleting old RS records for {market_date}...")
            delete_rs = text("DELETE FROM rs_daily_v2 WHERE date = :target_date")
            db.execute(delete_rs, {"target_date": market_date})
            db.commit()
            logger.info(f"✅ Deleted old RS records")
            
            # Now calculate new ones
            from scripts.calculate_rs_final_precise import RSCalculatorUltraFast
            
            logger.info("🧮 Calculating RS (Vectorized)...")
            calculator = RSCalculatorUltraFast(str(settings.DATABASE_URL))
            df_all_results = calculator.calculate_full_history_optimized()
            
            if df_all_results is not None and not df_all_results.empty:
                df_all_results['date'] = pd.to_datetime(df_all_results['date']).dt.date
                df_today = df_all_results[df_all_results['date'] == market_date]
                
                if not df_today.empty:
                    logger.info(f"💾 Saving {len(df_today)} RS records...")
                    calculator.save_bulk_results(df_today)
                    logger.info(f"✅ RS Calculation Complete ({len(df_today)} records)")
                else:
                    logger.warning(f"⚠️ No RS results for {market_date}")
            else:
                logger.error("❌ RS calculation returned no results")
                
        except Exception as e:
            logger.error(f"❌ RS Calculation Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # PHASE 3: TECHNICAL INDICATORS
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3/5: TECHNICAL INDICATORS")
        logger.info("=" * 70)
        
        try:
            from scripts.calculate_technicals import TechnicalCalculator
            
            logger.info("🧹 Clearing old technical calculations for this date...")
            # Note: TechnicalCalculator saves to prices table, so records are updated
            
            logger.info("🧮 Calculating Technical Indicators (SMA, EMA, 52W, etc)...")
            tech_calc = TechnicalCalculator(str(settings.DATABASE_URL))
            df_tech = tech_calc.load_data()
            df_tech_res = tech_calc.calculate(df_tech)
            tech_calc.save_latest(df_tech_res)
            logger.info("✅ Technical Indicators Complete")
            
        except Exception as e:
            logger.error(f"❌ Technical Indicators Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # PHASE 4: IBD METRICS (RS Ratings & Acc/Dis)
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 4/5: IBD METRICS (Group RS, Acc/Dis)")
        logger.info("=" * 70)
        
        try:
            from scripts.calculate_ibd_metrics import IBDMetricsCalculator
            
            logger.info("🧹 Deleting old IBD records for this date...")
            delete_ibd = text("DELETE FROM rs_daily_v2 WHERE date = :target_date")
            db.execute(delete_ibd, {"target_date": market_date})
            db.commit()
            logger.info("✅ Deleted old IBD records")
            
            logger.info("🧮 Calculating Group RS and Acc/Dis ratings...")
            ibd_calc = IBDMetricsCalculator(db)
            df_ibd_prices = ibd_calc.load_data(lookback_days=230)
            
            if not df_ibd_prices.empty:
                group_rs_map = ibd_calc.calculate_group_rs(df_ibd_prices, market_date)
                acc_dis_map = ibd_calc.calculate_acc_dis(df_ibd_prices, market_date)
                
                if group_rs_map or acc_dis_map:
                    ibd_calc.save_results(group_rs_map, acc_dis_map, market_date)
                    logger.info(f"✅ IBD Metrics Complete")
                else:
                    logger.warning("⚠️ No IBD results generated")
            else:
                logger.warning("⚠️ No price data for IBD calculation")
                
        except Exception as e:
            logger.error(f"❌ IBD Metrics Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # PHASE 5: INDUSTRY GROUP METRICS
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 5/6: INDUSTRY GROUP METRICS")
        logger.info("=" * 70)
        
        try:
            from scripts.calculate_industry_groups import IndustryGroupCalculator
            
            logger.info("🧹 Deleting old Industry Group records for this date...")
            delete_ig = text("DELETE FROM industry_group_history WHERE date = :target_date")
            result = db.execute(delete_ig, {"target_date": market_date})
            deleted_count = result.rowcount
            db.commit()
            logger.info(f"✅ Deleted {deleted_count} old records")
            
            logger.info("🧮 Calculating Industry Group Metrics...")
            ig_calc = IndustryGroupCalculator(db)
            
            group_indices = ig_calc.calculate_group_index_prices(market_date)
            
            if group_indices:
                group_df = ig_calc.calculate_ibd_group_score(group_indices)
                
                if not group_df.empty:
                    summary_ig = ig_calc.prepare_summary_data(group_df, market_date)
                    
                    if not summary_ig.empty:
                        ig_calc.save(summary_ig, market_date)
                        logger.info(f"✅ Industry Group Metrics Complete ({len(summary_ig)} groups)")
                    else:
                        logger.warning("⚠️ No summary data generated")
                else:
                    logger.warning("⚠️ No IBD scores calculated")
            else:
                logger.warning("⚠️ No group indices found")
                
        except Exception as e:
            logger.error(f"❌ Industry Group Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # PHASE 6: STOCK TECHNICAL INDICATORS
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 6/6: STOCK TECHNICAL INDICATORS")
        logger.info("=" * 70)
        
        try:
            from scripts.calculate_stock_indicators import calculate_and_store_indicators
            
            logger.info("🧹 Deleting old Stock Indicator records for this date...")
            delete_si = text("DELETE FROM stock_indicators WHERE date = :target_date")
            db.execute(delete_si, {"target_date": market_date})
            db.commit()
            logger.info("✅ Deleted old Stock Indicator records")
            
            logger.info("🧮 Calculating and storing Stock Technical Indicators...")
            processed, errors, successful = calculate_and_store_indicators(db, market_date)
            logger.info(f"✅ Stock Indicators Complete (Processed: {processed}, Successful: {successful}, Errors: {errors})")
            
        except Exception as e:
            logger.error(f"❌ Stock Indicators Error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        logger.info("\n" + "=" * 70)
        logger.info("🎉 ALL CALCULATIONS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        logger.info(f"✅ All data for {market_date} has been:")
        logger.info(f"   - Validated")
        logger.info(f"   - Old records deleted")
        logger.info(f"   - New records calculated and saved")
        logger.info("=" * 70)
        
        return True
        
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
        description='Run Daily Calculations Only (No Scraping)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python daily_calculations_only.py                    # Use today's date
  python daily_calculations_only.py --date 2026-03-02  # Use specific date
        """
    )
    parser.add_argument('--date', type=str, help='Target date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    success = run_calculations_only(args.date)
    exit(0 if success else 1)
