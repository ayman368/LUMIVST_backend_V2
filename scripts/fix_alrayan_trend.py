import asyncio
import sys
import os

# Add parent directory to path so 'app' module can be found
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import SessionLocal

def fix_trend_signal():
    db = SessionLocal()
    try:
        print("Fixing trend_signal and weekly trend conditions in stock_indicators...")
        
        # SQL to update the weekly trend conditions that were missing historically
        update_weekly_sql = """
        UPDATE stock_indicators
        SET 
            price_gt_sma9_weekly = (close_w > sma9_w),
            sma_trend_weekly = (sma4_w > sma9_w AND sma9_w > sma18_w),
            cci_ema20_gt_0_weekly = (cci_ema20_w > 0)
        WHERE sma4_w IS NOT NULL AND sma9_w IS NOT NULL AND sma18_w IS NOT NULL;
        """
        db.execute(text(update_weekly_sql))
        
        # SQL to update trend_signal based on all conditions
        update_signal_sql = """
        UPDATE stock_indicators
        SET trend_signal = (
            price_gt_sma18 = True AND
            price_gt_sma9_weekly = True AND
            sma_trend_daily = True AND
            sma_trend_weekly = True AND
            cci_gt_100 = True AND
            cci_ema20_gt_0_daily = True AND
            cci_ema20_gt_0_weekly = True AND
            aroon_up_gt_70 = True AND
            aroon_down_lt_30 = True AND
            is_etf_or_index = False AND
            has_gap = False
        )
        """
        db.execute(text(update_signal_sql))
        db.commit()
        print("Successfully updated trend_signal historically!")
        
        # Also flush the cache
        from app.core.redis import redis_cache
        asyncio.run(redis_cache.flush_all())
        print("Flushed Redis cache!")
        
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    fix_trend_signal()
