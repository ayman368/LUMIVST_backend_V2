import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import SessionLocal


def fix_trend_signal_fast():
    db = SessionLocal()
    try:
        # Step 1: Update weekly conditions in batches
        print("=" * 60)
        print("Step 1/3: Updating weekly trend conditions...")
        print("=" * 60)

        batch_size = 50000
        offset = 0
        total_updated = 0
        start = time.time()

        while True:
            result = db.execute(text("""
                UPDATE stock_indicators
                SET 
                    price_gt_sma9_weekly = (close_w > sma9_w),
                    sma_trend_weekly = (sma4_w > sma9_w AND sma9_w > sma18_w),
                    cci_ema20_gt_0_weekly = (cci_ema20_w > 0)
                WHERE id IN (
                    SELECT id FROM stock_indicators
                    WHERE sma4_w IS NOT NULL AND sma9_w IS NOT NULL AND sma18_w IS NOT NULL
                    ORDER BY id
                    LIMIT :batch_size OFFSET :offset
                )
            """), {"batch_size": batch_size, "offset": offset})
            
            rows = result.rowcount
            total_updated += rows
            elapsed = time.time() - start
            print(f"  ✅ Batch {offset // batch_size + 1}: updated {rows} rows (total: {total_updated}, {elapsed:.1f}s)")
            
            if rows < batch_size:
                break
            offset += batch_size
        
        db.commit()
        print(f"  ✅ Step 1 done: {total_updated} rows in {time.time() - start:.1f}s\n")

        # Step 2: Update trend_signal in batches
        print("=" * 60)
        print("Step 2/3: Updating trend_signal...")
        print("=" * 60)

        offset = 0
        total_updated = 0
        start = time.time()

        while True:
            result = db.execute(text("""
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
                WHERE id IN (
                    SELECT id FROM stock_indicators
                    ORDER BY id
                    LIMIT :batch_size OFFSET :offset
                )
            """), {"batch_size": batch_size, "offset": offset})
            
            rows = result.rowcount
            total_updated += rows
            elapsed = time.time() - start
            print(f"  ✅ Batch {offset // batch_size + 1}: updated {rows} rows (total: {total_updated}, {elapsed:.1f}s)")
            
            if rows < batch_size:
                break
            offset += batch_size

        db.commit()
        print(f"  ✅ Step 2 done: {total_updated} rows in {time.time() - start:.1f}s\n")

        # Step 3: Quick verification
        print("=" * 60)
        print("Step 3/3: Verification...")
        print("=" * 60)
        
        count = db.execute(text("SELECT COUNT(*) FROM stock_indicators WHERE trend_signal = True")).scalar()
        print(f"  📊 Total rows with trend_signal = True: {count}")
        
        print("\n🎉 Done! Now run: ..\\venv\\Scripts\\python.exe scripts\\flush_cache.py")

    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    fix_trend_signal_fast()
