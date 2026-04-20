import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import time
import logging
from datetime import datetime, date
import requests
from app.core.database import SessionLocal
from app.models.economic_indicators import SP500History

logger = logging.getLogger(__name__)

YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

def fetch_sp500_yahoo(start_date: str = "1990-01-01", max_retries: int = 3):
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts   = int(datetime.now().timestamp())

    params = {
        "period1":  start_ts,
        "period2":  end_ts,
        "interval": "1d",
        "events":   "history",
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(YAHOO_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            result     = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            ohlcv      = result["indicators"]["quote"][0]

            opens   = ohlcv.get("open",   [])
            highs   = ohlcv.get("high",   [])
            lows    = ohlcv.get("low",    [])
            closes  = ohlcv.get("close",  [])
            volumes = ohlcv.get("volume", [])

            records = []
            for i, ts in enumerate(timestamps):
                close_val = closes[i] if i < len(closes) else None
                if close_val is None:
                    continue

                records.append({
                    "trade_date": date.fromtimestamp(ts),
                    "open":       opens[i]   if i < len(opens)   else None,
                    "high":       highs[i]   if i < len(highs)   else None,
                    "low":        lows[i]    if i < len(lows)    else None,
                    "close":      round(close_val, 2),
                    "volume":     volumes[i] if i < len(volumes) else None,
                })

            logger.info(f"   ✅ Fetched {len(records):,} trading days from Yahoo Finance")
            return records

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                wait = attempt * 5
                logger.warning(f"   Attempt {attempt} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"   ❌ Failed after {max_retries} attempts: {e}")
                return []

        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"   ❌ Failed to parse Yahoo Finance response: {e}")
            return []

def scrape_sp500():
    logger.info("Fetching S&P 500 history from Yahoo Finance...")

    db = SessionLocal()
    try:
        # ✅ Incremental: find the latest date already in DB
        from sqlalchemy import func
        from datetime import timedelta
        latest_row = db.query(func.max(SP500History.trade_date)).scalar()

        if latest_row:
            # Fetch from a few days before latest to catch any gaps
            start_date = (latest_row - timedelta(days=3)).strftime("%Y-%m-%d")
            logger.info(f"   📅 Latest in DB: {latest_row}. Fetching from {start_date} onwards...")
        else:
            start_date = "1990-01-01"  # First run
            logger.info("   📅 First run — fetching all historical data from 1990...")

        records = fetch_sp500_yahoo(start_date=start_date)

        if not records:
            logger.warning("⚠️ No data fetched. Aborting.")
            return False

        # ✅ Get existing dates to avoid duplicates
        existing_dates = {
            row[0] for row in db.query(SP500History.trade_date).all()
        }

        new_objs = [
            SP500History(
                trade_date=r["trade_date"],
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                volume=r["volume"],
            )
            for r in records
            if r["trade_date"] not in existing_dates
        ]

        if new_objs:
            db.bulk_save_objects(new_objs)
            db.commit()
            logger.info(f"✅ Inserted {len(new_objs):,} NEW S&P 500 records")
        else:
            logger.info("ℹ️ Already up to date — no new records")

        return True

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error saving S&P 500 data: {e}")
        return False
    finally:
        db.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    scrape_sp500()
