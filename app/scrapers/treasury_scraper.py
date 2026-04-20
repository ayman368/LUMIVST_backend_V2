import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import requests
import time
from datetime import datetime
import logging
from app.core.database import SessionLocal
from app.models.economic_indicators import TreasuryYieldCurve

logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY", "ec4abb3dad9a16eb00d3dc2640b43b3c")
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"

# ✅ حذفنا DGS2MO لأنها 404
FRED_SERIES = {
    "1 Mo":  "DGS1MO",
    "3 Mo":  "DGS3MO",
    "6 Mo":  "DGS6MO",
    "1 Yr":  "DGS1",
    "2 Yr":  "DGS2",
    "3 Yr":  "DGS3",
    "5 Yr":  "DGS5",
    "7 Yr":  "DGS7",
    "10 Yr": "DGS10",
    "20 Yr": "DGS20",
    "30 Yr": "DGS30",
}


def fetch_fred_series(series_id: str, observation_start: str = None, max_retries: int = 3):
    params = {
        "series_id":  series_id,
        "api_key":    FRED_API_KEY,
        "file_type":  "json",
        "sort_order": "asc",
    }
    if observation_start:
        params["observation_start"] = observation_start

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(FRED_API_URL, params=params, timeout=30)
            resp.raise_for_status()

            results = []
            for obs in resp.json().get("observations", []):
                val_str = obs.get("value", "")
                if val_str in (".", "", "N/A", "ND"):
                    continue
                try:
                    results.append({
                        "date":  datetime.strptime(obs["date"], "%Y-%m-%d").date(),
                        "value": float(val_str),
                    })
                except (ValueError, KeyError):
                    continue
            return results

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.warning(f"      {series_id} not found (404). Skipping.")
                return []
            if attempt < max_retries:
                time.sleep(attempt * 5)
            else:
                raise

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt < max_retries:
                logger.warning(f"      Attempt {attempt} failed: {e}. Retrying...")
                time.sleep(attempt * 5)
            else:
                logger.error(f"      Failed after {max_retries} attempts: {e}")
                return []

    return []


def scrape_treasury_yield_curve():
    logger.info("Fetching US Treasury Yield Curve from FRED API...")

    if FRED_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("❌ FRED_API_KEY not set!")
        return False

    db = SessionLocal()
    try:
        # ✅ Incremental: find the latest date already in DB
        from sqlalchemy import func
        latest_row = db.query(func.max(TreasuryYieldCurve.report_date)).scalar()
        
        if latest_row:
            # Fetch only from the day after the latest existing date
            from datetime import timedelta
            start_date = (latest_row + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"   📅 Latest in DB: {latest_row}. Fetching from {start_date} onwards...")
        else:
            start_date = None  # First run — fetch everything
            logger.info("   📅 First run — fetching all historical data...")

        data_dict = {}

        for name, series_id in FRED_SERIES.items():
            logger.info(f"   → Fetching {name} ({series_id})")
            try:
                parsed_data = fetch_fred_series(series_id, observation_start=start_date)
                logger.info(f"   ✅ {name}: {len(parsed_data)} records")

                for item in parsed_data:
                    dt = item["date"]
                    if dt not in data_dict:
                        data_dict[dt] = {}
                    data_dict[dt][name] = item["value"]

            except Exception as e:
                logger.warning(f"   ⚠️ Skipping {name}: {e}")
                continue

            time.sleep(0.5)  # FRED rate limit: 120 req/min

        if not data_dict:
            logger.info("ℹ️ No new data to insert. Already up to date!")
            return True

        # ✅ Get existing dates to avoid duplicates
        existing_dates = {
            row[0] for row in db.query(TreasuryYieldCurve.report_date).all()
        }

        new_objs = [
            TreasuryYieldCurve(
                report_date=dt,
                month_1=values.get("1 Mo"),
                month_2=None,          # DGS2MO غير موجود على FRED
                month_3=values.get("3 Mo"),
                month_4=None,
                month_6=values.get("6 Mo"),
                year_1=values.get("1 Yr"),
                year_2=values.get("2 Yr"),
                year_3=values.get("3 Yr"),
                year_5=values.get("5 Yr"),
                year_7=values.get("7 Yr"),
                year_10=values.get("10 Yr"),
                year_20=values.get("20 Yr"),
                year_30=values.get("30 Yr"),
            )
            for dt, values in sorted(data_dict.items())
            if dt not in existing_dates
        ]

        if new_objs:
            db.bulk_save_objects(new_objs)
            db.commit()
            logger.info(f"✅ Inserted {len(new_objs):,} NEW days of Yield Curve data")
        else:
            logger.info("ℹ️ No new dates to insert")

        return True

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error: {e}")
        return False
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scrape_treasury_yield_curve()