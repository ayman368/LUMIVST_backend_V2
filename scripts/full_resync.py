"""
full_resync.py
==============
Master script that:
  1. Clears ALL data from historical_reports & market_pulse tables
  2. Re-scrapes historical_reports from Saudi Exchange (Reports.py scraper)
  3. Recalculates market_pulse from the fresh data (recalculate_market_pulse.py)

Run from backend/:
    ..\venv\Scripts\python.exe scripts\full_resync.py
"""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.database import SessionLocal
from app.models.market_reports import HistoricalReport
from app.models.market_pulse import MarketPulse


def step1_clear_tables():
    """Delete all data from historical_reports and market_pulse."""
    print("=" * 60)
    print("STEP 1: Clearing old data from database")
    print("=" * 60)

    db = SessionLocal()
    try:
        mp_count = db.query(MarketPulse).count()
        hr_count = db.query(HistoricalReport).count()
        print(f"  Current market_pulse rows: {mp_count}")
        print(f"  Current historical_reports rows: {hr_count}")

        print("\n  🗑️  Deleting all market_pulse rows...")
        db.query(MarketPulse).delete()
        db.commit()
        print("  ✅ market_pulse cleared.")

        print("  🗑️  Deleting all historical_reports rows...")
        db.query(HistoricalReport).delete()
        db.commit()
        print("  ✅ historical_reports cleared.")

        # Verify
        assert db.query(MarketPulse).count() == 0, "market_pulse not empty!"
        assert db.query(HistoricalReport).count() == 0, "historical_reports not empty!"
        print("\n  ✅ Both tables are now empty.\n")

    except Exception as e:
        db.rollback()
        print(f"  ❌ Error clearing tables: {e}")
        raise
    finally:
        db.close()


def step2_scrape_reports():
    """Run the Reports.py scraper to re-scrape all historical data."""
    print("=" * 60)
    print("STEP 2: Re-scraping historical reports from Saudi Exchange")
    print("=" * 60)

    # Import and run the scraper's main function
    from scrapers.Reports import main as scraper_main
    scraper_main()

    # Verify results
    db = SessionLocal()
    try:
        count = db.query(HistoricalReport).count()
        print(f"\n  📊 After scraping: {count} rows in historical_reports")
        if count == 0:
            print("  ⚠️  WARNING: No data was scraped! Check scraper logs above.")
            return False
        return True
    finally:
        db.close()


def step3_recalculate_market_pulse():
    """Recalculate market_pulse from the fresh historical data."""
    print("\n" + "=" * 60)
    print("STEP 3: Recalculating market_pulse from fresh data")
    print("=" * 60)

    # Import and run the recalculate script
    from scripts.recalculate_market_pulse import main as recalc_main
    recalc_main()

    # Verify results
    db = SessionLocal()
    try:
        hr_count = db.query(HistoricalReport).count()
        mp_count = db.query(MarketPulse).count()
        print(f"\n  📊 Final counts:")
        print(f"     historical_reports: {hr_count}")
        print(f"     market_pulse: {mp_count}")
        return True
    finally:
        db.close()


def main():
    start_time = time.time()
    print(f"\n🚀 FULL RESYNC started at {datetime.now()}")
    print(f"   This will: clear DB → scrape fresh data → recalculate signals\n")

    # Step 1: Clear old data
    step1_clear_tables()

    # Step 2: Re-scrape from Saudi Exchange
    scraped_ok = step2_scrape_reports()
    if not scraped_ok:
        print("\n❌ Scraping failed. Aborting market pulse recalculation.")
        return

    # Step 3: Recalculate market pulse
    step3_recalculate_market_pulse()

    elapsed = time.time() - start_time
    print(f"\n🎉 FULL RESYNC completed in {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()
