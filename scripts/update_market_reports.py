import gc
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.models.market_reports import (
    SubstantialShareholder,
    NetShortPosition,
    ForeignHeadroom,
    ShareBuyback,
    SBLPosition,
)
from app.services.daily_detailed_scraper import build_driver
from app.services.market_reports_scraper import (
    scrape_substantial_shareholders,
    scrape_net_short_positions,
    scrape_foreign_headroom,
    scrape_share_buybacks,
    scrape_sbl_positions,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _create_driver():
    """Create a memory-optimized ChromeDriver instance."""
    driver = build_driver(headless=True)
    # Set timeouts to prevent Chrome from hanging indefinitely
    driver.set_page_load_timeout(60)        # 60s max per page load
    driver.set_script_timeout(30)           # 30s max for scripts
    driver.implicitly_wait(10)              # 10s implicit wait
    return driver


def _run_scraper_with_own_driver(scrape_fn, save_fn, name):
    """
    Run a single scraper task with its own ChromeDriver.

    Each scraper gets a fresh driver that is quit immediately after,
    preventing Chrome from accumulating memory over 17+ minutes.
    """
    driver = None
    try:
        logger.info(f"Initializing ChromeDriver for {name}...")
        driver = _create_driver()
        logger.info(f"Scraping {name}...")

        data = scrape_fn(driver)
        logger.info(f"Scraped {len(data)} {name} records. Closing ChromeDriver...")

        # Quit driver BEFORE saving to DB (frees ~300MB)
        driver.quit()
        driver = None
        gc.collect()

        # Now save to DB without Chrome in memory
        save_fn(data)

        # Free the data list
        del data
        gc.collect()

    except Exception as e:
        logger.error(f"Error during {name}: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            driver = None
            gc.collect()


def save_substantial_shareholders(data):
    count = 0
    with SessionLocal() as db:
        for item in data:
            existing = db.query(SubstantialShareholder).filter(
                SubstantialShareholder.report_date == item['report_date'],
                SubstantialShareholder.company_name == item['company_name'],
                SubstantialShareholder.shareholder_name == item['shareholder_name']
            ).first()

            if not existing:
                obj = SubstantialShareholder(**item)
                db.add(obj)
                count += 1
        db.commit()
    logger.info(f"Added {count} new Substantial Shareholders records.")


def save_net_short_positions(data):
    count = 0
    with SessionLocal() as db:
        for item in data:
            existing = db.query(NetShortPosition).filter(
                NetShortPosition.report_date == item['report_date'],
                NetShortPosition.symbol == item['symbol']
            ).first()

            if not existing:
                obj = NetShortPosition(**item)
                db.add(obj)
                count += 1
        db.commit()
    logger.info(f"Added {count} new Net Short Positions records.")


def save_foreign_headroom(data):
    count = 0
    with SessionLocal() as db:
        for item in data:
            existing = db.query(ForeignHeadroom).filter(
                ForeignHeadroom.report_date == item['report_date'],
                ForeignHeadroom.symbol == item['symbol']
            ).first()

            if not existing:
                obj = ForeignHeadroom(**item)
                db.add(obj)
                count += 1
        db.commit()
    logger.info(f"Added {count} new Foreign Headroom records.")


def save_share_buybacks(data):
    count = 0
    with SessionLocal() as db:
        for item in data:
            existing = db.query(ShareBuyback).filter(
                ShareBuyback.report_date == item['report_date'],
                ShareBuyback.symbol == item['symbol']
            ).first()

            if not existing:
                obj = ShareBuyback(**item)
                db.add(obj)
                count += 1
        db.commit()
    logger.info(f"Added {count} new Share Buybacks records.")


def save_sbl_positions(data):
    count = 0
    with SessionLocal() as db:
        for item in data:
            existing = db.query(SBLPosition).filter(
                SBLPosition.report_date == item['report_date'],
                SBLPosition.symbol == item['symbol']
            ).first()

            if not existing:
                obj = SBLPosition(**item)
                db.add(obj)
                count += 1
        db.commit()
    logger.info(f"Added {count} new SBL Positions records.")


def main():
    logger.info("=" * 50)
    logger.info("Starting Market Reports Update (memory-safe mode)")
    logger.info("Each scraper will use its own short-lived ChromeDriver.")
    logger.info("=" * 50)

    tasks = [
        (scrape_substantial_shareholders, save_substantial_shareholders, "Substantial Shareholders"),
        (scrape_net_short_positions,      save_net_short_positions,      "Net Short Positions"),
        (scrape_foreign_headroom,         save_foreign_headroom,         "Foreign Headroom"),
        (scrape_share_buybacks,           save_share_buybacks,           "Share Buybacks"),
        (scrape_sbl_positions,            save_sbl_positions,            "SBL Positions"),
    ]

    for scrape_fn, save_fn, name in tasks:
        _run_scraper_with_own_driver(scrape_fn, save_fn, name)

    logger.info("=" * 50)
    logger.info("Market Reports Update finished.")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
