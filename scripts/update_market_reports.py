import asyncio
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

def update_substantial_shareholders(db: Session, driver):
    logger.info("Scraping Substantial Shareholders...")
    data = scrape_substantial_shareholders(driver)
    count = 0
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

def update_net_short_positions(db: Session, driver):
    logger.info("Scraping Net Short Positions...")
    data = scrape_net_short_positions(driver)
    count = 0
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

def update_foreign_headroom(db: Session, driver):
    logger.info("Scraping Foreign Headroom...")
    data = scrape_foreign_headroom(driver)
    count = 0
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

def update_share_buybacks(db: Session, driver):
    logger.info("Scraping Share Buybacks...")
    data = scrape_share_buybacks(driver)
    count = 0
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

def update_sbl_positions(db: Session, driver):
    logger.info("Scraping SBL Positions...")
    data = scrape_sbl_positions(driver)
    count = 0
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
    db = SessionLocal()
    
    # Initialize the WebDriver once and share it!
    # This prevents the Windows ChromeDriver crashing issue (unable to connect to renderer).
    logger.info("Initializing ChromeDriver...")
    driver = build_driver(headless=True)
    
    try:
        update_substantial_shareholders(db, driver)
        update_net_short_positions(db, driver)
        update_foreign_headroom(db, driver)
        update_share_buybacks(db, driver)
        update_sbl_positions(db, driver)
    except Exception as e:
        logger.error(f"Error during update: {e}")
    finally:
        driver.quit()
        db.close()

if __name__ == "__main__":
    main()
