import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from app.core.database import SessionLocal
from app.models.economic_indicators import SP500History

logger = logging.getLogger(__name__)

def fetch_sp500_pe():
    url = "https://www.multpl.com/s-p-500-pe-ratio/table/by-month"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html"
    }
    logger.info(f"Fetching S&P 500 PE Ratio from {url}...")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'datatable'})

        if not table:
            logger.error("Could not find datatable on multpl.com")
            return []

        records = []
        rows = table.find_all('tr')[1:]
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                date_str = cols[0].text.strip()
                raw_val = cols[1].text
                import re
                match = re.search(r'\d+\.\d+', raw_val)
                if match:
                    val_str = match.group()
                    try:
                        dt = datetime.strptime(date_str, "%b %d, %Y").date()
                        val = float(val_str)
                        records.append({"date": dt, "pe": val})
                    except ValueError as ve:
                        logger.warning(f"Could not parse row '{date_str}' / '{val_str}': {ve}")
                        continue

        logger.info(f"✅ Fetched {len(records)} PE records from Multpl")
        return records
    except Exception as e:
        logger.error(f"❌ Failed to fetch PE data: {e}")
        return []


def scrape_sp500_pe():
    db = SessionLocal()
    try:
        # Ensure pe_ratio column exists
        try:
            db.execute(text("ALTER TABLE sp500_history ADD COLUMN pe_ratio FLOAT;"))
            db.commit()
            logger.info("✅ Added pe_ratio column to sp500_history table")
        except Exception:
            db.rollback()  # Column already exists, that's fine
            
        try:
            # Fix for PostgreSQL: make close column nullable
            db.execute(text("ALTER TABLE sp500_history ALTER COLUMN close DROP NOT NULL;"))
            db.commit()
        except Exception:
            db.rollback()

        records = fetch_sp500_pe()
        if not records:
            return False

        # Build a dict: (year, month) -> pe value
        pe_dict = {(r['date'].year, r['date'].month): r for r in records}

        # Load existing SP500History records into a lookup dict
        existing = db.query(SP500History).all()
        existing_dict = {
            (sp.trade_date.year, sp.trade_date.month): sp
            for sp in existing
        }

        inserted = 0
        updated = 0

        for (year, month), r in pe_dict.items():
            if (year, month) in existing_dict:
                # UPDATE existing record
                sp = existing_dict[(year, month)]
                if sp.pe_ratio != r['pe']:
                    sp.pe_ratio = r['pe']
                    updated += 1
            else:
                # INSERT new record with just the date + PE ratio
                new_record = SP500History(
                    trade_date=r['date'],
                    pe_ratio=r['pe']
                    # add other required fields here if your model demands them
                )
                db.add(new_record)
                inserted += 1

        db.commit()
        logger.info(f"✅ Done — Inserted: {inserted} new records | Updated: {updated} existing records")
        return True

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error in scrape_sp500_pe: {e}")
        return False
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scrape_sp500_pe()