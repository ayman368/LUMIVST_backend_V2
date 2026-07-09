"""
FRED CSV Scraper
Fetches full historical data from FRED using the CSV endpoint (no 1000-row API limit).
"""

import requests
import csv
import time
import logging
from datetime import datetime
from app.core.database import SessionLocal
from app.models.economic_indicators import EconomicIndicator

logger = logging.getLogger(__name__)

# All FRED CSV endpoints — add new series here only
FRED_CSV_CONFIG = {
    # ── Labor market ─────────────────────────────────────────────────────────
    "UNRATE":       "https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE",
    "PAYEMS":       "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PAYEMS",
    "IC4WSA":       "https://fred.stlouisfed.org/graph/fredgraph.csv?id=IC4WSA",

    # ── Corporate bond spreads (OAS) ─────────────────────────────────────────
    "BAMLC0A3CA":   "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A3CA",    # A-rated OAS
    "BAMLC0A4CBBB": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A4CBBB", # BBB-rated OAS
    "BAMLC0A3CAEY": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A3CAEY", # A-rated EY
    "BAMLC0A4CBBBEY": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A4CBBBEY", # BBB-rated EY

    # ── High-yield bond spreads ───────────────────────────────────────────────
    # BB-rated: ICE BofA BB US High Yield Index Option-Adjusted Spread
    "BAMLH0A1HYBBEY": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A1HYBBEY",

    # B-rated: ICE BofA B US High Yield Index Option-Adjusted Spread
    "BAMLH0A2HYBEY":  "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A2HYBEY",

    # Fed Interest Rate
    "FEDFUNDS": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS",

    # ── Macro / Monetary / Balance Sheet ──────────────────────────────────────
    "TLAACBW027SBOG": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=TLAACBW027SBOG", # Banks Balance Sheet
    "WALCL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WALCL", # Fed Balance Sheet
    "TREAST": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=TREAST", # Foreign Exchange Reserves
    "CPIAUCSL_PC1": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL&units=pc1", # Inflation Rate YoY
    "TOTLL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=TOTLL", # Loans to Private Sector
    "BOGMBASE": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BOGMBASE", # Money Supply M0
    "M1SL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=M1SL", # Money Supply M1
    "M2SL": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=M2SL", # Money Supply M2
}


def parse_fred_csv(text: str) -> list[dict]:
    """
    Parse a FRED CSV response into a list of {date, value} dicts.
    Skips missing values (FRED uses '.' as a placeholder).
    """
    data = []
    lines = text.strip().splitlines()
    if not lines:
        return data

    reader = csv.reader(lines)
    next(reader, None)  # skip header row

    for row in reader:
        if len(row) < 2:
            continue
        date_str, val_str = row[0].strip(), row[1].strip()
        if val_str in (".", "", "N/A"):
            continue
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            val = float(val_str)
            data.append({"date": dt, "value": val})
        except ValueError:
            continue

    return data


def scrape_fred_indicator(indicator_code: str) -> bool:
    """
    Fetch and store full historical data from FRED for one indicator.
    Uses bulk insert with duplicate detection to avoid re-inserting existing dates.
    """
    indicator_code = indicator_code.upper()
    if indicator_code not in FRED_CSV_CONFIG:
        logger.error(f"Unknown indicator code: {indicator_code}")
        return False

    url = FRED_CSV_CONFIG[indicator_code]
    logger.info(f"Fetching {indicator_code} from FRED CSV...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    session = requests.Session()
    try:
        response = session.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        parsed_data = parse_fred_csv(response.text)
        logger.info(f"Parsed {len(parsed_data)} records for {indicator_code}")

        if not parsed_data:
            logger.warning(f"No data for {indicator_code}. First 200 chars: {response.text[:200]}")
            return False

        db = SessionLocal()
        try:
            # Load existing dates in a single query — avoids N individual lookups
            existing_dates = {
                row[0]
                for row in db.query(EconomicIndicator.report_date)
                              .filter(EconomicIndicator.indicator_code == indicator_code)
                              .all()
            }

            new_objects = [
                EconomicIndicator(
                    report_date=item["date"],
                    indicator_code=indicator_code,
                    value=item["value"],
                )
                for item in parsed_data
                if item["date"] not in existing_dates
            ]

            if new_objects:
                db.bulk_save_objects(new_objects)
                db.commit()

            logger.info(f"✅ {indicator_code}: inserted {len(new_objects)} new records")
            return True

        except Exception as e:
            db.rollback()
            logger.error(f"DB error saving {indicator_code}: {e}")
            return False
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Fetch error for {indicator_code}: {e}")
        return False


def scrape_all_fred() -> dict[str, bool]:
    """Run all configured FRED indicators and return a result map."""
    results = {}
    for code in FRED_CSV_CONFIG:
        results[code] = scrape_fred_indicator(code)
        time.sleep(2)  # be polite to FRED servers
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = scrape_all_fred()
    for code, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {code}")
