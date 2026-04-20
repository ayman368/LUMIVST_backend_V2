import requests
import pandas as pd
import io
from datetime import datetime
from app.core.database import SessionLocal
from app.models.economic_indicators import EconomicIndicator
import logging
import time

logger = logging.getLogger(__name__)

# Using the CSV endpoint which returns the full historical data without 1000-row limits
FRED_CSV_CONFIG = {
    "UNRATE": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE",
    "PAYEMS": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PAYEMS",
    "IC4WSA": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=IC4WSA",
    "BAMLC0A3CA": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A3CA",
    "BAMLC0A4CBBB": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A4CBBB",
    "BAMLC0A3CAEY": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A3CAEY"
}

def parse_fred_csv(text: str):
    """
    Parser مرن وبسيط بيقرأ ملف الـ CSV الكامل من FRED بيضمن إننا منخسرش ولا سنة تاريخية.
    """
    import csv
    from datetime import datetime
    data = []
    
    # Read CSV lines
    lines = text.strip().splitlines()
    if not lines:
        return []
        
    reader = csv.reader(lines)
    header = next(reader, None) # Skip Header
    
    for row in reader:
        if len(row) < 2:
            continue
            
        date_str, val_str = row[0].strip(), row[1].strip()
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            if val_str not in ('.', '', 'N/A'):
                val = float(val_str)
                data.append({'date': dt, 'value': val})
        except ValueError:
            continue

    return data

def scrape_fred_indicator(indicator_code: str):
    """Scrapes historical data from FRED for a given indicator using the CSV endpoint."""
    indicator_code = indicator_code.upper()
    if indicator_code not in FRED_CSV_CONFIG:
        logger.error(f"Unknown indicator code: {indicator_code}")
        return False
        
    url = FRED_CSV_CONFIG[indicator_code]
    logger.info(f"Fetching {indicator_code} from {url}")
    
    # Using more robust browser headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    
    session = requests.Session()
    
    try:
        # Add a timeout and retry logic
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        response = session.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
        
        # Parse the CSV text
        parsed_data = parse_fred_csv(response.text)
        
        logger.info(f"Parsed {len(parsed_data)} records for {indicator_code} from FRED CSV")
        
        if not parsed_data:
            logger.warning(f"No valid data parsed for {indicator_code}. First 300 chars:\n{response.text[:300]}")
            return False
            
        db = SessionLocal()
        try:
            # نستخرج كل التواريخ المسجلة مسبقاً لهذا المؤشر بجملة استعلام واحدة بدلاً من مئات الاستعلامات
            existing_dates = {
                row[0] for row in db.query(EconomicIndicator.report_date)
                                      .filter(EconomicIndicator.indicator_code == indicator_code)
                                      .all()
            }
            
            new_objs = []
            for item in parsed_data:
                dt = item['date']
                val = item['value']
                
                if dt not in existing_dates:
                    new_objs.append(
                        EconomicIndicator(
                            report_date=dt,
                            indicator_code=indicator_code,
                            value=val
                        )
                    )
            
            if new_objs:
                db.bulk_save_objects(new_objs)
                db.commit()
                
            logger.info(f"✅ Inserted {len(new_objs)} new records for {indicator_code}")
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving {indicator_code} to DB: {e}")
            return False
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error fetching data from FRED for {indicator_code}: {e}")
        return False

def scrape_all_fred():
    """Scrape all configured FRED indicators."""
    results = {}
    for code in FRED_CSV_CONFIG.keys():
        results[code] = scrape_fred_indicator(code)
        # Sleep slightly between requests to not overwhelm FRED
        time.sleep(2)
        
    return results

if __name__ == "__main__":
    scrape_all_fred()
