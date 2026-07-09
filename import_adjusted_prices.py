import sys
from pathlib import Path
import csv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.append(str(Path(__file__).resolve().parent))
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def run():
    csv_file = Path(r"d:\Work\LUMIVST\Equites_Historical_Adjusted_Prices_Report(5).csv")
    if not csv_file.exists():
        logger.error(f"File not found: {csv_file}")
        return

    engine = create_engine(str(settings.DATABASE_URL))
    Session = sessionmaker(bind=engine)
    db = Session()

    target_symbols = {'1120', '1150'}
    
    logger.info("Reading CSV and updating DB for symbols 1120 and 1150...")
    
    updates = 0
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row.get('Symbol', '').strip()
            if symbol in target_symbols:
                # Parse Date: M/D/YYYY
                date_str = row.get('Date', '').strip()
                try:
                    dt = datetime.strptime(date_str, "%m/%d/%Y").date()
                except ValueError:
                    logger.warning(f"Skipping invalid date: {date_str}")
                    continue
                
                open_p = float(row.get('Open', 0))
                high_p = float(row.get('High', 0))
                low_p = float(row.get('Low', 0))
                close_p = float(row.get('Close', 0))
                change = float(row.get('Change', 0) or 0)
                change_pct = float(row.get('% Change', 0) or 0)
                volume = int(float(row.get('Volume Traded', 0)))
                value = float(row.get('Value Traded (SAR)', 0) or 0)
                trades = int(float(row.get('No. of Trades', 0) or 0))

                # Update DB
                sql = text("""
                    UPDATE prices
                    SET open = :open_p, high = :high_p, low = :low_p, close = :close_p,
                        change = :change, change_percent = :change_pct,
                        volume_traded = :volume, value_traded_sar = :value, no_of_trades = :trades
                    WHERE symbol = :symbol AND date = :dt
                """)
                
                res = db.execute(sql, {
                    'open_p': open_p, 'high_p': high_p, 'low_p': low_p, 'close_p': close_p,
                    'change': change, 'change_pct': change_pct, 'volume': volume,
                    'value': value, 'trades': trades, 'symbol': symbol, 'dt': dt
                })
                
                if res.rowcount > 0:
                    updates += 1
                
                if updates % 1000 == 0 and updates > 0:
                    db.commit()
                    logger.info(f"Updated {updates} rows so far...")

    db.commit()
    logger.info(f"Finished! Total rows updated: {updates}")

if __name__ == "__main__":
    run()
