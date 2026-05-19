import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.database import SessionLocal
from app.models.market_reports import HistoricalReport
from app.models.market_pulse import MarketPulse

def main():
    db = SessionLocal()
    try:
        # Check historical reports
        hist_2026 = db.query(HistoricalReport).filter(HistoricalReport.report_date >= '2026-01-01').count()
        print(f"📊 Historical Reports for 2026 onwards: {hist_2026} rows")

        # Check market pulse
        mp_2026 = db.query(MarketPulse).filter(MarketPulse.date >= '2026-01-01').count()
        print(f"📈 Market Pulse records for 2026 onwards: {mp_2026} rows")
        
        if mp_2026 > 0:
            latest = db.query(MarketPulse.date).order_by(MarketPulse.date.desc()).first()
            print(f"🗓️ Latest date in Market Pulse: {latest[0]}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
