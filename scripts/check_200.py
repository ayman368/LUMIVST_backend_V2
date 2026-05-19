import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.core.database import SessionLocal
from app.models.market_pulse import MarketPulse

db = SessionLocal()
records = db.query(MarketPulse).order_by(MarketPulse.date.desc()).limit(200).all()
for r in records[-5:]:
    print(r.date, r.close)
print(f"Oldest date in last 200 rows: {records[-1].date}")
print(f"Total rows fetched: {len(records)}")
db.close()
