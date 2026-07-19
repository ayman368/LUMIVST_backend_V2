import sys
import os

sys.path.append('d:/Work/LUMIVST/backend')
from app.core.database import SessionLocal

try:
    db = SessionLocal()
    from sqlalchemy import text
    # Removed adj_close and split_coefficient as they might not exist
    prices = db.execute(text("SELECT date, close FROM prices WHERE symbol='2001' ORDER BY date DESC LIMIT 300")).fetchall()
    
    print("Recent prices:")
    for p in prices[:10]:
        print(p)
    print("...")
    print("Old prices (~1 year ago):")
    for p in prices[-10:]:
        print(p)
    
except Exception as e:
    print("Error:", e)
