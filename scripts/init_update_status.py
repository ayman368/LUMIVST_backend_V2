"""
Script to dynamically create the update_status table and populate the initial record
with the most recent date available in the prices table to avoid downtime.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal, engine, Base
from app.models.update_status import UpdateStatus
from app.models.price import Price
from sqlalchemy import func
import datetime

# Create table
Base.metadata.create_all(bind=engine)

db = SessionLocal()
try:
    print("Initializing UpdateStatus table...")
    
    # Check if a record already exists
    existing = db.query(UpdateStatus).filter(UpdateStatus.id == 1).first()
    
    if existing:
        print(f"Record already exists with date: {existing.latest_ready_date}")
    else:
        # Get the max date from prices to seed it
        latest_date = db.query(func.max(Price.date)).scalar()
        
        if not latest_date:
            latest_date = datetime.date.today()
            
        print(f"Seeding with max date from prices: {latest_date}")
        
        status = UpdateStatus(
            id=1,
            latest_ready_date=latest_date,
            is_updating=False,
            started_at=None,
            completed_at=datetime.datetime.utcnow()
        )
        db.add(status)
        db.commit()
        print("Success: update_status initialized.")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
