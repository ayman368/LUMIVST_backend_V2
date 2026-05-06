"""
Quick script to reset the naaim_exposure table.
Run from backend dir: ..\venv\Scripts\python.exe -m scripts.reset_naaim_table
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import engine, Base
from app.models.naaim_exposure import NaaimExposure
from sqlalchemy import text

def reset_table():
    with engine.connect() as conn:
        # Drop table if exists
        conn.execute(text("DROP TABLE IF EXISTS naaim_exposure CASCADE"))
        conn.commit()
        print("🗑️  Dropped naaim_exposure table")

    # Recreate
    NaaimExposure.__table__.create(bind=engine, checkfirst=True)
    print("✅ Created naaim_exposure table with fresh schema")

if __name__ == "__main__":
    reset_table()
