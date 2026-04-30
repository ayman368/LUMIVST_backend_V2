import logging
import sqlite3
from sqlalchemy import text
from pathlib import Path

# Adjust path to your db as necessary
db_path = Path("D:/Work/LUMIVST/backend/data/lumivst.db")  # Replace with actual db if different

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_symbol_column():
    try:
        from app.core.database import engine
        
        # First, check if column exists
        column_exists = False
        with engine.connect() as conn:
            try:
                conn.execute(text("SELECT symbol FROM sofr_futures LIMIT 1"))
                column_exists = True
                logger.info("✅ Column 'symbol' already exists in sofr_futures.")
            except Exception:
                # Column doesn't exist, we will add it below
                pass

        if not column_exists:
            with engine.begin() as conn:
                logger.info("⚙️ Adding 'symbol' column to sofr_futures...")
                conn.execute(text("ALTER TABLE sofr_futures ADD COLUMN symbol VARCHAR(50)"))
                logger.info("✅ Column 'symbol' added successfully!")
                
    except Exception as e:
        logger.error(f"❌ Error updating database schema: {e}")

if __name__ == "__main__":
    add_symbol_column()
