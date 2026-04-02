import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal

def main():
    print("🔧 Connecting to database...")
    db = SessionLocal()
    try:
        # Check if beta exists first or just add IF NOT EXISTS
        query = "ALTER TABLE stock_indicators ADD COLUMN IF NOT EXISTS beta NUMERIC(12, 4);"
        db.execute(text(query))
        db.commit()
        print("✅ Column 'beta' (NUMERIC 12,4) added successfully to 'stock_indicators' table.")
    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
