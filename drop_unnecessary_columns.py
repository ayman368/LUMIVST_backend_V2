import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

from sqlalchemy import text
from app.core.database import SessionLocal

db = SessionLocal()

# Drop unnecessary columns from stock_indicators table
columns_to_drop = ['ema10', 'ema21', 'sma50', 'sma150', 'sma200']

print("=" * 80)
print("🗑️  DROPPING UNNECESSARY COLUMNS FROM stock_indicators TABLE")
print("=" * 80)

for col in columns_to_drop:
    try:
        query = f"ALTER TABLE stock_indicators DROP COLUMN IF EXISTS {col};"
        db.execute(text(query))
        db.commit()
        print(f"✅ Dropped column: {col}")
    except Exception as e:
        print(f"❌ Failed to drop {col}: {str(e)}")
        db.rollback()

print("\n" + "=" * 80)
print("✅ ALL UNNECESSARY COLUMNS REMOVED")
print("=" * 80)

db.close()
