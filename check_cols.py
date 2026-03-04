from app.core.database import SessionLocal
from sqlalchemy import text
db = SessionLocal()

# Check if columns exist in the actual DB table
res = db.execute(text("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'prices'
    ORDER BY ordinal_position
""")).fetchall()
print("=== PRICES TABLE COLUMNS ===")
for r in res:
    print(r[0], '-', r[1])

# Check how many non-null values exist for these columns
check_cols = ['ema_10', 'sma_3', 'ema_20_sma3', 'sma_4', 'sma_9', 'sma_18', 'sma_4w', 'sma_9w', 'sma_18w']
print("\n=== NULL COUNT CHECK FOR EACH COLUMN ===")
for col in check_cols:
    try:
        count = db.execute(text(f"SELECT COUNT(*) FROM prices WHERE {col} IS NOT NULL")).scalar()
        print(f"{col}: {count} non-null rows")
    except Exception as e:
        print(f"{col}: COLUMN MISSING - {e}")
