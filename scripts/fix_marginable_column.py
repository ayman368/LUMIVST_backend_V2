#!/usr/bin/env python3
"""
Quick fix: ALTER marginable_percent from NUMERIC(5,4) to NUMERIC(10,4)
so it can hold values like 100.0 (whole percentages).

This may take 1-3 minutes on a large table. DO NOT KILL the process.
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import text
from app.core.database import engine

print("Altering marginable_percent column to NUMERIC(10,4)...")
print("This may take a few minutes. Please wait...\n")

with engine.connect() as conn:
    # Check current type
    row = conn.execute(text("""
        SELECT numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = 'prices' AND column_name = 'marginable_percent'
    """)).fetchone()
    
    if row:
        print(f"  Current type: NUMERIC({row[0]},{row[1]})")
    
    if row and row[0] == 10 and row[1] == 4:
        print("  ✓ Already NUMERIC(10,4) — no change needed!")
    else:
        print("  Altering to NUMERIC(10,4)...")
        conn.execute(text(
            "ALTER TABLE prices ALTER COLUMN marginable_percent TYPE NUMERIC(10,4)"
        ))
        conn.commit()
        print("  ✓ Done!")

print("\nFinished. You can now run the import script.")
