#!/usr/bin/env python3
"""
Script to add / fix static stock fields on the prices table.
Run this whenever the column types need updating.

Column specs:
  approval_with_controls : VARCHAR(255)
  purge_amount           : NUMERIC(18,6)  — high-precision decimals e.g. 0.053800
  marginable_percent     : NUMERIC(10,4)  — whole percentages e.g. 100.0000, 75.0000
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent   # backend/scripts -> backend
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import text
from app.core.database import engine


def add_static_stock_columns():
    """Add or ALTER the static stock columns to the correct precision."""

    # Target definitions — keep order deterministic
    columns_to_add = [
        ("approval_with_controls", "VARCHAR(255)"),
        ("purge_amount",           "NUMERIC(18,6)"),
        ("marginable_percent",     "NUMERIC(10,4)"),
    ]

    target_cols = {name for name, _ in columns_to_add}

    with engine.connect() as conn:
        # ── 1. Find which columns already exist ────────────────────────────
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'prices'
              AND column_name = ANY(:cols)
        """), {"cols": list(target_cols)})
        existing_columns = {row[0] for row in result}

        print(f"Existing static columns: {existing_columns or 'none'}")

        # ── 2. Add or ALTER each column ────────────────────────────────────
        for column_name, column_type in columns_to_add:
            if column_name not in existing_columns:
                print(f"  + Adding  {column_name!r:<30} {column_type}")
                try:
                    conn.execute(text(
                        f"ALTER TABLE prices ADD COLUMN {column_name} {column_type}"
                    ))
                    conn.commit()
                    print(f"    ✓ done")
                except Exception as exc:
                    print(f"    ✗ {exc}")
                    conn.rollback()
            else:
                # Column already exists — ensure correct type / precision
                print(f"  ~ Altering {column_name!r:<30} → {column_type}")
                try:
                    conn.execute(text(
                        f"ALTER TABLE prices "
                        f"ALTER COLUMN {column_name} TYPE {column_type}"
                    ))
                    conn.commit()
                    print(f"    ✓ done")
                except Exception as exc:
                    print(f"    ✗ {exc}")
                    conn.rollback()

        print("\nSchema update completed.")


if __name__ == "__main__":
    add_static_stock_columns()