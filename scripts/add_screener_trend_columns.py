"""
add_screener_trend_columns.py
=============================
Adds missing columns to screener_daily_trend_counts directly in PostgreSQL.
Bypasses Alembic — safe to re-run (IF NOT EXISTS).

Usage:
  cd backend
  ..\\venv\\Scripts\\python.exe scripts\\add_screener_trend_columns.py
"""
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal, engine
from app.core.config import settings


COLUMNS = [
    # (column_name, sql_type, server_default)
    ("alhussain", "INTEGER", "0"),
]


def ensure_table_exists(db) -> None:
    """Create screener_daily_trend_counts if the table is missing entirely."""
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS screener_daily_trend_counts (
            date DATE PRIMARY KEY,
            trend_1m INTEGER NOT NULL DEFAULT 0,
            trend_4m INTEGER NOT NULL DEFAULT 0,
            trend_5m_wide INTEGER NOT NULL DEFAULT 0,
            alrayan INTEGER NOT NULL DEFAULT 0,
            alhussain INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))
    db.commit()


def add_column_if_missing(db, col_name: str, col_type: str, default: str | None = None) -> bool:
    """Return True if column was added, False if it already existed."""
    row = db.execute(
        text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'screener_daily_trend_counts'
              AND column_name = :col
        """),
        {"col": col_name},
    ).fetchone()

    if row:
        print(f"  ✓ {col_name} — already exists")
        return False

    default_clause = f" DEFAULT {default}" if default is not None else ""
    db.execute(text(
        f"ALTER TABLE screener_daily_trend_counts "
        f"ADD COLUMN {col_name} {col_type} NOT NULL{default_clause}"
    ))
    db.commit()
    print(f"  + {col_name} — added ({col_type})")
    return True


def main() -> None:
    print("=" * 60)
    print("Add screener_daily_trend_counts columns (direct SQL)")
    print(f"DB: {settings.DATABASE_URL.split('@')[-1] if '@' in str(settings.DATABASE_URL) else 'local'}")
    print("=" * 60)

    db = SessionLocal()
    try:
        print("\n[1/2] Ensure table exists...")
        ensure_table_exists(db)
        print("  ✓ screener_daily_trend_counts OK")

        print("\n[2/2] Add missing columns...")
        added = 0
        for col_name, col_type, default in COLUMNS:
            if add_column_if_missing(db, col_name, col_type, default):
                added += 1

        print(f"\nDone — {added} new column(s) added.")
        if added == 0:
            print("Nothing to do; schema is up to date.")
        else:
            print("\nNext step — backfill alhussain history:")
            print("  ..\\venv\\Scripts\\python.exe scripts\\backfill_alhussain_daily.py")
    except Exception as e:
        db.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
