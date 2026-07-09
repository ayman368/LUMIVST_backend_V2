"""
add_weekly_market_reports_table.py
==================================
Creates weekly_market_reports table. Safe to re-run.

Usage:
  cd backend
  ..\\venv\\Scripts\\python.exe scripts\\add_weekly_market_reports_table.py
"""

import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal, engine
from app.core.config import settings


def main() -> None:
    print(f"Connecting to: {settings.DATABASE_URL[:40]}…")
    db = SessionLocal()
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS weekly_market_reports (
                id SERIAL PRIMARY KEY,
                week_start DATE NOT NULL,
                week_end DATE NOT NULL UNIQUE,
                week_label VARCHAR(120),
                report_data JSONB NOT NULL,
                generated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        db.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_weekly_reports_week_end
            ON weekly_market_reports (week_end DESC)
        """))
        db.commit()
        print("weekly_market_reports table ready.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
