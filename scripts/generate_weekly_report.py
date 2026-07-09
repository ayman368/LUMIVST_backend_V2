"""
generate_weekly_report.py
=========================
Build and persist a Saudi Weekly Market Update report.

Usage:
  cd backend
  ..\\venv\\Scripts\\python.exe scripts\\generate_weekly_report.py
  ..\\venv\\Scripts\\python.exe scripts\\generate_weekly_report.py --week-end 2026-06-11
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.services.weekly_report.data_loader import resolve_week_end
from app.services.weekly_report.persistence import generate_and_save

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate weekly market update report")
    parser.add_argument(
        "--week-end",
        type=str,
        default=None,
        help="Week end date YYYY-MM-DD (default: latest price date)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build report but do not save to DB",
    )
    args = parser.parse_args()

    week_end = date.fromisoformat(args.week_end) if args.week_end else None

    db = SessionLocal()
    try:
        target = resolve_week_end(db, week_end)
        logger.info("Generating report for week ending %s", target)

        if args.dry_run:
            from app.services.weekly_report.report_builder import build_weekly_report

            report = build_weekly_report(db, target)
            print(json.dumps(
                {
                    "week_label": report["week_label"],
                    "sectors": len(report["sector_analytics"]),
                    "breakouts": len(report["breakouts"]["breakouts"]),
                    "volume_gainers": len(report["volume_gainers"]),
                },
                indent=2,
            ))
        else:
            row = generate_and_save(db, target)
            logger.info("Saved report id=%s  %s", row.id, row.week_label)
    finally:
        db.close()


if __name__ == "__main__":
    main()
