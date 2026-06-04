"""
Verify screener_daily_trend_counts — do you need backfill_screener_daily_trend.py?

Compares the pre-aggregated table to stock_indicators trading days and spot-checks
stored counts vs live compute_counts_for_date().

Usage:
  cd backend
  python scripts/verify_screener_daily_trend.py

  python scripts/verify_screener_daily_trend.py --limit 6000 --spot-check 12
  python scripts/verify_screener_daily_trend.py --show-missing 20
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, text

from app.core.database import SessionLocal
from app.models.screener_daily_trend import ScreenerDailyTrend
from app.models.stock_indicators import StockIndicator
from app.services.screener_daily_trend_service import compute_counts_for_date

PASS = "\033[92m✅\033[0m"
FAIL = "\033[91m❌\033[0m"
WARN = "\033[93m⚠️\033[0m"


def header(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


def _expected_dates(db, limit: int) -> list[date]:
    rows = (
        db.query(StockIndicator.date)
        .distinct()
        .order_by(StockIndicator.date.desc())
        .limit(limit)
        .all()
    )
    return sorted(r[0] for r in rows)


def _stored_dates(db) -> set[date]:
    return {r[0] for r in db.query(ScreenerDailyTrend.date).all()}


def _find_long_zero_runs(db, min_days: int) -> list[dict]:
    rows = db.query(ScreenerDailyTrend).order_by(ScreenerDailyTrend.date).all()
    runs: list[dict] = []
    start: date | None = None
    end: date | None = None
    for r in rows:
        all_zero = (
            r.trend_1m == 0
            and r.trend_4m == 0
            and r.trend_5m_wide == 0
            and r.alrayan == 0
        )
        if all_zero:
            if start is None:
                start = r.date
            end = r.date
        else:
            if start is not None and end is not None and (end - start).days + 1 >= min_days:
                runs.append(
                    {"from": start, "to": end, "days": (end - start).days + 1}
                )
            start = None
            end = None
    if start is not None and end is not None and (end - start).days + 1 >= min_days:
        runs.append({"from": start, "to": end, "days": (end - start).days + 1})
    return runs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify screener_daily_trend_counts coverage and accuracy"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=6000,
        help="Match backfill_screener_daily_trend.py window (default 6000)",
    )
    parser.add_argument(
        "--spot-check",
        type=int,
        default=10,
        help="Random dates to recompute and compare (default 10)",
    )
    parser.add_argument(
        "--show-missing",
        type=int,
        default=15,
        help="How many missing dates to print (default 15)",
    )
    parser.add_argument(
        "--zero-run-days",
        type=int,
        default=60,
        help="Warn if all-four-zero streak >= N days (default 60)",
    )
    args = parser.parse_args()

    issues: list[str] = []
    warnings: list[str] = []

    db = SessionLocal()
    try:
        header("1) Table overview")
        try:
            total_stored = db.query(func.count(ScreenerDailyTrend.date)).scalar() or 0
        except Exception as e:
            print(f"  {FAIL} Cannot read screener_daily_trend_counts: {e}")
            print("  → Run: python scripts/add_screener_trend_columns.py")
            return 1

        if total_stored == 0:
            print(f"  {FAIL} Table is EMPTY (0 rows)")
            issues.append("EMPTY_TABLE")
            print("\n  → You NEED: python scripts/backfill_screener_daily_trend.py")
            return 1

        min_d = db.query(func.min(ScreenerDailyTrend.date)).scalar()
        max_d = db.query(func.max(ScreenerDailyTrend.date)).scalar()
        print(f"  Stored rows:     {total_stored:,}")
        print(f"  Date range:      {min_d} → {max_d}")

        header("2) Coverage vs stock_indicators (backfill scope)")
        expected = _expected_dates(db, args.limit)
        stored = _stored_dates(db)
        expected_set = set(expected)

        missing = sorted(expected_set - stored)
        extra = sorted(stored - expected_set)

        print(f"  Trading days in stock_indicators (last {args.limit}): {len(expected):,}")
        print(f"  Days in screener_daily_trend_counts (all time):     {len(stored):,}")
        print(f"  Missing from table (need backfill):               {len(missing):,}")
        print(f"  Extra in table (outside SI window):               {len(extra):,}")

        if missing:
            issues.append(f"MISSING_{len(missing)}_DAYS")
            print(f"\n  {FAIL} Backfill still needed for {len(missing)} day(s)")
            if args.show_missing > 0:
                show = missing[: args.show_missing]
                print(f"  First missing: {show[0]}" + (f" … {show[-1]}" if len(show) > 1 else ""))
                for d in show:
                    print(f"    - {d}")
                if len(missing) > len(show):
                    print(f"    … and {len(missing) - len(show)} more")
            print("\n  → Run: python scripts/backfill_screener_daily_trend.py")
            print("     (Safe to re-run — skips days already stored)")
        else:
            print(f"\n  {PASS} Every stock_indicators day in window has a stored row")

        header("3) Latest day vs daily job")
        si_latest = (
            db.query(func.max(StockIndicator.date)).scalar()
        )
        print(f"  Latest stock_indicators date:  {si_latest}")
        print(f"  Latest screener_trend date:    {max_d}")

        try:
            status = db.execute(
                text(
                    "SELECT latest_ready_date, is_updating, completed_at "
                    "FROM update_status WHERE id = 1"
                )
            ).fetchone()
            if status:
                print(f"  update_status.latest_ready_date: {status[0]}")
                if status[1]:
                    warnings.append("UPDATE_IN_PROGRESS")
                    print(f"  {WARN} is_updating=TRUE (daily job may be running)")
        except Exception:
            warnings.append("NO_UPDATE_STATUS")
            print(f"  {WARN} update_status table not readable")

        if si_latest and max_d:
            if max_d < si_latest:
                gap = (si_latest - max_d).days
                issues.append("STALE_LATEST_DAY")
                print(
                    f"\n  {FAIL} Table is {gap} calendar day(s) behind stock_indicators"
                )
                print(
                    f"  → Run daily_market_update for missing days, or backfill for history"
                )
            elif max_d == si_latest:
                print(f"\n  {PASS} Latest screener row matches latest stock_indicators")
            else:
                warnings.append("SCREENER_AHEAD_OF_SI")
                print(f"\n  {WARN} screener max date is after stock_indicators (unusual)")

        header("4) Spot-check stored vs recomputed counts")
        in_window = [d for d in expected if d in stored]
        if not in_window:
            print(f"  {WARN} No overlap to spot-check")
        else:
            n = min(args.spot_check, len(in_window))
            sample = sorted(
                random.sample(in_window, n) if len(in_window) > n else in_window
            )
            mismatches: list[str] = []
            for d in sample:
                row = (
                    db.query(ScreenerDailyTrend)
                    .filter(ScreenerDailyTrend.date == d)
                    .first()
                )
                if not row:
                    continue
                live = compute_counts_for_date(db, d)
                stored_vals = {
                    "trend_1m": row.trend_1m,
                    "trend_4m": row.trend_4m,
                    "trend_5m_wide": row.trend_5m_wide,
                    "alrayan": row.alrayan,
                    "alhussain": row.alhussain,
                }
                if stored_vals != live:
                    mismatches.append(str(d))
                    print(f"  {FAIL} {d}")
                    print(f"       stored:     {stored_vals}")
                    print(f"       recomputed: {live}")
                else:
                    print(f"  {PASS} {d}  (1M={live['trend_1m']} 4M={live['trend_4m']} "
                          f"5MW={live['trend_5m_wide']} ALR={live['alrayan']} ALH={live['alhussain']})")

            if mismatches:
                issues.append(f"MISMATCH_{len(mismatches)}_DATES")
                print(
                    f"\n  {FAIL} {len(mismatches)}/{n} spot-checks differ "
                    f"(rules or data changed since backfill)"
                )
                print("  → Re-run: python scripts/backfill_screener_daily_trend.py")
                print("     or: python scripts/backfill_alhussain_daily.py (if only alhussain)")
            elif n:
                print(f"\n  {PASS} All {n} spot-checks match live computation")

        header("5) Alhussain column")
        alh_max = db.query(func.max(ScreenerDailyTrend.alhussain)).scalar() or 0
        alh_nonzero_days = (
            db.query(func.count(ScreenerDailyTrend.date))
            .filter(ScreenerDailyTrend.alhussain > 0)
            .scalar()
            or 0
        )
        print(f"  Days with alhussain > 0: {alh_nonzero_days:,} / {total_stored:,}")
        print(f"  Max alhussain count:      {alh_max}")

        if si_latest and max_d and max_d >= si_latest:
            latest_row = (
                db.query(ScreenerDailyTrend)
                .filter(ScreenerDailyTrend.date == max_d)
                .first()
            )
            if latest_row and latest_row.alhussain == 0:
                live_latest = compute_counts_for_date(db, max_d)
                if live_latest.get("alhussain", 0) > 0:
                    issues.append("ALHUSSAIN_NOT_BACKFILLED")
                    print(
                        f"\n  {FAIL} Latest day has alhussain=0 in table but "
                        f"live count={live_latest['alhussain']}"
                    )
                    print("  → Run: python scripts/backfill_alhussain_daily.py")
                else:
                    print(f"\n  {PASS} Latest alhussain=0 matches live (market may have none)")
            elif latest_row and latest_row.alhussain > 0:
                print(f"\n  {PASS} Latest alhussain={latest_row.alhussain}")

        if alh_nonzero_days == 0 and total_stored > 100:
            warnings.append("ALHUSSAIN_ALL_ZEROS")
            print(f"\n  {WARN} alhussain is 0 for all stored days — column may never have been backfilled")

        header("6) Long all-zero chart periods (informational)")
        runs = _find_long_zero_runs(db, args.zero_run_days)
        if not runs:
            print(f"  {PASS} No streak of all-four-zero >= {args.zero_run_days} days")
        else:
            print(
                f"  {WARN} Found {len(runs)} period(s) where 1M/4M/5MW/Alrayan are all 0 "
                f"(>= {args.zero_run_days} days):"
            )
            for run in runs[:5]:
                print(f"    {run['from']} → {run['to']}  ({run['days']} days)")
            if len(runs) > 5:
                print(f"    … and {len(runs) - 5} more")
            warnings.append("LONG_ZERO_RUNS")
            print(
                "\n  This is often normal for old years (no stock_indicators / strict rules)."
            )
            print("  Use audit_screener_trend_gaps.py --find-runs to diagnose a range.")

        header("VERDICT")
        if not issues:
            print(f"  {PASS} You do NOT need backfill_screener_daily_trend.py right now.")
            print("  Daily updates via daily_market_update → update_market_date() are enough.")
            if warnings:
                print(f"\n  Notes ({len(warnings)}):")
                for w in warnings:
                    print(f"    - {w}")
            return 0

        print(f"  {FAIL} Action recommended:\n")
        if "EMPTY_TABLE" in issues or any(i.startswith("MISSING_") for i in issues):
            print("    • python scripts/backfill_screener_daily_trend.py")
        if "STALE_LATEST_DAY" in issues:
            print("    • python scripts/daily_market_update.py  (or --date YYYY-MM-DD)")
        if "ALHUSSAIN_NOT_BACKFILLED" in issues or "ALHUSSAIN_ALL_ZEROS" in issues:
            print("    • python scripts/backfill_alhussain_daily.py")
        if any(i.startswith("MISMATCH_") for i in issues):
            print("    • python scripts/backfill_screener_daily_trend.py  (refresh counts)")
        for i in issues:
            print(f"    [{i}]")
        return 1

    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
