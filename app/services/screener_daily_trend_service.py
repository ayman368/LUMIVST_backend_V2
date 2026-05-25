"""
Pre-computed Minervini / Alrayan daily counts — how production dashboards avoid heavy queries.

- Daily job: upsert ONE row per market date (~4 fast COUNT queries).
- Backfill script: bulk load history once into `screener_daily_trend_counts`.
- API: SELECT ordered rows (milliseconds), optional Redis cache on top.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.models.screener_daily_trend import ScreenerDailyTrend
from app.models.stock_indicators import StockIndicator
from app.models.rs_daily import RSDaily

logger = logging.getLogger(__name__)

_WIDE_FILTERS = and_(
    StockIndicator.sma_50 > StockIndicator.sma_200,
    StockIndicator.sma_200 > StockIndicator.sma_200_5m_ago,
    StockIndicator.price_vs_sma_50_percent > 0.0,
    StockIndicator.price_vs_sma_150_percent > 0.0,
    StockIndicator.price_vs_sma_200_percent > 0.0,
    StockIndicator.sma_30w.isnot(None),
    StockIndicator.close > StockIndicator.sma_30w,
    StockIndicator.sma_40w.isnot(None),
    StockIndicator.close > StockIndicator.sma_40w,
)

_MONTH1_FILTERS = and_(
    RSDaily.rs_rating > 69,
    StockIndicator.sma_50 > StockIndicator.sma_150,
    StockIndicator.sma_50 > StockIndicator.sma_200,
    StockIndicator.sma_150 > StockIndicator.sma_200,
    StockIndicator.sma_200 > StockIndicator.sma_200_1m_ago,
    StockIndicator.percent_off_52w_low > 30.0,
    StockIndicator.percent_off_52w_high > -25.0,
    StockIndicator.price_vs_sma_50_percent > 0.0,
    StockIndicator.price_vs_sma_150_percent > 0.0,
    StockIndicator.price_vs_sma_200_percent > 0.0,
    StockIndicator.sma_30w.isnot(None),
    StockIndicator.close > StockIndicator.sma_30w,
    StockIndicator.sma_40w.isnot(None),
    StockIndicator.close > StockIndicator.sma_40w,
)

_MONTH4_FILTERS = and_(
    RSDaily.rs_rating > 69,
    StockIndicator.sma_50 > StockIndicator.sma_150,
    StockIndicator.sma_50 > StockIndicator.sma_200,
    StockIndicator.sma_150 > StockIndicator.sma_200,
    StockIndicator.sma_200 > StockIndicator.sma_200_1m_ago,
    StockIndicator.sma_200 > StockIndicator.sma_200_2m_ago,
    StockIndicator.sma_200 > StockIndicator.sma_200_3m_ago,
    StockIndicator.sma_200 > StockIndicator.sma_200_4m_ago,
    StockIndicator.sma_200_1m_ago > StockIndicator.sma_200_2m_ago,
    StockIndicator.sma_200_2m_ago > StockIndicator.sma_200_3m_ago,
    StockIndicator.sma_200_3m_ago > StockIndicator.sma_200_4m_ago,
    StockIndicator.percent_off_52w_high > -25.0,
    StockIndicator.percent_off_52w_low > 30.0,
    StockIndicator.price_vs_sma_50_percent > 0.0,
    StockIndicator.price_vs_sma_150_percent > 0.0,
    StockIndicator.price_vs_sma_200_percent > 0.0,
    StockIndicator.sma_30w.isnot(None),
    StockIndicator.close > StockIndicator.sma_30w,
    StockIndicator.sma_40w.isnot(None),
    StockIndicator.close > StockIndicator.sma_40w,
)


def compute_counts_for_date(db: Session, target: date) -> dict[str, int]:
    """Four indexed COUNTs for a single trading day — used by the daily pipeline."""
    wide = (
        db.query(func.count(StockIndicator.symbol))
        .filter(StockIndicator.date == target, _WIDE_FILTERS)
        .scalar()
        or 0
    )

    month1 = (
        db.query(func.count(StockIndicator.symbol))
        .join(
            RSDaily,
            and_(
                RSDaily.symbol == StockIndicator.symbol,
                RSDaily.date == StockIndicator.date,
            ),
        )
        .filter(StockIndicator.date == target, _MONTH1_FILTERS)
        .scalar()
        or 0
    )

    month4 = (
        db.query(func.count(StockIndicator.symbol))
        .join(
            RSDaily,
            and_(
                RSDaily.symbol == StockIndicator.symbol,
                RSDaily.date == StockIndicator.date,
            ),
        )
        .filter(StockIndicator.date == target, _MONTH4_FILTERS)
        .scalar()
        or 0
    )

    alrayan = (
        db.query(func.count(StockIndicator.symbol))
        .filter(StockIndicator.date == target, StockIndicator.trend_signal == True)
        .scalar()
        or 0
    )

    return {
        "trend_1m": int(month1),
        "trend_4m": int(month4),
        "trend_5m_wide": int(wide),
        "alrayan": int(alrayan),
    }


def upsert_daily_row(db: Session, target: date, counts: dict[str, int]) -> None:
    row = db.query(ScreenerDailyTrend).filter(ScreenerDailyTrend.date == target).first()
    if row is None:
        row = ScreenerDailyTrend(date=target)
        db.add(row)
    row.trend_1m = counts["trend_1m"]
    row.trend_4m = counts["trend_4m"]
    row.trend_5m_wide = counts["trend_5m_wide"]
    row.alrayan = counts["alrayan"]


def update_market_date(db: Session, market_date: date) -> None:
    counts = compute_counts_for_date(db, market_date)
    upsert_daily_row(db, market_date, counts)
    db.commit()
    logger.info(
        "Screener daily trend updated for %s (1m=%s, 4m=%s, 5mw=%s, alr=%s)",
        market_date,
        counts["trend_1m"],
        counts["trend_4m"],
        counts["trend_5m_wide"],
        counts["alrayan"],
    )


def row_count(db: Session) -> int:
    return db.query(func.count(ScreenerDailyTrend.date)).scalar() or 0


def load_series(db: Session, limit: int = 6000) -> list[dict[str, Any]]:
    rows = (
        db.query(ScreenerDailyTrend)
        .order_by(ScreenerDailyTrend.date.desc())
        .limit(limit)
        .all()
    )
    rows = list(reversed(rows))
    return [
        {
            "date": str(r.date),
            "trend_1m": r.trend_1m,
            "trend_4m": r.trend_4m,
            "trend_5m_wide": r.trend_5m_wide,
            "alrayan": r.alrayan,
        }
        for r in rows
    ]


def build_payload(db: Session, limit: int = 6000) -> dict[str, Any]:
    series = load_series(db, limit)
    return {
        "title": "Minervini Trend",
        "series": series,
        "total_dates": len(series),
    }


def _counts_for_date_chunk(db: Session, chunk_dates: list[date]) -> dict[str, dict[str, int]]:
    """Aggregate four screeners for a small date window (avoids 20+ min mega-queries)."""
    if not chunk_dates:
        return {}

    date_filter = StockIndicator.date.in_(chunk_dates)

    wide_rows = (
        db.query(StockIndicator.date, func.count(StockIndicator.symbol).label("count"))
        .filter(date_filter, _WIDE_FILTERS)
        .group_by(StockIndicator.date)
        .all()
    )
    wide_map = {str(r.date): int(r.count) for r in wide_rows}

    month1_rows = (
        db.query(StockIndicator.date, func.count(StockIndicator.symbol).label("count"))
        .join(
            RSDaily,
            and_(
                RSDaily.symbol == StockIndicator.symbol,
                RSDaily.date == StockIndicator.date,
            ),
        )
        .filter(date_filter, _MONTH1_FILTERS)
        .group_by(StockIndicator.date)
        .all()
    )
    month1_map = {str(r.date): int(r.count) for r in month1_rows}

    month4_rows = (
        db.query(StockIndicator.date, func.count(StockIndicator.symbol).label("count"))
        .join(
            RSDaily,
            and_(
                RSDaily.symbol == StockIndicator.symbol,
                RSDaily.date == StockIndicator.date,
            ),
        )
        .filter(date_filter, _MONTH4_FILTERS)
        .group_by(StockIndicator.date)
        .all()
    )
    month4_map = {str(r.date): int(r.count) for r in month4_rows}

    alrayan_rows = (
        db.query(StockIndicator.date, func.count(StockIndicator.symbol).label("count"))
        .filter(date_filter, StockIndicator.trend_signal == True)
        .group_by(StockIndicator.date)
        .all()
    )
    alrayan_map = {str(r.date): int(r.count) for r in alrayan_rows}

    out: dict[str, dict[str, int]] = {}
    for d in chunk_dates:
        key = str(d)
        out[key] = {
            "trend_1m": month1_map.get(key, 0),
            "trend_4m": month4_map.get(key, 0),
            "trend_5m_wide": wide_map.get(key, 0),
            "alrayan": alrayan_map.get(key, 0),
        }
    return out


def backfill_history(
    limit: int = 6000,
    *,
    chunk_size: int = 60,
    verbose: bool = True,
) -> int:
    """
    Safe backfill: small SQL chunks + commit per chunk + skip existing rows (resumable).
    """
    import time
    from app.core.database import SessionLocal

    db = SessionLocal()
    try:
        date_rows = (
            db.query(StockIndicator.date)
            .distinct()
            .order_by(StockIndicator.date.desc())
            .limit(limit)
            .all()
        )
        all_dates = sorted(r[0] for r in date_rows)
        existing = {r[0] for r in db.query(ScreenerDailyTrend.date).all()}
    finally:
        db.close()

    pending = [d for d in all_dates if d not in existing]
    if verbose:
        print(
            f"Backfill: {len(pending)} days to process "
            f"({len(existing)} already in table, chunk={chunk_size})",
            flush=True,
        )
    if not pending:
        return 0

    written = 0
    started = time.time()
    for start in range(0, len(pending), chunk_size):
        chunk = pending[start : start + chunk_size]
        db = SessionLocal()
        try:
            maps = _counts_for_date_chunk(db, chunk)
            for d in chunk:
                upsert_daily_row(db, d, maps[str(d)])
            db.commit()
            written += len(chunk)
            if verbose:
                pct = min(100, int(100 * (start + len(chunk)) / len(pending)))
                print(
                    f"  chunk {start // chunk_size + 1}: "
                    f"{start + len(chunk)}/{len(pending)} days ({pct}%) "
                    f"— {time.time() - started:.0f}s elapsed",
                    flush=True,
                )
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    return written


def bulk_upsert_series(db: Session, series: list[dict[str, Any]], *, batch: int = 200) -> int:
    """Insert/update many days (from one-off backfill compute)."""
    written = 0
    for i, point in enumerate(series, start=1):
        d = point["date"]
        if isinstance(d, str):
            from datetime import datetime

            d = datetime.strptime(d[:10], "%Y-%m-%d").date()
        upsert_daily_row(
            db,
            d,
            {
                "trend_1m": int(point.get("trend_1m", 0)),
                "trend_4m": int(point.get("trend_4m", 0)),
                "trend_5m_wide": int(point.get("trend_5m_wide", 0)),
                "alrayan": int(point.get("alrayan", 0)),
            },
        )
        written += 1
        if i % batch == 0:
            db.commit()
            logger.info("Backfill progress: %s / %s days", i, len(series))
    db.commit()
    return written
