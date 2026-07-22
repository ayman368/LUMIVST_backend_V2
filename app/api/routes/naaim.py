"""
NAAIM Exposure Index API Router

Endpoints:
  GET  /api/naaim/latest      → Current value + summary statistics
  GET  /api/naaim/history     → Paginated historical data
  GET  /api/naaim/chart-data  → Chart-ready data with moving averages
  POST /api/naaim/scrape      → Trigger manual scrape (protected by X-Internal-Key)

Changes from previous version:
  - GET /scrape endpoint removed — it was a security risk (proxies/CDNs could trigger
    scrapes without authentication). Admin dashboard should use POST /scrape instead.
  - Cache key strings replaced with CACHE_KEYS constants imported from the scraper
    module — single source of truth, no more copy/paste typos between files.
  - `posted_on` and `last_quarter_label` now properly included in the response dict
    (they were already being sent but not declared in the Pydantic schema).
  - Redis socket_timeout unified via REDIS_SOCKET_TIMEOUT constant from scraper module.
  - Cache-clear after scrape uses CACHE_KEYS["all_pattern"] — no hardcoded "naaim:*".
"""

import logging
import threading
from typing import Optional
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from app.core.database import get_db
from app.core.security import verify_internal_key
from app.core.redis import redis_cache
from app.models.naaim_exposure import NaaimExposure
from app.schemas.naaim_exposure import (
    NaaimExposureResponse,
    NaaimLatestResponse,
    NaaimHistoryResponse,
    NaaimChartPoint,
    NaaimChartResponse,
)
# Import cache key registry and Redis timeout from the scraper — single source of truth
from app.scrapers.naaim_scraper import CACHE_KEYS, REDIS_SOCKET_TIMEOUT

logger = logging.getLogger(__name__)
router = APIRouter()


# ──────────────────────────────────────────────────────────────
# Helper: read page metadata (last_quarter_avg, last_quarter_label, posted_on)
# ──────────────────────────────────────────────────────────────
def _get_cached_page_metadata() -> dict:
    """
    Read NAAIM page metadata from Redis.
    Populated by the scraper after each successful run.
    """
    import json
    from app.core.config import settings
    import redis as sync_redis

    result = {"last_quarter_avg": None, "last_quarter_label": None, "posted_on": None}
    try:
        r = sync_redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
        )
        cached = r.get(CACHE_KEYS["page_metadata"])
        r.close()

        if cached:
            data = json.loads(cached)
            result["last_quarter_avg"] = data.get("last_quarter_avg")
            result["last_quarter_label"] = data.get("last_quarter_label")
            result["posted_on"] = data.get("posted_on")
    except Exception as e:
        logger.warning(f"⚠️ Failed to read NAAIM page metadata from Redis: {e}")

    return result


# ──────────────────────────────────────────────────────────────
# GET /latest
# ──────────────────────────────────────────────────────────────
def _fetch_latest(db: Session) -> dict:
    """Build the latest response dict with statistics."""

    current = db.query(NaaimExposure).order_by(desc(NaaimExposure.date)).first()
    if not current:
        return None

    previous = (
        db.query(NaaimExposure)
        .filter(NaaimExposure.date < current.date)
        .order_by(desc(NaaimExposure.date))
        .first()
    )

    week_change = None
    if previous:
        week_change = round(current.naaim_index - previous.naaim_index, 2)

    today = date.today()

    # Page metadata from Redis (populated by scraper)
    page_info = _get_cached_page_metadata()
    last_quarter_avg = page_info.get("last_quarter_avg")
    last_quarter_label = page_info.get("last_quarter_label")
    posted_on = page_info.get("posted_on")

    # Fallback: compute last_quarter_avg from DB if Redis has nothing
    if last_quarter_avg is None:
        current_quarter = (today.month - 1) // 3 + 1
        if current_quarter == 1:
            q_start = date(today.year - 1, 10, 1)
            q_end = date(today.year - 1, 12, 31)
        else:
            q_month = (current_quarter - 2) * 3 + 1
            q_start = date(today.year, q_month, 1)
            q_end_month = q_month + 2
            if q_end_month == 12:
                q_end = date(today.year, 12, 31)
            else:
                q_end = date(today.year, q_end_month + 1, 1) - timedelta(days=1)

        last_quarter_avg = (
            db.query(func.avg(NaaimExposure.naaim_index))
            .filter(NaaimExposure.date >= q_start, NaaimExposure.date <= q_end)
            .scalar()
        )
        if last_quarter_avg is not None:
            last_quarter_avg = round(float(last_quarter_avg), 2)

    # YTD average
    ytd_start = date(today.year, 1, 1)
    ytd_avg = (
        db.query(func.avg(NaaimExposure.naaim_index))
        .filter(NaaimExposure.date >= ytd_start)
        .scalar()
    )

    # All-time high / low
    all_time_high = db.query(func.max(NaaimExposure.naaim_index)).scalar()
    all_time_low = db.query(func.min(NaaimExposure.naaim_index)).scalar()

    total_records = db.query(func.count(NaaimExposure.id)).scalar()

    return {
        "current": _to_response(current),
        "previous": _to_response(previous) if previous else None,
        "week_change": week_change,
        "last_quarter_avg": round(float(last_quarter_avg), 2) if last_quarter_avg is not None else None,
        "last_quarter_label": last_quarter_label,   # e.g. "Q1" — declared in schema now
        "posted_on": posted_on,                     # declared in schema now
        "ytd_avg": round(float(ytd_avg), 2) if ytd_avg else None,
        "all_time_high": float(all_time_high) if all_time_high else None,
        "all_time_low": float(all_time_low) if all_time_low else None,
        "total_records": total_records or 0,
    }


def _to_response(obj) -> dict:
    """Convert SQLAlchemy NaaimExposure model instance to a plain dict."""
    if not obj:
        return None
    return {
        "id": obj.id,
        "date": obj.date.isoformat(),
        "naaim_index": obj.naaim_index,
        "sp500": obj.sp500,
        "bearish": obj.bearish,
        "quartile_1": obj.quartile_1,
        "quartile_2": obj.quartile_2,
        "quartile_3": obj.quartile_3,
        "bullish": obj.bullish,
        "std_deviation": obj.std_deviation,
        "yoy_pct": obj.yoy_pct,
        "created_at": obj.created_at.isoformat() if obj.created_at else None,
        "updated_at": obj.updated_at.isoformat() if obj.updated_at else None,
    }


@router.get("/latest")
async def get_naaim_latest(db: Session = Depends(get_db)):
    """Get the most recent NAAIM Exposure Index value with summary statistics."""
    cache_key = CACHE_KEYS["latest"]
    cached = await redis_cache.get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    result = await run_in_threadpool(_fetch_latest, db)
    if not result:
        raise HTTPException(status_code=404, detail="No NAAIM data available")

    try:
        await redis_cache.set(cache_key, result, expire=1800)  # 30 min
    except Exception as e:
        logger.warning(f"Failed to cache NAAIM latest: {e}")

    return JSONResponse(content=result)


# ──────────────────────────────────────────────────────────────
# GET /history
# ──────────────────────────────────────────────────────────────
def _fetch_history(db: Session, limit: int, offset: int, start_date, end_date) -> dict:
    query = db.query(NaaimExposure)

    if start_date:
        query = query.filter(NaaimExposure.date >= start_date)
    if end_date:
        query = query.filter(NaaimExposure.date <= end_date)

    total = query.count()
    records = (
        query.order_by(desc(NaaimExposure.date))
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "data": [_to_response(r) for r in records],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/history")
async def get_naaim_history(
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
):
    """Get paginated NAAIM historical data."""
    cache_key = f"{CACHE_KEYS['history_prefix']}{limit}:{offset}:{start_date}:{end_date}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    result = await run_in_threadpool(_fetch_history, db, limit, offset, start_date, end_date)

    try:
        await redis_cache.set(cache_key, result, expire=1800)
    except Exception as e:
        logger.warning(f"Failed to cache NAAIM history: {e}")

    return JSONResponse(content=result)


# ──────────────────────────────────────────────────────────────
# GET /chart-data
# ──────────────────────────────────────────────────────────────
def _fetch_chart_data(db: Session, limit: int) -> dict:
    """Build chart-optimised data with 2-week moving average."""
    records = (
        db.query(
            NaaimExposure.date,
            NaaimExposure.naaim_index,
            NaaimExposure.sp500,
            NaaimExposure.yoy_pct,
            NaaimExposure.updated_at,
        )
        .order_by(NaaimExposure.date)
        .limit(limit)
        .all()
    )

    if not records:
        return {"data": [], "last_updated": None}

    data_points = []
    for i, r in enumerate(records):
        ma = round((records[i].naaim_index + records[i - 1].naaim_index) / 2, 2) if i >= 1 else r.naaim_index
        data_points.append({
            "date": r.date.isoformat(),
            "naaim_index": r.naaim_index,
            "sp500": r.sp500,
            "naaim_ma": ma,
            "yoy_pct": r.yoy_pct,
        })

    last_updated = records[-1].updated_at.isoformat() if records[-1].updated_at else None
    return {"data": data_points, "last_updated": last_updated}


@router.get("/chart-data")
async def get_naaim_chart_data(
    limit: int = Query(2000, ge=1, le=5000, description="Max records to return"),
    db: Session = Depends(get_db),
):
    """Get chart-ready NAAIM data with moving averages for Recharts / Chart.js."""
    cache_key = f"{CACHE_KEYS['chart_prefix']}{limit}"
    cached = await redis_cache.get(cache_key)
    if cached:
        return JSONResponse(content=cached)

    result = await run_in_threadpool(_fetch_chart_data, db, limit)

    try:
        await redis_cache.set(cache_key, result, expire=1800)
    except Exception as e:
        logger.warning(f"Failed to cache NAAIM chart data: {e}")

    return JSONResponse(content=result)


# ──────────────────────────────────────────────────────────────
# POST /scrape — Protected manual trigger
# ──────────────────────────────────────────────────────────────
@router.post("/scrape")
def trigger_naaim_scrape(
    mode: str = Query("auto", description="Scrape mode: auto, full, incremental"),
    _: bool = Depends(verify_internal_key),
):
    """
    Manually trigger the NAAIM scraper.
    Protected by X-Internal-Key header.

    Note: The previous GET /scrape endpoint has been removed. GET endpoints are
    cached by proxies and CDNs and could bypass authentication. Use POST only.
    """
    from app.scrapers.naaim_scraper import scrape_naaim

    def _scrape_with_error_handling():
        try:
            result = scrape_naaim(mode=mode)
            logger.info(f"NAAIM scraping completed: {result}")

            # Clear all NAAIM cache keys after successful scrape
            try:
                import os
                import redis as sync_redis
                redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
                r = sync_redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_timeout=REDIS_SOCKET_TIMEOUT,
                )
                keys = r.keys(CACHE_KEYS["all_pattern"])
                # Do not delete page_metadata, it was just cached by the scraper
                keys_to_delete = [k for k in keys if k != CACHE_KEYS["page_metadata"]]
                if keys_to_delete:
                    r.delete(*keys_to_delete)
                    logger.info(f"🗑️ Cleared {len(keys_to_delete)} NAAIM cache keys")
                r.close()
            except Exception as e:
                logger.warning(f"⚠️ NAAIM cache clear failed: {e}")

        except Exception as e:
            logger.error(f"NAAIM scraping failed: {e}")

    thread = threading.Thread(target=_scrape_with_error_handling, daemon=True)
    thread.start()
    return {"message": f"NAAIM scraping started (mode={mode})"}