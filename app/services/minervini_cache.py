"""
Minervini historical-trend cache — warmed automatically on server startup
and after the daily market update. No manual script required for users.
"""
import asyncio
import logging

from app.core.redis import redis_cache
from app.api.routes.screeners import (
    HISTORICAL_TREND_CACHE_KEY,
    HISTORICAL_TREND_CACHE_TTL,
    HISTORICAL_TREND_FULL_LIMIT,
    _compute_historical_trend_sync,
)

logger = logging.getLogger(__name__)
_compute_lock = asyncio.Lock()


async def get_historical_trend_full() -> dict:
    """Return full cached series; compute once if missing (deduplicated)."""
    cached = await redis_cache.get(HISTORICAL_TREND_CACHE_KEY)
    if cached is not None:
        return cached

    async with _compute_lock:
        cached = await redis_cache.get(HISTORICAL_TREND_CACHE_KEY)
        if cached is not None:
            return cached

        logger.info("Minervini trend cache miss — computing in background thread")
        data = await asyncio.to_thread(
            _compute_historical_trend_sync, HISTORICAL_TREND_FULL_LIMIT
        )
        await redis_cache.set(
            HISTORICAL_TREND_CACHE_KEY, data, expire=HISTORICAL_TREND_CACHE_TTL
        )
        logger.info(
            "Minervini trend cache stored (%s dates)",
            data.get("total_dates", 0),
        )
        return data


async def warm_minervini_trend_cache(*, force: bool = False) -> bool:
    """
    Pre-populate Redis. Skips if already cached unless force=True.
    Safe to call from startup, cron, or optional dev script.
    """
    if not force:
        existing = await redis_cache.get(HISTORICAL_TREND_CACHE_KEY)
        if existing is not None:
            return True

    async with _compute_lock:
        if not force:
            existing = await redis_cache.get(HISTORICAL_TREND_CACHE_KEY)
            if existing is not None:
                return True

        logger.info("Warming Minervini historical-trend cache...")
        data = await asyncio.to_thread(
            _compute_historical_trend_sync, HISTORICAL_TREND_FULL_LIMIT
        )
        await redis_cache.set(
            HISTORICAL_TREND_CACHE_KEY, data, expire=HISTORICAL_TREND_CACHE_TTL
        )
        logger.info(
            "Minervini trend cache warm complete (%s dates)",
            data.get("total_dates", 0),
        )
        return True


def schedule_minervini_cache_warm() -> None:
    """Fire-and-forget warm after API startup (does not block requests)."""
    asyncio.create_task(warm_minervini_trend_cache())
