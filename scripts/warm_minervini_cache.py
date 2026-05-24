"""
OPTIONAL — for developers only. Users never run this.

The API warms this cache automatically on startup and after daily_market_update.
Use this script only if you want to refresh Redis manually without restarting uvicorn.

Usage: python scripts/warm_minervini_cache.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.redis import redis_cache
from app.services.minervini_cache import warm_minervini_trend_cache


async def main():
    await redis_cache.init_redis()
    await warm_minervini_trend_cache(force=True)
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
