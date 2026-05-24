import sys
import asyncio
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.redis import redis_cache

async def clear_all_cache():
    print("🧹 Attempting to clear Redis Cache...")
    try:
        await redis_cache.flush_all()
        print("✅ Redis Cache cleared successfully!")
    except Exception as e:
        print(f"❌ Failed to clear cache: {e}")

if __name__ == "__main__":
    asyncio.run(clear_all_cache())
