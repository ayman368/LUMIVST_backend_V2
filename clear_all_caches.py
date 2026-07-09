import asyncio
import sys
import os

# Add the backend directory to sys.path to resolve 'app'
sys.path.insert(0, os.path.abspath('d:/Work/LUMIVST/backend'))

from app.core.cache_helpers import invalidate_all_caches

async def main():
    print("Clearing all caches...")
    await invalidate_all_caches()
    print("Cache cleared successfully!")

if __name__ == "__main__":
    asyncio.run(main())
