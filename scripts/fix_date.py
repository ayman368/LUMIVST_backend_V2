import sys
from pathlib import Path
import asyncio
from datetime import date

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from app.core.database import SessionLocal
from app.core.cache_helpers import invalidate_all_caches

def fix_date(target_date="2026-04-29"):
    db = SessionLocal()
    try:
        print(f"🔄 Updating latest_ready_date to {target_date}...")
        db.execute(text("""
            UPDATE update_status 
            SET latest_ready_date = :market_date, 
                is_updating = FALSE, 
                completed_at = CURRENT_TIMESTAMP
            WHERE id = 1
        """), {"market_date": target_date})
        db.commit()
        print("✅ Date updated in database successfully!")
        
        print("🧹 Clearing application caches...")
        asyncio.run(invalidate_all_caches())
        print("✅ Caches cleared successfully! The website should now show the new data.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        fix_date(sys.argv[1])
    else:
        fix_date()
