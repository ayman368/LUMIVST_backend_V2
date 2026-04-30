import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def rename_table():
    try:
        from app.core.database import engine
        
        with engine.begin() as conn:
            # Check if old table exists
            try:
                conn.execute(text("SELECT 1 FROM sofr_futures LIMIT 1"))
                logger.info("⚙️ Renaming table 'sofr_futures' to 'eurodollar_futures'...")
                conn.execute(text("ALTER TABLE sofr_futures RENAME TO eurodollar_futures"))
                logger.info("✅ Table renamed successfully!")
            except Exception:
                logger.info("✅ Table 'sofr_futures' does not exist or already renamed.")
                
    except Exception as e:
        logger.error(f"❌ Error updating database schema: {e}")

if __name__ == "__main__":
    rename_table()
