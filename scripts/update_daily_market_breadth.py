import logging
from sqlalchemy import text
from datetime import date

logger = logging.getLogger(__name__)

def update_todays_market_breadth(db, target_date: date):
    """
    Calculates today's market breadth by aggregating the newly calculated 
    data from stock_indicators, and UPSERTS it into market_breadth efficiently.
    """
    try:
        logger.info(f"📊 Aggregating Market Breadth for {target_date}...")
        
        query = text("""
            INSERT INTO market_breadth (date, pct_above_20, pct_above_50, pct_above_100, pct_above_200)
            SELECT 
                :target_date,
                ROUND(SUM(CASE WHEN close > sma_20 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(id), 0) * 100, 2),
                ROUND(SUM(CASE WHEN close > sma_50 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(id), 0) * 100, 2),
                ROUND(SUM(CASE WHEN close > sma_100 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(id), 0) * 100, 2),
                ROUND(SUM(CASE WHEN close > sma_200 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(id), 0) * 100, 2)
            FROM stock_indicators
            WHERE date = :target_date
              AND close IS NOT NULL
              AND is_etf_or_index = FALSE
            ON CONFLICT (date) DO UPDATE SET
                pct_above_20 = EXCLUDED.pct_above_20,
                pct_above_50 = EXCLUDED.pct_above_50,
                pct_above_100 = EXCLUDED.pct_above_100,
                pct_above_200 = EXCLUDED.pct_above_200;
        """)
        
        db.execute(query, {"target_date": target_date})
        db.commit()
        logger.info(f"✅ Market Breadth successfully updated for {target_date}.")
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Failed to aggregate Market Breadth for {target_date}: {e}")
