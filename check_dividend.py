"""
Check if stock_indicators stores price_vs_ema_10_percent directly
"""
import pandas as pd
from sqlalchemy import create_engine, text
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

with engine.connect() as conn:
    # Check what columns exist in stock_indicators
    cols = pd.read_sql(text("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'stock_indicators' AND column_name LIKE '%ema%'
        ORDER BY column_name
    """), conn)
    print("EMA-related columns in stock_indicators:")
    print(cols)

    # Check stock price history to detect any suspicious jumps (dividend cut)
    df = pd.read_sql(text("""
        SELECT date, close, 
               close - LAG(close) OVER (ORDER BY date) as day_change,
               ROUND(((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date)) * 100, 2) as pct_change
        FROM prices WHERE symbol = '1321' 
        ORDER BY date ASC
    """), conn)
    
    # Find large overnight drops (possible dividend ex-date)
    big_drops = df[df['day_change'] < -5]
    print("\nBig overnight drops for 1321 (possible dividend dates):")
    print(big_drops[['date', 'close', 'day_change', 'pct_change']].tail(10).to_string(index=False))
