import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

with engine.connect() as conn:
    df_prices = pd.read_sql(text("SELECT symbol, date, close, ema_10 as p_ema10, ema_21 as p_ema21, price_vs_ema_10_percent as p_ema10_pct, price_vs_ema_21_percent as p_ema21_pct FROM prices WHERE date = '2026-03-09' AND symbol = '1321'"), conn)
    print('PRICES TABLE:')
    print(df_prices)

    df_ind = pd.read_sql(text("SELECT symbol, date, close, ema10 as i_ema10, ema21 as i_ema21 FROM stock_indicators WHERE date = '2026-03-09' AND symbol = '1321'"), conn)
    print('\nSTOCK INDICATORS:')
    print(df_ind)
