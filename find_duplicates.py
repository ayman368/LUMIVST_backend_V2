"""
كشف الأيام المكررة في الداتابيز (أيام فيها نفس البيانات من اليوم السابق)
مع معرفة كم يوماً بيُحذف لكل سهم
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

with engine.connect() as conn:
    # نجيب بيانات آخر 6 شهور (منذ سبتمبر 2025)
    df = pd.read_sql(
        text("SELECT symbol, date, open, high, low, close FROM prices WHERE date >= '2025-09-01' ORDER BY symbol, date ASC"),
        conn
    )

print(f"Total rows since Sep 2025: {len(df)}")
print()

# كشف الصفوف المكررة (نفس open, high, low, close من اليوم السابق)
columns_to_check = ['open', 'high', 'low', 'close']
df_sorted = df.sort_values(['symbol', 'date'])
mask_dup = (df_sorted[columns_to_check] == df_sorted.groupby('symbol')[columns_to_check].shift(1)).all(axis=1)

duplicates = df_sorted[mask_dup][['symbol', 'date', 'open', 'high', 'low', 'close']]

print(f"Total duplicate rows found: {len(duplicates)}")
print()
print("=== Duplicate Rows (by date) ===")
print(duplicates[['symbol', 'date', 'close']].to_string(index=False))
