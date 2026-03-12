"""
نفس الاسكريبت - بيطبع آخر 15 سعر لسهم 1321 ويحسب EMA10 خطوة بخطوة
لمقارنته مع TradingView
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

SYMBOL = '1321'
TARGET_DATE = '2026-03-09'

with engine.connect() as conn:
    df = pd.read_sql(
        text(f"SELECT date, open, high, low, close FROM prices WHERE symbol='{SYMBOL}' ORDER BY date ASC"),
        conn
    )

print(f"Total rows: {len(df)}")
print()

# ===================== Holiday Filter (same as indicators_data_service) =====================
df_raw = df.copy()
columns_to_check = ['open', 'high', 'low', 'close']
mask = (df[columns_to_check] != df[columns_to_check].shift(1)).any(axis=1)
df_filtered = df[mask].copy()

print(f"With holiday filter: {len(df_filtered)} rows (removed {len(df_raw) - len(df_filtered)} holiday rows)")
print()

# ===================== Print last 20 close prices =====================
print("=== Last 20 Close Prices (unfiltered) ===")
print(df_raw.tail(20)[['date', 'close']].to_string(index=False))
print()

print("=== Last 20 Close Prices (WITH holiday filter) ===")
print(df_filtered.tail(20)[['date', 'close']].to_string(index=False))
print()

# ===================== Manual EMA10 calculation (last 20 steps) =====================
def calc_ema_step_by_step(df, period, label):
    import numpy as np
    closes = df['close'].tolist()
    dates = df['date'].tolist()
    alpha = 2 / (period + 1)
    
    # Start with SMA of first `period` closes
    ema = np.mean(closes[:period])
    
    ema_history = [(None, None)] * period  # (date, ema) for first period (no EMA yet)
    ema_history[period-1] = (dates[period-1], ema)
    
    for i in range(period, len(closes)):
        ema = closes[i] * alpha + ema * (1 - alpha)
        ema_history.append((dates[i], ema))

    print(f"=== {label} EMA{period} — Last 20 Steps ===")
    for d, e in ema_history[-20:]:
        if e is not None:
            daily_close_idx = [str(dd)[:10] for dd in dates].index(str(d)[:10])
            close_val = closes[daily_close_idx]
            pct = (close_val - e) / e * 100
            print(f"  {str(d)[:10]}  close={close_val:.4f}  EMA{period}={e:.4f}  pct={pct:.3f}%")
    print()

calc_ema_step_by_step(df_raw, 10, "NO FILTER")
calc_ema_step_by_step(df_filtered, 10, "WITH HOLIDAY FILTER")

print("=" * 60)
print(f"Expected TradingView: EMA10 gives -1.2% → EMA10 ≈ 138.16")
print(f"Our result should be compared to TradingView's chart for {SYMBOL}")
