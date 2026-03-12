import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

def calc_ema_tv_real(series, period):
    vals = series.values
    ema_vals = [np.nan] * len(vals)
    alpha = 2.0 / (period + 1.0)
    
    first_valid_idx = None
    for i, v in enumerate(vals):
        if not np.isnan(v):
            first_valid_idx = i
            break
            
    if first_valid_idx is None or len(vals) - first_valid_idx < period:
        return pd.Series(ema_vals, index=series.index)
        
    start_idx = first_valid_idx
    first_ema_idx = start_idx + period - 1
    ema_vals[first_ema_idx] = np.mean(vals[start_idx : first_ema_idx + 1])
    
    for i in range(first_ema_idx + 1, len(vals)):
        ema_vals[i] = (vals[i] - ema_vals[i-1]) * alpha + ema_vals[i-1]
            
    return pd.Series(ema_vals, index=series.index)

def verify_1321():
    engine = create_engine(str(settings.DATABASE_URL))
    query = text("SELECT date, open, close, high, low FROM prices WHERE symbol = '1321' ORDER BY date ASC")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    print(f"Total rows for 1321: {len(df)}")
    
    # 1. Calculate without holiday filter
    df['ema10_raw'] = calc_ema_tv_real(df['close'], 10)
    
    # 2. Calculate with holiday filter (matching IndicatorsDataService)
    columns_to_check = ['open', 'high', 'low', 'close']
    mask = (df[columns_to_check] != df[columns_to_check].shift(1)).any(axis=1)
    df_filtered = df[mask].copy()
    print(f"Rows after holiday filter: {len(df_filtered)}")
    df_filtered['ema10_filtered'] = calc_ema_tv_real(df_filtered['close'], 10)
    
    # Merge back to see comparison on 2026-03-09
    df = df.merge(df_filtered[['ema10_filtered']], left_index=True, right_index=True, how='left')
    
    target_date = pd.to_datetime('2026-03-09')
    results = df[df['date'] == target_date]
    
    if not results.empty:
        row = results.iloc[0]
        close = row['close']
        print(f"\nDate: 2026-03-09, Close: {close}")
        
        e10_raw = row['ema10_raw']
        pct_raw = ((close - e10_raw) / e10_raw * 100) if e10_raw else 0
        print(f"EMA10 (No Filter): {e10_raw:.4f} -> {pct_raw:.2f}%")
        
        e10_filt = row['ema10_filtered']
        pct_filt = ((close - e10_filt) / e10_filt * 100) if e10_filt else 0
        print(f"EMA10 (With Filter): {e10_filt:.4f} -> {pct_filt:.2f}%")
        
        print("\nIf none of these are -1.2%, then TradingView period or calculation is still different.")

if __name__ == "__main__":
    verify_1321()
