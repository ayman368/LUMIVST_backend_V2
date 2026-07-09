"""Test Sector Synthetic Index approach."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.core.database import SessionLocal
from app.services.weekly_report.data_loader import load_stocks_dataframe
import pandas as pd

db = SessionLocal()
df = load_stocks_dataframe(db, week_end=__import__('datetime').date(2026, 6, 25))

ws = pd.to_datetime("2026-06-21")
we = pd.to_datetime("2026-06-25")

# Group by sector and date, sum market cap
df_clean = df.dropna(subset=["market_cap"]).copy()
# Filter out 0 market cap to avoid distortions if data is missing?
# Actually, just sum market cap per sector per date
sector_daily = df_clean.groupby(["sector", "date"])["market_cap"].sum().reset_index()
sector_daily.rename(columns={"market_cap": "close"}, inplace=True) # use market cap as the 'close' price of the index

for sector_name in ["Food & Beverages", "Energy", "Banks"]:
    sec_idx = sector_daily[sector_daily["sector"] == sector_name].sort_values("date").copy()
    
    # Calculate SMAs
    sec_idx["sma_50"] = sec_idx["close"].rolling(50, min_periods=1).mean()
    sec_idx["sma_200"] = sec_idx["close"].rolling(200, min_periods=1).mean()
    sec_idx["high_250"] = sec_idx["close"].rolling(250, min_periods=1).max()
    
    prev = sec_idx[sec_idx["date"] < ws]
    week = sec_idx[(sec_idx["date"] >= ws) & (sec_idx["date"] <= we)]
    
    if prev.empty or week.empty:
        continue
        
    start_val = prev.iloc[-1]["close"]
    end_val = week.iloc[-1]["close"]
    
    ret_pct = (end_val - start_val) / start_val * 100
    
    latest = week.iloc[-1]
    pct_below = (latest["high_250"] - latest["close"]) / latest["high_250"] * 100
    
    # Days since 250d high
    # Find the index of the max value in the last 250 days
    last_250 = sec_idx.iloc[-250:]
    max_idx = last_250["close"].idxmax()
    days_since = len(sec_idx) - 1 - sec_idx.index.get_loc(max_idx)
    
    print(f"=== {sector_name} ===")
    print(f"  Weekly Return: {ret_pct:.2f}%")
    print(f"  % Below 250d High: {pct_below:.2f}%")
    print(f"  Days Since 250d High: {days_since}")
    
    # Daily Trend
    if latest["close"] > latest["sma_50"]:
        dt = "Bull"
    elif latest["close"] < latest["sma_200"]:
        dt = "Bear"
    else:
        dt = "Neutral"
    print(f"  Daily Trend: {dt}")
