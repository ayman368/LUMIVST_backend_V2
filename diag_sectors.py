"""Diagnostic: Food & Beverages detail + MCW comparison."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.core.database import SessionLocal
from app.services.weekly_report.data_loader import load_stocks_dataframe
import pandas as pd

db = SessionLocal()
df = load_stocks_dataframe(db, week_end=__import__('datetime').date(2026, 6, 25))

ws = pd.to_datetime("2026-06-21")
we = pd.to_datetime("2026-06-25")

for sector_name in ["Food & Beverages", "Energy", "Banks"]:
    sec = df[df["sector"] == sector_name]
    symbols = sec["symbol"].unique()
    
    prev = sec[sec["date"] < ws]
    week = sec[(sec["date"] >= ws) & (sec["date"] <= we)]
    
    sym_start = prev.sort_values("date").groupby("symbol")["close"].last()
    sym_end = week.sort_values("date").groupby("symbol")["close"].last()
    sym_mktcap = week.groupby("symbol")["market_cap"].mean()
    
    ret = pd.DataFrame({"start": sym_start, "end": sym_end, "mktcap": sym_mktcap}).dropna()
    ret["ret_pct"] = (ret["end"] - ret["start"]) / ret["start"] * 100
    
    # Market-cap weighted
    ret["weight"] = ret["mktcap"] / ret["mktcap"].sum()
    mcw = (ret["ret_pct"] * ret["weight"]).sum()
    
    print(f"\n=== {sector_name} ({len(ret)} stocks) ===")
    print(f"  Simple avg: {ret['ret_pct'].mean():.2f}%")
    print(f"  MCW avg:    {mcw:.2f}%")
    print(f"  {'sym':<8} {'start':>10} {'end':>10} {'ret%':>8} {'mktcap':>15} {'weight':>8}")
    for sym, row in ret.sort_values("mktcap", ascending=False).iterrows():
        print(f"  {sym:<8} {row['start']:>10.2f} {row['end']:>10.2f} {row['ret_pct']:>8.2f} {row['mktcap']:>15.0f} {row['weight']:>8.2%}")
