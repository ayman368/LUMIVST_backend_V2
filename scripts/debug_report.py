"""Quick debug: show what's ACTUALLY in the DB report vs what the code would compute now."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.models.weekly_market_report import WeeklyMarketReport
from sqlalchemy import desc

db = SessionLocal()

# 1) What's stored in the DB right now?
row = db.query(WeeklyMarketReport).order_by(desc(WeeklyMarketReport.week_end)).first()
if not row:
    print("NO REPORT IN DB"); sys.exit(1)

report = row.report_data
print("=" * 60)
print(f"REPORT IN DB  id={row.id}  week_end={row.week_end}  generated_at={row.generated_at}")
print("=" * 60)

# Top market cap
top = report.get("top_market_cap", [])
print(f"\n--- Top Market Cap ({len(top)} stocks) ---")
for s in top[:5]:
    print(f"  {s['symbol']:6s} {s.get('stock_name','')[:20]:20s}  days_since={s.get('days_since_250d_high')}  pct_below={s.get('pct_below_250d_high')}  return={s.get('weekly_return')}")

# Sector analytics
sectors = report.get("sector_analytics", [])
print(f"\n--- Sector Analytics ({len(sectors)} sectors) ---")
for s in sectors[:5]:
    print(f"  {s['sector'][:25]:25s}  days_since={s.get('days_since_250d_high')}  pct_below={s.get('pct_below_250d_high')}  return={s.get('weekly_return')}")

# Breakouts
brk = report.get("breakouts", {})
print(f"\n--- Breakouts ---")
print(f"  total={len(brk.get('breakouts', []))}  positive={brk.get('positive_count',0)}  negative={brk.get('negative_count',0)}")

# Stock performance
sp = report.get("stock_performance", {})
print(f"\n--- Stock Performance ---")
print(f"  positive={sp.get('positive_count')}  negative={sp.get('negative_count')}  mean_return={sp.get('mean_return')}")

# Now compute FRESH from the code
print("\n" + "=" * 60)
print("COMPUTING FRESH REPORT FROM CODE...")
print("=" * 60)

from app.services.weekly_report.data_loader import load_stocks_dataframe, trading_week_bounds
import pandas as pd
from datetime import date

week_start, week_end = trading_week_bounds(row.week_end)
print(f"Week: {week_start} -> {week_end}")

df = load_stocks_dataframe(db, week_end)
print(f"DataFrame: {len(df)} rows, {df['symbol'].nunique()} symbols")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")

# Check Al Rajhi specifically
rajhi = df[df["symbol"] == "1120"]
print(f"\nAl Rajhi (1120): {len(rajhi)} rows, date range: {rajhi['date'].min()} to {rajhi['date'].max()}")

# Compute days_since for Al Rajhi
from app.services.weekly_report.calculators.sector_analytics import _days_since_250d_high
rajhi_days = _days_since_250d_high(rajhi)
print(f"Al Rajhi _days_since_250d_high (fresh calc) = {rajhi_days}")

db.close()
print("\nDone.")
