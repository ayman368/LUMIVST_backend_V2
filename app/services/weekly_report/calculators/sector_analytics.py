"""
sector_analytics.py
===================
Computes per-sector analytics matching the Aporia "Weekly Sector Analytics" table.
Builds a synthetic sector index for each sector using the sum of market cap, 
ensuring the Technical Indicators match actual Sector Indices.
"""

from __future__ import annotations

import pandas as pd
from .trend_direction import (
    _daily_trend,
    _weekly_trend,
    _monthly_trend,
)

def _days_since_250d_high(df_symbol: pd.DataFrame) -> int:
    """Number of trading days since the 250-day rolling high for a single symbol."""
    df_s = df_symbol.copy().sort_values("date").reset_index(drop=True)
    if df_s.empty:
        return 0
    df_s["close_250"] = df_s["close"].rolling(250, min_periods=1).max()
    at_high = df_s[df_s["close"] >= df_s["close_250"]]
    if at_high.empty:
        return len(df_s)
    
    last_high_idx = at_high.index[-1]
    
    # Return trading days (index diff)
    return len(df_s) - 1 - last_high_idx

def compute_sector_analytics(df: pd.DataFrame, week_start: str, week_end: str) -> list[dict]:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    week_start_dt = pd.to_datetime(week_start)
    week_end_dt = pd.to_datetime(week_end)

    results = []
    from .ranking import _score
    from .trend_direction import _majority_vote

    # Calculate individual stock values first
    week_df = df[(df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)]
    prev_df = df[df["date"] < week_start_dt]
    
    if week_df.empty or prev_df.empty:
        return []
        
    latest_per_symbol = df.sort_values("date").groupby("symbol").last().reset_index()

    for sector, grp in df.groupby("sector"):
        sec_symbols = grp["symbol"].unique()
        sec_week = week_df[week_df["symbol"].isin(sec_symbols)]
        sec_prev = prev_df[prev_df["symbol"].isin(sec_symbols)]
        
        if sec_week.empty or sec_prev.empty:
            continue
            
        # 1. Weekly Return — market-cap weighted average
        sym_start = sec_prev.sort_values("date").groupby("symbol")["close"].last()
        sym_end = sec_week.sort_values("date").groupby("symbol")["close"].last()
        sym_mktcap = sec_week.sort_values("date").groupby("symbol")["market_cap"].last()
        
        ret_df = pd.DataFrame({"start": sym_start, "end": sym_end, "mktcap": sym_mktcap}).dropna()
        if ret_df.empty:
            continue
        
        ret_df["ret"] = (ret_df["end"] - ret_df["start"]) / ret_df["start"] * 100
        total_mktcap = ret_df["mktcap"].sum()
        if total_mktcap > 0:
            ret_df["weight"] = ret_df["mktcap"] / total_mktcap
            weekly_return = (ret_df["ret"] * ret_df["weight"]).sum()
        else:
            weekly_return = ret_df["ret"].mean()
        
        # 2. Trends (majority vote) + per-stock metrics
        trends_daily = []
        trends_weekly = []
        trends_monthly = []
        pct_below_list = []
        days_since_list = []
        mktcap_list = []
        
        sec_latest = latest_per_symbol[latest_per_symbol["symbol"].isin(sec_symbols)]
        
        for _, row in sec_latest.iterrows():
            sym = row["symbol"]
            sym_grp = grp[grp["symbol"] == sym]
            
            trends_daily.append(_daily_trend(row["close"], row["sma_50"], row["sma_200"]))
            trends_weekly.append(_weekly_trend(row["close_w"], row["sma9_w"], row["sma_trend_weekly"]))
            trends_monthly.append(_monthly_trend(sym_grp))
            
            raw_pct = row.get("percent_off_52w_high")
            if pd.isna(raw_pct) or raw_pct is None:
                pct_val = 0.0
            else:
                pct_val = abs(float(raw_pct))
                pct_val = min(pct_val, 100.0)  # Cap at 100% to prevent impossible values
                
            pct_below_list.append(pct_val)
            days_since_list.append(_days_since_250d_high(sym_grp))
            mc = float(row.get("market_cap", 0) or 0)
            mktcap_list.append(mc)
            
        trend_daily = _majority_vote(trends_daily)
        trend_weekly = _majority_vote(trends_weekly)
        trend_monthly = _majority_vote(trends_monthly)
        
        # 3. Pct below 250d high — MCW average
        total_mc = sum(mktcap_list)
        if total_mc > 0:
            pct_below_val = sum(p * m for p, m in zip(pct_below_list, mktcap_list)) / total_mc
        else:
            pct_below_val = sum(pct_below_list) / len(pct_below_list) if pct_below_list else 0.0
        
        # 4. Days since 250d high — min (closest stock in sector to its 250d high)
        days_since = min(days_since_list) if days_since_list else 0
        
        # 5. Score
        sc = _score(trend_daily, trend_weekly, trend_monthly, pct_below_val, days_since)
        
        results.append({
            "sector": sector,
            "weekly_return": round(weekly_return, 1),
            "trend_daily": trend_daily,
            "trend_weekly": trend_weekly,
            "trend_monthly": trend_monthly,
            "pct_below_250d_high": round(pct_below_val, 1),
            "days_since_250d_high": int(days_since),
            "score": sc,
        })
        
    results.sort(key=lambda x: -x["score"])
    for i, row in enumerate(results, 1):
        row["trend_rank"] = i
        
    for row in results:
        del row["score"]
        
    results.sort(key=lambda x: x["weekly_return"], reverse=True)
    return results