"""
ranking.py
==========
Scores and ranks all stocks by trend strength.

Scoring logic (matching Aporia's "Trend Rank"):
    Daily  Bull=+2 / Neutral=0 / Bear=-2
    Weekly Bull=+3 / Neutral=0 / Bear=-3
    Monthly Bull=+5 / Neutral=0 / Bear=-5

    Base score range: -10 to +10
    Bonus: +1 if % below 250d high < 5%  (stock near its high)
    Penalty: -1 if days_since_250d_high > 500

Final rank: 1 = best (highest score), N = worst.

Input columns: symbol, date, close, sma_50, sma_200,
               close_w, sma9_w, sma_trend_weekly,
               percent_off_52w_high
    + optional: stock_name, sector, market_cap

Output:
    {
        "ranked_stocks": [
            {
                "symbol": str,
                "stock_name": str,
                "sector": str,
                "weekly_return": float,
                "trend_daily": str,
                "trend_weekly": str,
                "trend_monthly": str,
                "trend_rank": int,
                "score": float,
                "pct_below_250d_high": float,
                "days_since_250d_high": int,
            },
            ...
        ],   # sorted by rank ascending (rank 1 first)
        "top_15": [...],    # first 15
        "bottom_15": [...], # last 15 (reversed so worst is last)
    }
"""

from __future__ import annotations

import pandas as pd
from .trend_direction import _daily_trend, _weekly_trend, _monthly_trend
from .sector_analytics import _days_since_250d_high

_DAILY_SCORE = {"Bull": 2, "Neutral": 0, "Bear": -2}
_WEEKLY_SCORE = {"Bull": 3, "Neutral": 0, "Bear": -3}
_MONTHLY_SCORE = {"Bull": 5, "Neutral": 0, "Bear": -5}


def _score(daily: str, weekly: str, monthly: str,
           pct_below: float, days_since: int) -> float:
    s = (
        _DAILY_SCORE.get(daily, 0)
        + _WEEKLY_SCORE.get(weekly, 0)
        + _MONTHLY_SCORE.get(monthly, 0)
    )
    
    # Bonus / penalty
    if not pd.isna(pct_below) and pct_below < 5.0:
        s += 1
    if days_since > 500:
        s -= 1
        
    return float(s)


def compute_rankings(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
) -> dict:
    """
    Parameters
    ----------
    df         : full historical DataFrame
    week_start : ISO "YYYY-MM-DD"
    week_end   : ISO "YYYY-MM-DD"

    Returns
    -------
    dict with "ranked_stocks", "top_15", "bottom_15".
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    if "stock_name" not in df.columns:
        df["stock_name"] = df["symbol"]
    if "sector" not in df.columns:
        df["sector"] = "Unknown"

    week_start_dt = pd.to_datetime(week_start)
    week_end_dt = pd.to_datetime(week_end)

    # ── Per-symbol weekly return ──────────────────────────────────────
    week_df = df[(df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)]
    prev_df = df[df["date"] < week_start_dt]
    sym_start = (
        prev_df.sort_values("date")
        .groupby("symbol")["close"]
        .last()
        .rename("close_start")
    )
    sym_end = (
        week_df.sort_values("date")
        .groupby("symbol")["close"]
        .last()
        .rename("close_end")
    )
    weekly_return = (
        ((sym_end - sym_start) / sym_start * 100)
        .rename("weekly_return")
    )

    # ── Latest values per symbol ──────────────────────────────────────
    latest = df.sort_values("date").groupby("symbol").last().reset_index()

    results = []
    for _, row in latest.iterrows():
        sym = row["symbol"]
        sym_grp = df[df["symbol"] == sym]

        daily = _daily_trend(row["close"], row["sma_50"], row["sma_200"])
        weekly = _weekly_trend(row["close_w"], row["sma9_w"], row["sma_trend_weekly"])
        monthly = _monthly_trend(sym_grp)

        pct_below = float(row["percent_off_52w_high"]) if not pd.isna(row.get("percent_off_52w_high")) else float("nan")
        days_since = _days_since_250d_high(sym_grp)

        sc = _score(daily, weekly, monthly, pct_below, days_since)

        wr = float(weekly_return.get(sym, float("nan")))

        results.append(
            {
                "symbol": sym,
                "stock_name": row.get("stock_name", sym),
                "sector": row.get("sector", ""),
                "market_cap": row.get("market_cap", 0.0),
                "weekly_return": round(wr, 2) if not pd.isna(wr) else None,
                "trend_daily": daily,
                "trend_weekly": weekly,
                "trend_monthly": monthly,
                "score": sc,
                "pct_below_250d_high": round(pct_below, 2) if not pd.isna(pct_below) else None,
                "days_since_250d_high": days_since,
            }
        )

    # Sort by score descending, then by symbol ascending for tie-breaking
    results.sort(key=lambda x: (-x["score"], x["symbol"]))

    # Assign rank
    for i, r in enumerate(results, 1):
        r["trend_rank"] = i

    top_15 = results[:15]
    bottom_15 = [x for x in results[-15:] if x not in top_15]
    bottom_15.reverse()

    return {
        "ranked_stocks": results,
        "top_15": top_15,
        "bottom_15": bottom_15,
    }