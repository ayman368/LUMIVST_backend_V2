"""
volume_gainers.py
=================
Computes the "Weekly Volume Gainers" table — stocks with the largest
percentage increase in 5-day (current week) average volume vs the
previous 5-day period.

Input columns: symbol, date, volume
    + optional: stock_name

Output:
    list of dicts (top N, sorted descending by volume_pct_change):
    {
        "symbol": str,
        "stock_name": str,
        "volume_pct_change": float,   # 5-day volume % change
        "current_week_vol": float,    # total volume this week
        "prev_week_vol": float,       # total volume previous week
    }
"""

from __future__ import annotations

import pandas as pd


def compute_volume_gainers(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    top_n: int = 40,
) -> list[dict]:
    """
    Parameters
    ----------
    df         : DataFrame with daily data (at least 2 weeks of history)
    week_start : ISO date "YYYY-MM-DD"
    week_end   : ISO date "YYYY-MM-DD"
    top_n      : how many gainers to return (default 40, matching Aporia)

    Returns
    -------
    List of top_n gainers, sorted by volume_pct_change descending.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    if "stock_name" not in df.columns:
        df["stock_name"] = df["symbol"]

    week_start_dt = pd.to_datetime(week_start)
    week_end_dt = pd.to_datetime(week_end)

    # ── Current week volume ──────────────────────────────────────────
    curr_mask = (df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)
    curr_vol = (
        df[curr_mask]
        .groupby("symbol")
        .agg(
            current_week_vol=("volume", "sum"),
            stock_name=("stock_name", "last"),
        )
        .reset_index()
    )

    # ── Previous week volume (5 trading days before week_start) ──────
    # Find all unique trading dates before week_start, take last 5
    all_dates_before = df[df["date"] < week_start_dt]["date"].drop_duplicates().sort_values()
    if len(all_dates_before) < 5:
        # Not enough history — return empty
        return []

    prev_week_dates = set(all_dates_before.iloc[-5:])
    prev_mask = df["date"].isin(prev_week_dates)
    prev_vol = (
        df[prev_mask]
        .groupby("symbol")
        .agg(prev_week_vol=("volume", "sum"))
        .reset_index()
    )

    # ── Merge and compute % change ───────────────────────────────────
    merged = curr_vol.merge(prev_vol, on="symbol", how="inner")

    # Avoid division by zero
    merged = merged[merged["prev_week_vol"] > 0].copy()
    merged["volume_pct_change"] = (
        (merged["current_week_vol"] - merged["prev_week_vol"])
        / merged["prev_week_vol"]
        * 100
    ).round(1)

    # Sort and take top N
    merged = merged.sort_values("volume_pct_change", ascending=False).head(top_n)

    top_symbols = merged["symbol"].tolist()
    
    # ── Fetch historical weekly volumes for these top symbols ───────────
    history_df = df[df["symbol"].isin(top_symbols)].copy()
    history_df["week_start"] = history_df["date"].dt.to_period("W-SAT").apply(lambda p: p.start_time)
    
    weekly_stats = (
        history_df.groupby(["symbol", "week_start"])
        .agg(volume=("volume", "sum"), price=("close", "last"))
        .reset_index()
    )
    weekly_stats = weekly_stats.sort_values(["symbol", "week_start"])
    
    # Get last 12 weeks for each symbol
    series_dict = {}
    for sym, group in weekly_stats.groupby("symbol"):
        tail_group = group.tail(12)
        sym_series = []
        for _, r in tail_group.iterrows():
            sym_series.append({
                "x": r["week_start"].strftime("%Y-%m-%d"),
                "volume": round(float(r["volume"]) / 1e6, 2),
                "price": round(float(r["price"]), 2),
            })
        series_dict[sym] = sym_series

    result = []
    for _, row in merged.iterrows():
        sym = row["symbol"]
        result.append(
            {
                "symbol": sym,
                "stock_name": row["stock_name"],
                "volume_pct_change": float(row["volume_pct_change"]),
                "current_week_vol": int(row["current_week_vol"]),
                "prev_week_vol": int(row["prev_week_vol"]),
                "series": series_dict.get(sym, [])
            }
        )

    return result