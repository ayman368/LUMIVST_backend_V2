"""
index_summary.py
================
Computes the index-level weekly summary:
    - Weekly % return for each index / market-cap group
    - Total weekly traded volume + % change vs previous week
    - Stock performance distribution (positive / negative counts)
    - Top market-cap stock weekly returns

Input columns: symbol, date, open, high, low, close, volume,
               sector, market_cap

Additional dicts for index-level data (scraped externally):
    global_indices: {"MSCI ACWI": float, "MSCI Emerging Markets": float}
    tasi_market_cap_groups: {"Large Cap": [...symbols...], ...}

Output:
    {
        "week_label": str,
        "index_performance": {
            "market_indices": [
                {"name": str, "return": float},
                ...
            ],
            "market_cap_indices": [...],
            "global_indices": [...],
        },
        "volume": {
            "current_week": float,
            "prev_week": float,
            "pct_change": float,
            "current_index_level": float,
            "series": [{"date": str, "volume": float, "index_level": float}, ...]
        },
        "stock_performance": {
            "positive_count": int,
            "negative_count": int,
            "mean_return": float,
            "returns": [{"symbol": str, "stock_name": str, "return": float}, ...]
        }
    }
"""

from __future__ import annotations

import math
import pandas as pd
from datetime import datetime


def _weighted_index_return(df: pd.DataFrame, week_start_dt: pd.Timestamp, week_end_dt: pd.Timestamp, symbols: list[str] | None = None) -> float:
    """Market-cap weighted return for a basket of symbols over the week."""
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    
    prev_df = df[df["date"] < week_start_dt]
    week_df = df[(df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)]
    
    if prev_df.empty or week_df.empty:
        return 0.0

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
    sym_mktcap = week_df.groupby("symbol")["market_cap"].mean()

    sym_df = pd.concat([sym_start, sym_end, sym_mktcap], axis=1).dropna()
    if sym_df.empty:
        return 0.0

    sym_df["weight"] = sym_df["market_cap"] / sym_df["market_cap"].sum()
    sym_df["return"] = (sym_df["close_end"] - sym_df["close_start"]) / sym_df["close_start"]

    weighted = (sym_df["return"] * sym_df["weight"]).sum()

    return round(float(weighted * 100), 2)


def compute_index_summary(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    global_indices: dict | None = None,
    tasi_market_cap_groups: dict | None = None,
    msci30_symbols: list[str] | None = None,
    tasi50_symbols: list[str] | None = None,
    tasi_return: float | None = None,
    df_tasi: pd.DataFrame | None = None,
    total_market_vol: float = 0.0,
    prev_market_vol: float = 0.0,
) -> dict:
    """
    Parameters
    ----------
    df                    : full historical DataFrame
    week_start            : ISO "YYYY-MM-DD"
    week_end              : ISO "YYYY-MM-DD"
    global_indices        : {"MSCI ACWI": -2.54, "MSCI Emerging Markets": -4.99}
    tasi_market_cap_groups: {"Large Cap": [symbols], "Medium Cap": [...], "Small Cap": [...]}
    msci30_symbols        : list of symbols in MSCI Tadawul 30
    tasi50_symbols        : list of symbols in TASI 50
    tasi_return           : precomputed TASI return
    df_tasi               : TASI index historical data
    total_market_vol      : Sum of market volume for the week
    prev_market_vol       : Sum of market volume for previous week

    Returns
    -------
    Full index summary dict.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    if "stock_name" not in df.columns:
        df["stock_name"] = df["symbol"]

    week_start_dt = pd.to_datetime(week_start)
    week_end_dt = pd.to_datetime(week_end)

    week_df = df[(df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)]

    # ── Week label ────────────────────────────────────────────────────
    ws = pd.to_datetime(week_start)
    we = pd.to_datetime(week_end)
    week_num = ws.isocalendar()[1]
    week_label = (
        f"Week {week_num}: "
        f"{ws.strftime('%b')} {ws.day} - {we.strftime('%b')} {we.day}, {we.year}"
    )

    # ── TASI overall return ───────────────────────────────────────────
    if tasi_return is None:
        tasi_return = _weighted_index_return(df, week_start_dt, week_end_dt)

    # ── MSCI 30 return ────────────────────────────────────────────────
    msci30_return = (
        _weighted_index_return(df, week_start_dt, week_end_dt, msci30_symbols)
        if msci30_symbols
        else tasi_return
    )

    # ── TASI50 return ─────────────────────────────────────────────────
    tasi50_return = (
        _weighted_index_return(df, week_start_dt, week_end_dt, tasi50_symbols)
        if tasi50_symbols
        else tasi_return
    )

    # ── Market Cap Indices ────────────────────────────────────────────
    large_return = (
        _weighted_index_return(df, week_start_dt, week_end_dt, tasi_market_cap_groups.get("Large Cap"))
        if tasi_market_cap_groups else tasi_return
    )
    medium_return = (
        _weighted_index_return(df, week_start_dt, week_end_dt, tasi_market_cap_groups.get("Medium Cap"))
        if tasi_market_cap_groups else tasi_return
    )
    small_return = (
        _weighted_index_return(df, week_start_dt, week_end_dt, tasi_market_cap_groups.get("Small Cap"))
        if tasi_market_cap_groups else tasi_return
    )

    gi = global_indices or {}
    
    index_performance = {
        "market_indices": [
            {"name": "Tadawul All-Share Index (TASI)", "return": round(tasi_return, 2)},
            {"name": "MSCI Tadawul 30 Index", "return": round(msci30_return, 2)},
            {"name": "TASI50 Index", "return": round(tasi50_return, 2)},
        ],
        "market_cap_indices": [
            {"name": "Tadawul Large Cap Index", "return": round(large_return, 2)},
            {"name": "Tadawul Medium Cap Index", "return": round(medium_return, 2)},
            {"name": "Tadawul Small Cap Index", "return": round(small_return, 2)},
        ],
        "global_indices": [
            {"name": k, "return": round(v, 2)} for k, v in gi.items()
        ],
    }

    # ── Volume ────────────────────────────────────────────────────────
    if total_market_vol > 0:
        curr_vol = float(total_market_vol) / 1_000_000
        prev_vol = float(prev_market_vol) / 1_000_000
        vol_change = round((curr_vol - prev_vol) / prev_vol * 100, 1) if prev_vol > 0 else 0.0
    else:
        # Fallback if somehow volume couldn't be fetched
        curr_vol = float(week_df["volume"].sum()) / 1_000_000
        all_dates_before = df[df["date"] < week_start_dt]["date"].drop_duplicates().sort_values()
        if len(all_dates_before) >= 5:
            prev_dates = set(all_dates_before.iloc[-5:])
            prev_vol = float(df[df["date"].isin(prev_dates)]["volume"].sum()) / 1_000_000
            vol_change = round((curr_vol - prev_vol) / prev_vol * 100, 1) if prev_vol > 0 else 0.0
        else:
            prev_vol = 0.0
            vol_change = 0.0

    # Weekly volume series (group by week)
    if df_tasi is not None and not df_tasi.empty and "volume" in df_tasi.columns:
        df_tasi_copy = df_tasi.copy()
        df_tasi_copy["date"] = pd.to_datetime(df_tasi_copy["date"])
        df_tasi_copy["week_start"] = (
            df_tasi_copy["date"].dt.to_period("W-SAT").apply(lambda p: p.start_time)
        )
        vol_df = df_tasi_copy.groupby("week_start")["volume"].sum().reset_index()
        vol_df.columns = ["week_start", "volume"]
        
        tasi_weekly = (
            df_tasi_copy.sort_values("date")
            .groupby("week_start")["close"]
            .last()
            .reset_index()
        )
        tasi_weekly.columns = ["week_start", "index_level"]
        vol_series_df = vol_df.merge(tasi_weekly, on="week_start", how="left")
        vol_series_df = vol_series_df[vol_series_df["index_level"].notna() & (vol_series_df["index_level"] > 0)]
    else:
        df["week_start"] = df["date"].dt.to_period("W-SAT").apply(lambda p: p.start_time)
        vol_df = df.groupby("week_start")["volume"].sum().reset_index()
        vol_df.columns = ["week_start", "volume"]
        vol_series_df = vol_df.copy()
        vol_series_df["index_level"] = 0.0

    vol_series = [
        {
            "date": row["week_start"].strftime("%Y-%m-%d"),
            "volume": round(float(row["volume"]) / 1e6, 1),
            "index_level": round(float(row["index_level"]), 0),
        }
        for _, row in vol_series_df.iterrows()
    ]
    # Keep only the last 104 weeks (approx 2 years) for the chart
    if len(vol_series) > 104:
        vol_series = vol_series[-104:]

    if df_tasi is not None and not df_tasi.empty:
        tasi_week = df_tasi[
            (pd.to_datetime(df_tasi["date"]) >= week_start_dt) &
            (pd.to_datetime(df_tasi["date"]) <= week_end_dt)
        ]
        if not tasi_week.empty:
            current_index_level = float(
                tasi_week.sort_values("date")["close"].iloc[-1]
            )
        else:
            current_index_level = float(
                df_tasi.sort_values("date")["close"].iloc[-1]
            )
    else:
        current_index_level = 0.0
    # Removed hardcoded exact PDF values for Week 26 2026

    volume = {
        "current_week_millions": round(curr_vol, 1),
        "prev_week_millions": round(prev_vol, 1),
        "pct_change": vol_change,
        "current_index_level": round(current_index_level, 0),
        "series": vol_series,
    }

    # ── Stock performance distribution ────────────────────────────────
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
    sym_names = week_df.groupby("symbol")["stock_name"].last()

    stock_returns_df = pd.concat([sym_start, sym_end, sym_names], axis=1).dropna()
    stock_returns_df["return"] = ((stock_returns_df["close_end"] - stock_returns_df["close_start"]) / stock_returns_df["close_start"] * 100).round(2)
    stock_returns_df = stock_returns_df.sort_values("return").reset_index()
    stock_returns_df = stock_returns_df[["symbol", "stock_name", "return"]]

    positive_count = int((stock_returns_df["return"] > 0).sum())
    negative_count = int((stock_returns_df["return"] < 0).sum())
    raw_mean = stock_returns_df["return"].mean()
    mean_return = 0.0 if pd.isna(raw_mean) else round(float(raw_mean), 2)

    # Replace any remaining NaN/inf in records before serialization
    returns_records = stock_returns_df.to_dict("records")
    for rec in returns_records:
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = 0.0

    stock_performance = {
        "positive_count": positive_count,
        "negative_count": negative_count,
        "mean_return": mean_return,
        "returns": returns_records,
    }

    return {
        "week_label": week_label,
        "week_start": week_start,
        "week_end": week_end,
        "index_performance": index_performance,
        "volume": volume,
        "stock_performance": stock_performance,
    }