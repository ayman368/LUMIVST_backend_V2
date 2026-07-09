"""
new_highs_lows.py
=================
Computes the "Stocks Making New Highs & New Lows" section.

For each trading date, computes:
    % of stocks making a new 250-day (52-week) high
    % of stocks making a new 250-day (52-week) low

Input columns: symbol, date, close, high, low

Output:
    {
        "series": [
            {
                "date": "YYYY-MM-DD",
                "pct_new_highs": float,
                "pct_new_lows": float,
                "n_new_highs": int,
                "n_new_lows": int,
                "total_stocks": int,
            },
            ...
        ],
        "current": {
            "pct_new_highs": float,
            "pct_new_lows": float,
            "date": str,
        }
    }
"""

from __future__ import annotations

import pandas as pd


def compute_new_highs_lows(df: pd.DataFrame, df_tasi: pd.DataFrame = None, window: int = 250) -> dict:
    """
    Parameters
    ----------
    df      : full historical DataFrame
    df_tasi : TASI dataframe to pull 'close' price
    window  : rolling window for high/low detection (default 250 = 52 weeks)

    Returns
    -------
    dict with "series" (historical) and "current" (latest values).
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    # ── Per-symbol rolling highs / lows ──────────────────────────────
    df["rolling_high"] = df.groupby("symbol")["high"].transform(
        lambda s: s.rolling(window, min_periods=max(1, window // 2)).max()
    )
    df["rolling_low"] = df.groupby("symbol")["low"].transform(
        lambda s: s.rolling(window, min_periods=max(1, window // 2)).min()
    )

    # A stock makes a new high if today's high touches the rolling max
    df["is_new_high"] = df["high"] >= df["rolling_high"]
    # A stock makes a new low if today's low touches the rolling min
    df["is_new_low"] = df["low"] <= df["rolling_low"]

    # ── Aggregate per date ────────────────────────────────────────────
    daily = (
        df.groupby("date")
        .agg(
            n_new_highs=("is_new_high", "sum"),
            n_new_lows=("is_new_low", "sum"),
            total_stocks=("symbol", "nunique"),
        )
        .reset_index()
    )

    daily["pct_new_highs"] = (
        daily["n_new_highs"] / daily["total_stocks"] * 100
    ).round(1)
    daily["pct_new_lows"] = (
        daily["n_new_lows"] / daily["total_stocks"] * 100
    ).round(1)

    if df_tasi is not None and not df_tasi.empty:
        df_tasi = df_tasi.copy()
        df_tasi["date"] = pd.to_datetime(df_tasi["date"])
        tasi_map = df_tasi.set_index("date")["close"].to_dict()
    else:
        tasi_map = {}

    series = []
    for _, row in daily.iterrows():
        date_obj = row["date"]
        date_str = date_obj.strftime("%Y-%m-%d")
        tasi_close = tasi_map.get(date_obj, None)
        
        series.append(
            {
                "date": date_str,
                "pct_new_highs": float(row["pct_new_highs"]),
                "pct_new_lows": float(row["pct_new_lows"]),
                "n_new_highs": int(row["n_new_highs"]),
                "n_new_lows": int(row["n_new_lows"]),
                "total_stocks": int(row["total_stocks"]),
                "close": round(float(tasi_close), 2) if tasi_close is not None else None,
            }
        )

    current = series[-1] if series else {}

    return {
        "series": series,
        "current": {
            "pct_new_highs": current.get("pct_new_highs", 0.0),
            "pct_new_lows": current.get("pct_new_lows", 0.0),
            "n_new_highs": current.get("n_new_highs", 0),
            "n_new_lows": current.get("n_new_lows", 0),
            "date": current.get("date", ""),
        },
    }