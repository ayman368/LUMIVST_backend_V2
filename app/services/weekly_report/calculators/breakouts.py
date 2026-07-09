"""
breakouts.py
============
Detects price breakouts for all stocks in the current week.

Breakout types detected:
    All-Time High / Low
    1-Year (250-day) High / Low
    2-Year (500-day) High / Low
    3-Year (750-day) High / Low
    5-Year (1250-day) High / Low
    6-Year (1500-day) High / Low
    8-Year (2000-day) High / Low
    17-Year High / Low
    Positive Breakout  (close > 20-day resistance)
    Negative Breakout  (close < 20-day support)

Input columns: symbol, date, high, low, close, sector
    + optional: stock_name (falls back to symbol)

Output dict:
    {
      "summary": {
          "all_time_highs": int,
          "all_time_lows": int,
          "positive_breakouts": int,
          "negative_breakouts": int,
      },
      "breakouts": [
          {
            "symbol": str,
            "stock_name": str,
            "sector": str,
            "price": float,
            "breakout_type": str,   # e.g. "All-Time High", "1-Year Low"
            "date": str,            # ISO date of breakout
          },
          ...
      ]
    }
"""

from __future__ import annotations

import pandas as pd
from typing import Optional

# How many trading days correspond to each period label
_PERIOD_DAYS: list[tuple[str, int]] = [
    ("All-Time", None),      # None → use entire history
    ("8-Year", 2000),
    ("5-Year", 1250),
    ("3-Year", 750),
    ("2-Year", 500),
    ("1-Year", 250),
]


def _label_period(days: Optional[int]) -> str:
    for label, d in _PERIOD_DAYS:
        if d == days:
            return label
    return f"{days // 250}-Year"


def _get_period_window(history_len: int, days: Optional[int]) -> int:
    """Return the rolling window size, capped at history length."""
    if days is None:
        return history_len
    return min(days, history_len)


def _detect_breakout_type(
    grp: pd.DataFrame,
    week_dates: set,
) -> list[dict]:
    """
    For a single stock, find the most significant breakout that occurred
    during the current week. Returns a list (may be empty or multiple).
    """
    grp = grp.sort_values("date").copy()
    n = len(grp)
    if n < 2:
        return []

    results = []
    week_mask = grp["date"].isin(week_dates)
    week_rows = grp[week_mask]

    if week_rows.empty:
        return []

    for _, row in week_rows.iterrows():
        idx = grp.index.get_loc(row.name)
        history_up_to = grp.iloc[: idx + 1]
        hist_len = len(history_up_to)

        date_str = f"{row['date'].strftime('%b')} {row['date'].day}" if hasattr(row["date"], "strftime") else str(row["date"])

        # ── Check period highs / lows ─────────────────────────────────
        for label, days in _PERIOD_DAYS:
            window = _get_period_window(hist_len, days)
            if window < 5:
                continue
            window_data = history_up_to.tail(window)
            period_high = window_data["high"].max()
            period_low = window_data["low"].min()

            # Is this day's high the period high (and it's a new high)?
            if row["high"] >= period_high and hist_len >= window:
                results.append(
                    {
                        "symbol": row["symbol"],
                        "stock_name": row.get("stock_name", row["symbol"]),
                        "sector": row.get("sector", ""),
                        "price": round(float(row["close"]), 2),
                        "breakout_type": f"{label} High",
                        "date": date_str,
                        "_rank": _PERIOD_DAYS.index((label, days)),
                    }
                )
                break  # Report only most significant high

        for label, days in _PERIOD_DAYS:
            window = _get_period_window(hist_len, days)
            if window < 5:
                continue
            window_data = history_up_to.tail(window)
            period_low = window_data["low"].min()

            if row["low"] <= period_low and hist_len >= window:
                results.append(
                    {
                        "symbol": row["symbol"],
                        "stock_name": row.get("stock_name", row["symbol"]),
                        "sector": row.get("sector", ""),
                        "price": round(float(row["close"]), 2),
                        "breakout_type": f"{label} Low",
                        "date": date_str,
                        "_rank": _PERIOD_DAYS.index((label, days)),
                    }
                )
                break  # Most significant low only

        # ── Positive breakout (close > 20-day resistance) ─────────────
        if idx >= 20:
            resistance = grp.iloc[idx - 20 : idx]["high"].max()
            sma_50 = float(row.get("sma_50", 0))
            if row["close"] > resistance and row["close"] > sma_50:
                results.append(
                    {
                        "symbol": row["symbol"],
                        "stock_name": row.get("stock_name", row["symbol"]),
                        "sector": row.get("sector", ""),
                        "price": round(float(row["close"]), 2),
                        "breakout_type": "Positive Breakout",
                        "date": date_str,
                        "_rank": 99,
                    }
                )

        # ── Negative breakout (close < 20-day support) ────────────────
        if idx >= 20:
            support = grp.iloc[idx - 20 : idx]["low"].min()
            sma_50 = float(row.get("sma_50", 999999))
            if row["close"] < support and row["close"] < sma_50:
                results.append(
                    {
                        "symbol": row["symbol"],
                        "stock_name": row.get("stock_name", row["symbol"]),
                        "sector": row.get("sector", ""),
                        "price": round(float(row["close"]), 2),
                        "breakout_type": "Negative Breakout",
                        "date": date_str,
                        "_rank": 100,
                    }
                )

    # Deduplicate: keep highest-priority (lowest _rank) breakout per type category
    seen = set()
    unique = []
    for r in sorted(results, key=lambda x: x["_rank"]):
        key = (r["symbol"], "high" if "High" in r["breakout_type"] or "Positive" in r["breakout_type"] else "low")
        if key not in seen:
            seen.add(key)
            r.pop("_rank", None)
            unique.append(r)

    return unique


def compute_breakouts(df: pd.DataFrame, week_start: str, week_end: str) -> dict:
    """
    Main entry point.

    Parameters
    ----------
    df : DataFrame with all history (as many years as available)
    week_start : ISO date string "YYYY-MM-DD"
    week_end   : ISO date string "YYYY-MM-DD"

    Returns
    -------
    dict with "summary" and "breakouts" list, sorted by breakout_type then symbol.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Add stock_name fallback
    if "stock_name" not in df.columns:
        df["stock_name"] = df["symbol"]

    week_start_dt = pd.to_datetime(week_start)
    week_end_dt = pd.to_datetime(week_end)
    week_dates = set(df[(df["date"] >= week_start_dt) & (df["date"] <= week_end_dt)]["date"])

    all_breakouts: list[dict] = []
    for symbol, grp in df.groupby("symbol"):
        b = _detect_breakout_type(grp, week_dates)
        all_breakouts.extend(b)

    # Sort: All-Time first, then by symbol alphabetically
    type_order = {
        "All-Time High": 0, "All-Time Low": 1,
        "17-Year High": 2, "17-Year Low": 3,
        "8-Year High": 4, "8-Year Low": 5,
        "6-Year High": 6, "6-Year Low": 7,
        "5-Year High": 8, "5-Year Low": 9,
        "3-Year High": 10, "3-Year Low": 11,
        "2-Year High": 12, "2-Year Low": 13,
        "1-Year High": 14, "1-Year Low": 15,
        "Positive Breakout": 16,
        "Negative Breakout": 17,
    }
    all_breakouts.sort(
        key=lambda x: (type_order.get(x["breakout_type"], 99), x["stock_name"])
    )

    # Compute summary counts
    all_time_highs = sum(1 for b in all_breakouts if b["breakout_type"] == "All-Time High")
    all_time_lows = sum(1 for b in all_breakouts if b["breakout_type"] == "All-Time Low")
    positive = sum(1 for b in all_breakouts if b["breakout_type"] == "Positive Breakout")
    negative = sum(1 for b in all_breakouts if b["breakout_type"] == "Negative Breakout")

    return {
        "summary": {
            "all_time_highs": all_time_highs,
            "all_time_lows": all_time_lows,
            "positive_breakouts": positive,
            "negative_breakouts": negative,
        },
        "breakouts": all_breakouts,
    }


def compute_breakout_stock_series(df: pd.DataFrame, breakouts_list: list[dict], days: int = 250) -> list[dict]:
    """
    Computes a historical price series for each breakout stock for charting.
    """
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    result = []
    seen_symbols = set()
    
    for b in breakouts_list:
        sym = b["symbol"]
        if sym in seen_symbols:
            continue
        seen_symbols.add(sym)
        
        if days is not None:
            sym_df = df[df["symbol"] == sym].sort_values("date").tail(days)
        else:
            sym_df = df[df["symbol"] == sym].sort_values("date")
        if sym_df.empty:
            continue
            
        is_positive = "High" in b["breakout_type"] or "Positive" in b["breakout_type"]
        color = "#10b981" if is_positive else "#ef4444"
        labelBg = "rgba(16, 185, 129, 0.1)" if is_positive else "rgba(239, 68, 68, 0.1)"
        
        # Downsample to max ~300 points to avoid huge JSON and browser crash
        step = max(1, len(sym_df) // 300)
        sym_df = sym_df.iloc[::step]
        
        series = []
        for _, row in sym_df.iterrows():
            date_str = row["date"].strftime("%Y-%m-%d")
            series.append({
                "date": date_str,
                "price": round(float(row["close"]), 2)
            })
            
        result.append({
            "symbol": sym,
            "stock_name": b.get("stock_name", sym),
            "breakout_type": b["breakout_type"],
            "price": round(float(sym_df["close"].iloc[-1]), 2),
            "color": color,
            "labelBg": labelBg,
            "series": series
        })
        
    return result