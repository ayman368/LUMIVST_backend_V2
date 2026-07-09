"""
trend_direction.py
==================
Computes Bull / Bear / Neutral trend for each stock on daily, weekly,
and monthly timeframes. Also produces the trend-breadth series used in
the "Trend Breadth" chart and the TASI trend-analysis series.

Input columns required in df:
    symbol, date, close, sma_50, sma_200,
    close_w, sma9_w, sma_trend_weekly

Output: dict with keys
    - stock_trends      : list[dict]   per-stock latest trends
    - tasi_trend_series : list[dict]   daily TASI close + trend label
    - breadth           : dict         daily/weekly/monthly breadth series
    - summary_labels    : dict         current daily/weekly/monthly label for index
"""

from __future__ import annotations

import pandas as pd
from typing import Literal

TrendLabel = Literal["Bull", "Neutral", "Bear"]


# ─────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────

def _daily_trend(close: float, sma_50: float, sma_200: float) -> TrendLabel:
    """Close vs SMA-50 / SMA-200."""
    if pd.isna(close) or pd.isna(sma_50) or pd.isna(sma_200):
        return "Neutral"
    if close > sma_50:
        return "Bull"
    if close < sma_200:
        return "Bear"
    return "Neutral"


def _weekly_trend(close_w: float, sma9_w: float, sma_trend_weekly: float) -> TrendLabel:
    """Weekly close vs 9-week SMA / 26-week trend SMA."""
    if pd.isna(close_w) or pd.isna(sma9_w) or pd.isna(sma_trend_weekly):
        return "Neutral"
    if close_w > sma9_w:
        return "Bull"
    if close_w < sma_trend_weekly:
        return "Bear"
    return "Neutral"


def _monthly_trend(df_symbol: pd.DataFrame) -> TrendLabel:
    """
    Monthly: resample to month-end closes, compute 12m & 24m SMA,
    return trend for the latest month.
    """
    if df_symbol.empty:
        return "Neutral"
    resample_rule = "ME" if int(pd.__version__.split(".")[0]) >= 2 else "M"
    monthly = (
        df_symbol.set_index("date")["close"]
        .resample(resample_rule)
        .last()
        .dropna()
    )
    if len(monthly) < 12:
        return "Neutral"
    sma12 = monthly.rolling(12).mean().iloc[-1]
    sma24 = monthly.rolling(24).mean().iloc[-1] if len(monthly) >= 24 else None
    last = monthly.iloc[-1]
    if last > sma12:
        return "Bull"
    if sma24 is not None and last < sma24:
        return "Bear"
    return "Neutral"


def _majority_vote(trends: list[TrendLabel]) -> TrendLabel:
    """Return the most common trend label; tie → Neutral."""
    if not trends:
        return "Neutral"
    counts = {"Bull": 0, "Bear": 0, "Neutral": 0}
    for t in trends:
        counts[t] += 1
    return max(counts, key=lambda k: counts[k])


# ─────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────

def compute_stock_trends(df: pd.DataFrame) -> list[dict]:
    """
    Per-symbol: latest daily, weekly, monthly trend.

    Returns list of dicts:
        symbol, daily, weekly, monthly
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    results = []

    for symbol, grp in df.groupby("symbol"):
        grp = grp.sort_values("date")
        latest = grp.iloc[-1]

        daily = _daily_trend(
            latest["close"], latest["sma_50"], latest["sma_200"]
        )
        weekly = _weekly_trend(
            latest["close_w"], latest["sma9_w"], latest["sma_trend_weekly"]
        )
        monthly = _monthly_trend(grp)

        results.append(
            {
                "symbol": symbol,
                "daily": daily,
                "weekly": weekly,
                "monthly": monthly,
            }
        )

    return results


def compute_tasi_trend_series(df_tasi: pd.DataFrame) -> list[dict]:
    """
    Build the line-chart series for the TASI Trend Analysis chart.
    Each row: date, close, trend_label (based on daily trend of TASI itself).

    df_tasi must be single-symbol TASI rows with date, close, sma_50, sma_200.
    Returns list of dicts: date (ISO str), close, trend
    """
    df = df_tasi.copy().sort_values("date")
    df["date"] = pd.to_datetime(df["date"])
    df["trend"] = df.apply(
        lambda r: _daily_trend(r["close"], r["sma_50"], r["sma_200"]), axis=1
    )
    # 250-day rolling high / low
    df["high_250"] = df["close"].rolling(250, min_periods=1).max()
    df["low_250"] = df["close"].rolling(250, min_periods=1).min()

    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "close": round(float(row["close"]), 2),
                "trend": row["trend"],
                "high_250": round(float(row["high_250"]), 2),
                "low_250": round(float(row["low_250"]), 2),
            }
        )
    return records


def compute_trend_breadth(df: pd.DataFrame) -> dict:
    """
    Daily / Weekly / Monthly breadth series.

    Returns:
        {
          "daily":   [{"date": "...", "breadth": int}, ...],
          "weekly":  [...],
          "monthly": [...],
          "current": {"daily": int, "weekly": int, "monthly": int}
        }
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # ── Daily breadth (per trading day) ──────────────────────────────
    df["d_trend"] = df.apply(lambda r: _daily_trend(r["close"], r["sma_50"], r["sma_200"]), axis=1)
    
    daily_bulls = (df["d_trend"] == "Bull").groupby(df["date"]).sum()
    daily_bears = (df["d_trend"] == "Bear").groupby(df["date"]).sum()
    daily_breadth = daily_bulls - daily_bears
    
    daily_records = [
        {"date": date.strftime("%Y-%m-%d"), "breadth": int(breadth)}
        for date, breadth in daily_breadth.items()
    ]
    daily_records.sort(key=lambda x: x["date"])

    # ── Weekly breadth (last day of each ISO week) ───────────────────
    df["week"] = df["date"].dt.to_period("W-SAT")
    weekly_last_day_idx = df.groupby(["week", "symbol"])["date"].idxmax()
    df_week_last = df.loc[weekly_last_day_idx].copy()
    
    df_week_last["w_trend"] = df_week_last.apply(lambda r: _weekly_trend(r["close_w"], r["sma9_w"], r["sma_trend_weekly"]), axis=1)
    
    w_bull_sum = (df_week_last["w_trend"] == "Bull").groupby(df_week_last["week"]).sum()
    w_bear_sum = (df_week_last["w_trend"] == "Bear").groupby(df_week_last["week"]).sum()
    w_dates = df_week_last.groupby("week")["date"].max()
    
    w_breadth = w_bull_sum - w_bear_sum
    weekly_records = [
        {"date": w_dates[week].strftime("%Y-%m-%d"), "breadth": int(breadth)}
        for week, breadth in w_breadth.items()
    ]
    weekly_records.sort(key=lambda x: x["date"])

    # ── Monthly breadth ──────────────────────────────────────────────
    df["month"] = df["date"].dt.to_period("M")
    monthly_closes = df.groupby(["symbol", "month"])["close"].last().reset_index()
    monthly_closes = monthly_closes.sort_values(["symbol", "month"])
    monthly_closes["sma12"] = monthly_closes.groupby("symbol")["close"].transform(lambda x: x.rolling(12, min_periods=6).mean())
    monthly_closes["sma24"] = monthly_closes.groupby("symbol")["close"].transform(lambda x: x.rolling(24, min_periods=12).mean())
    
    def _m_trend(r):
        if pd.isna(r["sma12"]): return "Neutral"
        if r["close"] > r["sma12"]: return "Bull"
        if not pd.isna(r["sma24"]) and r["close"] < r["sma24"]: return "Bear"
        return "Neutral"

    monthly_closes["m_trend"] = monthly_closes.apply(_m_trend, axis=1)
    m_bull_sum = (monthly_closes["m_trend"] == "Bull").groupby(monthly_closes["month"]).sum()
    m_bear_sum = (monthly_closes["m_trend"] == "Bear").groupby(monthly_closes["month"]).sum()
    m_dates = df.groupby("month")["date"].max()
    
    m_breadth = m_bull_sum - m_bear_sum
    monthly_records = [
        {"date": m_dates[month].strftime("%Y-%m-%d"), "breadth": int(breadth)}
        for month, breadth in m_breadth.items() if month in m_dates
    ]
    monthly_records.sort(key=lambda x: x["date"])

    current_daily = daily_records[-1]["breadth"] if daily_records else 0
    current_weekly = weekly_records[-1]["breadth"] if weekly_records else 0
    current_monthly = monthly_records[-1]["breadth"] if monthly_records else 0

    return {
        "daily": daily_records,
        "weekly": weekly_records,
        "monthly": monthly_records,
        "current": {
            "daily": current_daily,
            "weekly": current_weekly,
            "monthly": current_monthly,
        },
    }


def get_index_trend_labels(df_tasi: pd.DataFrame) -> dict:
    """
    Return current Daily / Weekly / Monthly trend label for the index itself.
    Used in the Trend Analysis chart badges (top-right).
    """
    df = df_tasi.copy().sort_values("date")
    df["date"] = pd.to_datetime(df["date"])
    latest = df.iloc[-1]

    daily = _daily_trend(latest["close"], latest["sma_50"], latest["sma_200"])
    weekly = _weekly_trend(latest["close_w"], latest["sma9_w"], latest["sma_trend_weekly"])
    monthly = _monthly_trend(df)

    high_250 = df["close"].rolling(250, min_periods=1).max().iloc[-1]
    low_250 = df["close"].rolling(250, min_periods=1).min().iloc[-1]

    return {
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
        "high_250": round(float(high_250), 2),
        "low_250": round(float(low_250), 2),
        "current_close": round(float(latest["close"]), 2),
    }