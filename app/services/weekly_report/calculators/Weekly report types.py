"""
weekly_report_types.py
======================
Pydantic v2 models for the Saudi Weekly Market Update report.
All models are JSON-serializable and match the Aporia PDF sections.
"""

from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field

TrendLabel = Literal["Bull", "Neutral", "Bear"]


# ─────────────────────────────────────────────
# Shared primitives
# ─────────────────────────────────────────────

class IndexReturn(BaseModel):
    name: str
    return_pct: float = Field(..., alias="return")

    model_config = {"populate_by_name": True}


class TrendBadges(BaseModel):
    daily: TrendLabel
    weekly: TrendLabel
    monthly: TrendLabel


# ─────────────────────────────────────────────
# Section 1 — Index Performance
# ─────────────────────────────────────────────

class IndexPerformance(BaseModel):
    market_indices: list[IndexReturn]
    market_cap_indices: list[IndexReturn]
    global_indices: list[IndexReturn]


# ─────────────────────────────────────────────
# Section 2 — Trend Analysis (TASI chart series)
# ─────────────────────────────────────────────

class TrendDataPoint(BaseModel):
    date: str            # ISO "YYYY-MM-DD"
    close: float
    trend: TrendLabel
    high_250: float
    low_250: float


class TrendAnalysis(BaseModel):
    series: list[TrendDataPoint]
    current_close: float
    high_250: float
    low_250: float
    daily: TrendLabel
    weekly: TrendLabel
    monthly: TrendLabel


# ─────────────────────────────────────────────
# Section 3 — Weekly Volume
# ─────────────────────────────────────────────

class VolumeDataPoint(BaseModel):
    date: str
    volume: float        # millions
    index_level: float


class VolumeSection(BaseModel):
    current_week_millions: float
    prev_week_millions: float
    pct_change: float
    current_index_level: float
    series: list[VolumeDataPoint]


# ─────────────────────────────────────────────
# Section 4 — Sector Analytics
# ─────────────────────────────────────────────

class SectorRow(BaseModel):
    sector: str
    weekly_return: float
    trend_daily: TrendLabel
    trend_weekly: TrendLabel
    trend_monthly: TrendLabel
    trend_rank: int
    pct_below_250d_high: float
    days_since_250d_high: int


# ─────────────────────────────────────────────
# Section 5 — Trend Breadth
# ─────────────────────────────────────────────

class BreadthDataPoint(BaseModel):
    date: str
    breadth: int         # positive = net bullish, negative = net bearish


class BreadthSection(BaseModel):
    daily: list[BreadthDataPoint]
    weekly: list[BreadthDataPoint]
    monthly: list[BreadthDataPoint]
    current: dict[str, int]   # {"daily": -34, "weekly": -134, "monthly": -118}


# ─────────────────────────────────────────────
# Section 6 — New Highs & Lows
# ─────────────────────────────────────────────

class NewHighLowPoint(BaseModel):
    date: str
    pct_new_highs: float
    pct_new_lows: float
    n_new_highs: int
    n_new_lows: int
    total_stocks: int
    close: float | None


class NewHighsLowsSection(BaseModel):
    series: list[NewHighLowPoint]
    current: dict   # {"pct_new_highs": 5.1, "pct_new_lows": 8.8, ...}


# ─────────────────────────────────────────────
# Section 7 — Stock Performance
# ─────────────────────────────────────────────

class StockReturn(BaseModel):
    symbol: str
    stock_name: str
    return_pct: float = Field(..., alias="return")

    model_config = {"populate_by_name": True}


class StockPerformanceSection(BaseModel):
    positive_count: int
    negative_count: int
    mean_return: float
    returns: list[StockReturn]


# ─────────────────────────────────────────────
# Section 8 — Top Market Cap Analytics (stock-level table)
# ─────────────────────────────────────────────

class StockRow(BaseModel):
    symbol: str
    stock_name: str
    sector: str
    weekly_return: Optional[float] = None
    trend_daily: TrendLabel
    trend_weekly: TrendLabel
    trend_monthly: TrendLabel
    trend_rank: int
    pct_below_250d_high: Optional[float] = None
    days_since_250d_high: int


# ─────────────────────────────────────────────
# Section 9 — Price Breakouts
# ─────────────────────────────────────────────

class BreakoutSummary(BaseModel):
    all_time_highs: int
    all_time_lows: int
    positive_breakouts: int
    negative_breakouts: int


class BreakoutRow(BaseModel):
    symbol: str
    stock_name: str
    sector: str
    price: float
    breakout_type: str   # "All-Time High", "1-Year Low", "Positive Breakout", etc.
    date: str            # e.g. "Jun 7"


class BreakoutsSection(BaseModel):
    summary: BreakoutSummary
    breakouts: list[BreakoutRow]


class BreakoutStockSeries(BaseModel):
    date: str
    price: float


class BreakoutStock(BaseModel):
    symbol: str
    stock_name: str
    breakout_type: str
    price: float
    color: str
    labelBg: str
    series: list[BreakoutStockSeries]


# ─────────────────────────────────────────────
# Section 10 — Top / Bottom Ranked Stocks
# ─────────────────────────────────────────────

class RankedSection(BaseModel):
    top_15: list[StockRow]
    bottom_15: list[StockRow]


# ─────────────────────────────────────────────
# Section 11 — Volume Gainers
# ─────────────────────────────────────────────

class VolumeGainerRow(BaseModel):
    symbol: str
    stock_name: str
    volume_pct_change: float
    current_week_vol: int
    prev_week_vol: int


# ─────────────────────────────────────────────
# Root — Full Report
# ─────────────────────────────────────────────

class WeeklyReport(BaseModel):
    week_label: str                              # "Week 25: Jun 7 - Jun 11, 2026"
    week_start: str
    week_end: str
    generated_at: str                            # ISO datetime

    index_performance: IndexPerformance
    trend_analysis: TrendAnalysis
    volume: VolumeSection
    sector_analytics: list[SectorRow]
    trend_breadth: BreadthSection
    new_highs_lows: NewHighsLowsSection
    stock_performance: StockPerformanceSection
    top_market_cap: list[StockRow]
    breakouts: BreakoutsSection
    breakout_stocks: list[BreakoutStock]
    top_ranked: list[StockRow]
    bottom_ranked: list[StockRow]
    volume_gainers: list[VolumeGainerRow]