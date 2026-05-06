"""
Pydantic schemas for NAAIM Exposure Index API responses.
"""

from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional, List


# ─── Base ─────────────────────────────────────────────
class NaaimExposureBase(BaseModel):
    date: date
    naaim_index: float
    sp500: Optional[float] = None
    bearish: Optional[float] = None
    quartile_1: Optional[float] = None
    quartile_2: Optional[float] = None
    quartile_3: Optional[float] = None
    bullish: Optional[float] = None
    std_deviation: Optional[float] = None
    yoy_pct: Optional[float] = None


# ─── Create ───────────────────────────────────────────
class NaaimExposureCreate(NaaimExposureBase):
    pass


# ─── Response (single record) ────────────────────────
class NaaimExposureResponse(NaaimExposureBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ─── Latest endpoint response ────────────────────────
class NaaimLatestResponse(BaseModel):
    """Current value + summary statistics."""
    current: NaaimExposureResponse
    previous: Optional[NaaimExposureResponse] = None
    week_change: Optional[float] = None           # Δ from previous week
    last_quarter_avg: Optional[float] = None       # Last full quarter avg
    ytd_avg: Optional[float] = None                # Year-to-date avg
    all_time_high: Optional[float] = None          # All-time high NAAIM
    all_time_low: Optional[float] = None           # All-time low NAAIM
    total_records: int = 0


# ─── History endpoint (paginated) ────────────────────
class NaaimHistoryResponse(BaseModel):
    data: List[NaaimExposureResponse]
    total: int
    limit: int
    offset: int


# ─── Chart data point ────────────────────────────────
class NaaimChartPoint(BaseModel):
    """Lightweight data point optimized for Recharts / Chart.js."""
    date: str                                      # ISO date string
    naaim_index: float
    sp500: Optional[float] = None
    naaim_ma: Optional[float] = None               # 2-week moving average
    yoy_pct: Optional[float] = None


class NaaimChartResponse(BaseModel):
    data: List[NaaimChartPoint]
    last_updated: Optional[str] = None
