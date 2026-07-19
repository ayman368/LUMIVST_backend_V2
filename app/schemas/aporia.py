from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class AporiaAnalyticsBase(BaseModel):
    filter_category: str
    ticker: Optional[str] = None
    name: Optional[str] = None
    sector: Optional[str] = None
    market_cap: Optional[str] = None
    val_avg_3mo: Optional[str] = None
    trailingPE: Optional[str] = None
    last: Optional[str] = None
    mtd_rtn: Optional[str] = None
    mo3_rtn: Optional[str] = None
    year_rtn: Optional[str] = None
    daily_trend: Optional[str] = None
    weekly_trend: Optional[str] = None
    monthly_trend: Optional[str] = None
    trend_rank: Optional[str] = None
    pfh_250: Optional[str] = None
    days_since_high_250: Optional[str] = None
    breakout: Optional[str] = None
    longest_consolidation_window: Optional[str] = None
    position: Optional[str] = None
    price_extreme: Optional[str] = None
    vol_5_day_chng: Optional[str] = None
    vol_20_day_chng: Optional[str] = None

class AporiaAnalyticsResponse(AporiaAnalyticsBase):
    id: int
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
