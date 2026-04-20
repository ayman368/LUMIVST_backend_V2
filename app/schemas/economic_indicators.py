from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

class EconomicIndicatorBase(BaseModel):
    report_date: date
    indicator_code: str
    value: Optional[float] = None
    yoy_pct: Optional[float] = None

class EconomicIndicatorCreate(EconomicIndicatorBase):
    pass

class EconomicIndicatorResponse(EconomicIndicatorBase):
    id: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class TreasuryYieldCurveResponse(BaseModel):
    id: int
    report_date: date
    month_1: Optional[float] = None
    month_1_5: Optional[float] = None
    month_2: Optional[float] = None
    month_3: Optional[float] = None
    month_4: Optional[float] = None
    month_6: Optional[float] = None
    year_1: Optional[float] = None
    year_2: Optional[float] = None
    year_3: Optional[float] = None
    year_5: Optional[float] = None
    year_7: Optional[float] = None
    year_10: Optional[float] = None
    year_20: Optional[float] = None
    year_30: Optional[float] = None

    class Config:
        from_attributes = True

class SP500HistoryResponse(BaseModel):
    id: int
    trade_date: date
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    volume: Optional[int] = None

    class Config:
        from_attributes = True


class SofrFuturesResponse(BaseModel):
    id: int
    scrape_date: date
    contract: str
    last_price: Optional[float] = None
    change: Optional[float] = None
    open_price: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    previous: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    updated_time: Optional[str] = None

    class Config:
        from_attributes = True


class CmeFedwatchResponse(BaseModel):
    id: int
    scrape_date: date
    meeting_date: str
    rate_range: str
    probability: float

    class Config:
        from_attributes = True

