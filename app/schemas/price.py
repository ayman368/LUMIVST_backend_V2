from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
from decimal import Decimal


class PriceResponse(BaseModel):
    """
    Pure OHLCV response from the prices table.
    All technical indicators are now in stock_indicators table.
    """
    symbol: str
    company_name: Optional[str] = None
    industry_group: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    date: date
    open: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    close: Decimal
    change: Optional[Decimal] = None
    change_percent: Optional[Decimal] = None
    volume_traded: Optional[int] = None
    value_traded_sar: Optional[Decimal] = None
    no_of_trades: Optional[int] = None
    market_cap: Optional[Decimal] = None
    approval_with_controls: Optional[str] = None
    purge_amount: Optional[Decimal] = None
    marginable_percent: Optional[Decimal] = None
    trading_view_symbol: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class LatestPricesResponse(BaseModel):
    date: date
    count: int
    data: list[PriceResponse]
