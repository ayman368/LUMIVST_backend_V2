from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
from decimal import Decimal

class PriceResponse(BaseModel):
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
    trading_view_symbol: Optional[str] = None
    
    # Technical Indicators
    price_minus_sma_10: Optional[Decimal] = None
    price_minus_sma_21: Optional[Decimal] = None
    price_minus_sma_50: Optional[Decimal] = None
    price_minus_sma_150: Optional[Decimal] = None
    price_minus_sma_200: Optional[Decimal] = None
    
    fifty_two_week_high: Optional[Decimal] = None
    fifty_two_week_low: Optional[Decimal] = None
    average_volume_50: Optional[int] = None
    
    # Technicals (Percentages)
    price_vs_sma_10_percent: Optional[Decimal] = None
    price_vs_sma_21_percent: Optional[Decimal] = None
    price_vs_sma_50_percent: Optional[Decimal] = None
    price_vs_sma_150_percent: Optional[Decimal] = None
    price_vs_sma_200_percent: Optional[Decimal] = None
    
    percent_off_52w_high: Optional[Decimal] = None
    percent_off_52w_low: Optional[Decimal] = None
    vol_diff_50_percent: Optional[Decimal] = None
    
    # New Technical Indicators - 21 Day EMA
    ema_21: Optional[Decimal] = None
    
    # Historical 200MA (for moving average comparisons)
    sma_200_1m_ago: Optional[Decimal] = None
    sma_200_2m_ago: Optional[Decimal] = None
    sma_200_3m_ago: Optional[Decimal] = None
    sma_200_4m_ago: Optional[Decimal] = None
    sma_200_5m_ago: Optional[Decimal] = None
    
    # Weekly Moving Averages
    sma_30w: Optional[Decimal] = None
    sma_40w: Optional[Decimal] = None
    
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class LatestPricesResponse(BaseModel):
    date: date
    count: int
    data: list[PriceResponse]
