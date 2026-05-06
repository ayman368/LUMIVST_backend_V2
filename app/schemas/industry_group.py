from typing import List, Optional
from pydantic import BaseModel
from datetime import date
from app.schemas.price import PriceResponse

class IndustryGroupBase(BaseModel):
    industry_group: str
    sector: Optional[str]
    number_of_stocks: int
    market_value: Optional[float]
    rs_score: Optional[float]
    rank: Optional[int]
    letter_grade: Optional[str]
    change_vs_last_week: Optional[int]
    change_vs_3m_ago: Optional[int]
    change_vs_6m_ago: Optional[int]
    rank_1_week_ago: Optional[int]
    rank_3_months_ago: Optional[int]
    rank_6_months_ago: Optional[int]
    ytd_change_percent: Optional[float]
    percent_above_ma20: Optional[float] = None
    percent_above_ma50: Optional[float] = None
    percent_above_ma150: Optional[float] = None
    percent_above_ma200: Optional[float] = None
    count_above_ma20: Optional[int] = None
    count_above_ma50: Optional[int] = None
    count_above_ma150: Optional[int] = None
    count_above_ma200: Optional[int] = None

class IndustryGroupResponse(IndustryGroupBase):
    id: int
    date: date

    class Config:
        from_attributes = True

class IndustryGroupStockResponse(PriceResponse):
    rs_rating: Optional[int] = None
    rs_rating_1_week_ago: Optional[int] = None
    rs_rating_4_weeks_ago: Optional[int] = None
    rs_rating_3_months_ago: Optional[int] = None
    rs_rating_6_months_ago: Optional[int] = None
    rs_rating_1_year_ago: Optional[int] = None
