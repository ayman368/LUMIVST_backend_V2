from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import date, datetime

class SubstantialShareholderResponse(BaseModel):
    id: int
    report_date: date
    company_name: Optional[str]
    shareholder_name: Optional[str]
    holding_percent_last_day: Optional[str]
    holding_percent_previous_day: Optional[str]
    change: Optional[str]
    managed_by_authorized_trading_day: Optional[str]
    managed_by_authorized_previous_day: Optional[str]
    
    class Config:
        from_attributes = True

class NetShortPositionResponse(BaseModel):
    id: int
    report_date: date
    symbol: Optional[str]
    company: Optional[str]
    percent_over_outstanding: Optional[str]
    percent_over_free_float: Optional[str]
    ratio_over_avg_daily: Optional[str]
    
    class Config:
        from_attributes = True

class ForeignHeadroomResponse(BaseModel):
    id: int
    report_date: date
    symbol: Optional[str]
    company: Optional[str]
    foreign_limit: Optional[str]
    actual_foreign_ownership: Optional[str]
    ownership_room: Optional[str]
    
    class Config:
        from_attributes = True

class ShareBuybackResponse(BaseModel):
    id: int
    report_date: date
    symbol: Optional[str]
    company: Optional[str]
    data: Optional[Dict[str, Any]]
    
    class Config:
        from_attributes = True

class SBLPositionResponse(BaseModel):
    id: int
    report_date: date
    symbol: Optional[str]
    company: Optional[str]
    total_issued_shares: Optional[str]
    lent_asset_quantity: Optional[str]
    percent_of_lent_asset: Optional[str]
    
    class Config:
        from_attributes = True
