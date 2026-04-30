from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from pydantic import BaseModel
from datetime import date

from app.core.database import get_db
from app.models.industry_group import IndustryGroupHistory

router = APIRouter()

from app.schemas.industry_group import IndustryGroupResponse, IndustryGroupStockResponse
from app.core.cache_helpers import (
    cache_read_through, 
    make_industry_groups_latest_key, 
    make_industry_groups_stocks_key
)
from app.core.cache_config import CACHE_TTL_INDUSTRY_GROUPS_LATEST, CACHE_TTL_INDUSTRY_GROUPS_STOCKS

@router.get("/latest", response_model=List[IndustryGroupResponse])
async def get_latest_industry_groups(
    db: Session = Depends(get_db)
):
    """
    Get industry group rankings for the latest available date.
    """
    cache_key = make_industry_groups_latest_key()
    
    async def fetch_latest():
        # Find latest date
        latest_date = db.query(func.max(IndustryGroupHistory.date)).scalar()
        
        if not latest_date:
            return []
            
        # Get all groups for that date, ordered by Rank
        # Handle NULL ranks by putting them last
        groups = db.query(IndustryGroupHistory).filter(
            IndustryGroupHistory.date == latest_date
        ).order_by(
            IndustryGroupHistory.rank.asc().nullslast()
        ).all()
        
        return groups
        
    return await cache_read_through(cache_key, CACHE_TTL_INDUSTRY_GROUPS_LATEST, fetch_latest)

# --- New Endpoint for Group Details ---

from app.models.price import Price
# Re-using PriceResponse or simpler
from app.schemas.price import PriceResponse

from app.models.rs_daily import RSDaily



@router.get("/stocks", response_model=List[IndustryGroupStockResponse])
async def get_industry_group_stocks(
    industry_group: str = Query(..., description="Name of the industry group"),
    db: Session = Depends(get_db)
):
    """
    Get all stocks belonging to a specific Industry Group for the latest available date,
    including historical RS ratings.
    """
    cache_key = make_industry_groups_stocks_key(industry_group)
    
    async def fetch_stocks():
        # 1. Find the latest date in Prices
        latest_date = db.query(func.max(Price.date)).scalar()
        
        if not latest_date:
            return []

        # 2. Query stocks
        stocks = db.query(Price).filter(
            Price.date == latest_date,
            Price.industry_group == industry_group
        ).order_by(Price.symbol).all()
        
        if not stocks:
            return []

        # 3. Get Historical Dates & RS Data
        # Get sorted distinct dates descending
        available_dates = db.query(RSDaily.date).distinct().order_by(RSDaily.date.desc()).limit(300).all()
        available_dates = [d[0] for d in available_dates] # flatten

        # Define indices for time periods (Trading Days)
        target_indices = {
            'current': 0,
            '1_week': 5,
            '4_weeks': 20,
            '3_months': 63,
            '6_months': 126,
            '1_year': 252
        }
        
        target_dates_map = {} # date -> period_key
        
        for key, idx in target_indices.items():
            if idx < len(available_dates):
                target_dates_map[available_dates[idx]] = key
                
        # Fetch RS data for these stocks and dates
        stock_symbols = [s.symbol for s in stocks]
        target_dates_list = list(target_dates_map.keys())
        
        rs_records = db.query(RSDaily).filter(
            RSDaily.symbol.in_(stock_symbols),
            RSDaily.date.in_(target_dates_list)
        ).all()
        
        # Organize RS data: symbol -> period_key -> rating
        rs_lookup = {}
        for record in rs_records:
            if record.symbol not in rs_lookup:
                rs_lookup[record.symbol] = {}
            
            # Determine period
            if record.date in target_dates_map:
                period = target_dates_map[record.date]
                rs_lookup[record.symbol][period] = record.rs_rating

        # Fetch Static Info
        from app.models.static_stock_info import StaticStockInfo
        static_rows = db.query(StaticStockInfo).filter(StaticStockInfo.symbol.in_(stock_symbols)).all()
        static_map = {row.symbol: row for row in static_rows}

        # 4. Merge Data
        result = []
        for stock in stocks:
            s_info = static_map.get(stock.symbol)
            if s_info:
                stock.approval_with_controls = s_info.approval_with_controls
                stock.purge_amount = s_info.purge_amount
                stock.marginable_percent = s_info.marginable_percent
            else:
                stock.approval_with_controls = None
                stock.purge_amount = None
                stock.marginable_percent = None

            # Pydantic model from ORM object
            stock_data = IndustryGroupStockResponse.from_orm(stock)
            
            # Add RS data
            if stock.symbol in rs_lookup:
                stock_rs = rs_lookup[stock.symbol]
                stock_data.rs_rating = stock_rs.get('current')
                stock_data.rs_rating_1_week_ago = stock_rs.get('1_week')
                stock_data.rs_rating_4_weeks_ago = stock_rs.get('4_weeks')
                stock_data.rs_rating_3_months_ago = stock_rs.get('3_months')
                stock_data.rs_rating_6_months_ago = stock_rs.get('6_months')
                stock_data.rs_rating_1_year_ago = stock_rs.get('1_year')
                
            result.append(stock_data)
        
        return result
        
    return await cache_read_through(cache_key, CACHE_TTL_INDUSTRY_GROUPS_STOCKS, fetch_stocks)
