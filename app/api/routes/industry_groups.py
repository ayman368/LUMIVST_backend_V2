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
        
        # Calculate Market Breadth dynamically
        from app.models.price import Price
        from app.models.stock_indicators import StockIndicator
        price_latest_date = db.query(func.max(Price.date)).scalar()
        
        breadth_dict = {}
        if price_latest_date:
            records = db.query(
                Price.industry_group,
                StockIndicator.close,
                StockIndicator.sma_20,
                StockIndicator.sma_50,
                StockIndicator.sma_150,
                StockIndicator.sma_200
            ).join(
                StockIndicator,
                (Price.symbol == StockIndicator.symbol) & (StockIndicator.date == price_latest_date)
            ).filter(
                Price.date == price_latest_date
            ).all()
            
            for row in records:
                ig = row.industry_group
                if not ig:
                    continue
                if ig not in breadth_dict:
                    breadth_dict[ig] = {'total': 0, 'ma20': 0, 'ma50': 0, 'ma150': 0, 'ma200': 0}
                
                c = row.close
                breadth_dict[ig]['total'] += 1
                if c is not None and row.sma_20 is not None and c > row.sma_20:
                    breadth_dict[ig]['ma20'] += 1
                if c is not None and row.sma_50 is not None and c > row.sma_50:
                    breadth_dict[ig]['ma50'] += 1
                if c is not None and row.sma_150 is not None and c > row.sma_150:
                    breadth_dict[ig]['ma150'] += 1
                if c is not None and row.sma_200 is not None and c > row.sma_200:
                    breadth_dict[ig]['ma200'] += 1
                    
        result = []
        for g in groups:
            g_dict = {c.name: getattr(g, c.name) for c in g.__table__.columns}
            if g.industry_group in breadth_dict and breadth_dict[g.industry_group]['total'] > 0:
                b = breadth_dict[g.industry_group]
                g_dict['percent_above_ma20'] = round((b['ma20'] / b['total']) * 100, 2)
                g_dict['percent_above_ma50'] = round((b['ma50'] / b['total']) * 100, 2)
                g_dict['percent_above_ma150'] = round((b['ma150'] / b['total']) * 100, 2)
                g_dict['percent_above_ma200'] = round((b['ma200'] / b['total']) * 100, 2)
                g_dict['count_above_ma20'] = b['ma20']
                g_dict['count_above_ma50'] = b['ma50']
                g_dict['count_above_ma150'] = b['ma150']
                g_dict['count_above_ma200'] = b['ma200']
            else:
                g_dict['percent_above_ma20'] = None
                g_dict['percent_above_ma50'] = None
                g_dict['percent_above_ma150'] = None
                g_dict['percent_above_ma200'] = None
                g_dict['count_above_ma20'] = None
                g_dict['count_above_ma50'] = None
                g_dict['count_above_ma150'] = None
                g_dict['count_above_ma200'] = None
            result.append(g_dict)
            
        return result
        
    return await cache_read_through(cache_key, CACHE_TTL_INDUSTRY_GROUPS_LATEST, fetch_latest)

# --- New Endpoint for Group Details ---

from app.models.price import Price
# Re-using PriceResponse or simpler
from app.schemas.price import PriceResponse

from app.models.rs_daily import RSDaily



@router.get("/stocks", response_model=List[IndustryGroupStockResponse])
async def get_industry_group_stocks(
    industry_group: Optional[str] = Query(None, description="Name of the industry group (optional, if omitted returns all stocks)"),
    db: Session = Depends(get_db)
):
    """
    Get all stocks belonging to a specific Industry Group for the latest available date,
    including historical RS ratings.
    """
    cache_key = make_industry_groups_stocks_key(industry_group or 'ALL_STOCKS')
    
    async def fetch_stocks():
        # 1. Find the latest date in Prices
        latest_date = db.query(func.max(Price.date)).scalar()
        
        if not latest_date:
            return []

        # 2. Query stocks
        query = db.query(Price).filter(Price.date == latest_date)
        if industry_group:
            query = query.filter(Price.industry_group == industry_group)
        stocks = query.order_by(Price.symbol).all()
        
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
