from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc, text
from typing import List, Optional
from datetime import date
from pydantic import BaseModel
import logging

from app.core.database import get_db
from app.core.limiter import limiter
from fastapi import Request
from app.core.cache_helpers import (
    cache_read_through,
    make_rsv2_latest_key,
    make_rsv2_history_key,
    make_rsv2_stats_key,
    make_rsv2_industries_key,
    make_rsv2_topmovers_key
)
from app.core.cache_config import CACHE_TTL_SCREENERS, CACHE_TTL_RS_HISTORY, CACHE_TTL_RS_V2_STATS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rs-v2", tags=["Relative Strength V2"])


class RSV2Item(BaseModel):
    symbol: str
    date: date
    rs_rating: Optional[int] = None
    rs_raw: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_9m: Optional[float] = None
    return_12m: Optional[float] = None
    rank_1m: Optional[int] = None
    rank_3m: Optional[int] = None
    rank_6m: Optional[int] = None
    rank_9m: Optional[int] = None
    rank_12m: Optional[int] = None
    company_name: Optional[str] = None
    industry_group: Optional[str] = None
    
    # New IBD Metrics
    sector_rs_rating: Optional[str] = None
    industry_group_rs_rating: Optional[str] = None
    industry_rs_rating: Optional[str] = None
    sub_industry_rs_rating: Optional[str] = None
    acc_dis_rating: Optional[str] = None

    class Config:
        from_attributes = True


class RSV2LatestResponse(BaseModel):
    data: List[RSV2Item]
    total_count: int
    date: date


class RSV2StatsResponse(BaseModel):
    total_records: int
    date_range: dict
    latest_date: date
    stocks_count: int
    avg_rs: float


@router.get("/latest", response_model=RSV2LatestResponse)
@limiter.limit("200/minute")
async def get_latest_rs_v2(
    request: Request,
    min_rs: Optional[int] = Query(None, ge=0, le=99, description="Minimum RS Rating"),
    max_rs: Optional[int] = Query(None, ge=0, le=99, description="Maximum RS Rating"),
    industry: Optional[str] = Query(None, description="Filter by industry group"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get latest RS ratings from the V2 table (new calculation method).
    Cached with 10-minute TTL.
    """
    cache_key = make_rsv2_latest_key(min_rs, max_rs, industry, limit, offset)
    
    async def fetch_latest_v2():
        try:
            # Get latest READY date - always from update_status (Atomic Switch)
            # During is_updating=TRUE, latest_ready_date still points to yesterday (safe data)
            # Fallback: only use MAX(date) when NOT updating (safe), otherwise keep old date
            result = db.execute(text("""
                SELECT latest_ready_date, is_updating FROM update_status WHERE id = 1
            """))
            row = result.fetchone()
            
            if row and row[0]:
                latest_date = row[0]
            elif row and row[1]:
                # is_updating=TRUE but no latest_ready_date → system just initialised, serve nothing
                return RSV2LatestResponse(data=[], total_count=0, date=date.today())
            else:
                # Fully safe fallback: only used when update_status is missing entirely
                result = db.execute(text("""
                    SELECT COALESCE(MAX(date), CURRENT_DATE)::date 
                    FROM rs_daily_v2 
                    WHERE acc_dis_rating IS NOT NULL
                """))
                latest_date = result.scalar()
            
            if not latest_date:
                return RSV2LatestResponse(data=[], total_count=0, date=date.today())
            
            # Build query - use CAST() instead of :: to avoid SQLAlchemy parameter binding conflict
            query = """
                SELECT symbol, date, rs_rating, rs_raw, 
                       return_3m, return_6m, return_9m, return_12m,
                       rank_1m, rank_3m, rank_6m, rank_9m, rank_12m,
                       company_name, industry_group,
                       sector_rs_rating, industry_group_rs_rating, industry_rs_rating, sub_industry_rs_rating, acc_dis_rating
                FROM rs_daily_v2
                WHERE CAST(date AS DATE) = CAST(:latest_date AS DATE)
            """
            params = {"latest_date": latest_date}
            
            if min_rs is not None:
                query += " AND rs_rating >= :min_rs"
                params["min_rs"] = min_rs
                
            if max_rs is not None:
                query += " AND rs_rating <= :max_rs"
                params["max_rs"] = max_rs
                
            if industry:
                query += " AND industry_group ILIKE :industry"
                params["industry"] = f"%{industry}%"
            
            query += " ORDER BY rs_rating DESC LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset
            
            result = db.execute(text(query), params)
            rows = result.fetchall()
            
            data = [RSV2Item(
                symbol=row[0],
                date=row[1],
                rs_rating=row[2],
                rs_raw=float(row[3]) if row[3] else None,
                return_3m=float(row[4]) if row[4] else None,
                return_6m=float(row[5]) if row[5] else None,
                return_9m=float(row[6]) if row[6] else None,
                return_12m=float(row[7]) if row[7] else None,
                rank_1m=row[8],
                rank_3m=row[9],
                rank_6m=row[10],
                rank_9m=row[11],
                rank_12m=row[12],
                company_name=row[13],
                industry_group=row[14],
                sector_rs_rating=row[15],
                industry_group_rs_rating=row[16],
                industry_rs_rating=row[17],
                sub_industry_rs_rating=row[18],
                acc_dis_rating=row[19]
            ) for row in rows]
            
            # Get total count
            count_query = "SELECT COUNT(*) FROM rs_daily_v2 WHERE CAST(date AS DATE) = CAST(:latest_date AS DATE)"
            count_result = db.execute(text(count_query), {"latest_date": latest_date})
            total_count = count_result.scalar()
            
            return RSV2LatestResponse(data=data, total_count=total_count, date=latest_date)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Error in get_latest_rs_v2: {e}")
            raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
    
    # Use cache read-through
    result = await cache_read_through(
        cache_key,
        CACHE_TTL_SCREENERS,
        fetch_latest_v2
    )
    return result


@router.get("/history/{symbol}")
@limiter.limit("60/minute")
async def get_rs_history_v2(
    request: Request,
    symbol: str,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    limit: int = Query(365, le=10000),
    db: Session = Depends(get_db)
):
    """
    Get RS history for a specific symbol from V2 table.
    Cached with 30-minute TTL.
    """
    cache_key = make_rsv2_history_key(
        symbol,
        start_date.isoformat() if start_date else None,
        end_date.isoformat() if end_date else None,
        limit
    )
    
    async def fetch_history_v2():
        try:
            query = """
                SELECT date, rs_rating, rs_raw, 
                       return_3m, return_6m, return_9m, return_12m,
                       rank_3m, rank_6m, rank_9m, rank_12m,
                       sector_rs_rating, industry_group_rs_rating, acc_dis_rating
                FROM rs_daily_v2
                WHERE symbol = :symbol
            """
            params = {"symbol": symbol}
            
            if start_date:
                query += " AND date >= :start_date"
                params["start_date"] = start_date
                
            if end_date:
                query += " AND date <= :end_date"
                params["end_date"] = end_date
            
            query += " ORDER BY date DESC LIMIT :limit"
            params["limit"] = limit
            
            result = db.execute(text(query), params)
            rows = result.fetchall()
            
            data = [{
                "date": str(row[0]),
                "rs_rating": row[1],
                "rs_raw": float(row[2]) if row[2] else None,
                "return_3m": float(row[3]) if row[3] else None,
                "return_6m": float(row[4]) if row[4] else None,
                "return_9m": float(row[5]) if row[5] else None,
                "return_12m": float(row[6]) if row[6] else None,
                "rank_3m": row[7],
                "rank_6m": row[8],
                "rank_9m": row[9],
                "rank_12m": row[10],
                "sector_rs_rating": row[11],
                "industry_group_rs_rating": row[12],
                "acc_dis_rating": row[13]
            } for row in rows]
            
            return {"symbol": symbol, "data": data, "count": len(data)}
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Error in get_rs_history_v2: {e}")
            raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
    
    # Use cache read-through
    result = await cache_read_through(
        cache_key,
        CACHE_TTL_RS_HISTORY,
        fetch_history_v2
    )
    return result


@router.get("/stats", response_model=RSV2StatsResponse)
@limiter.limit("60/minute")
async def get_rs_stats_v2(
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Get statistics about the RS V2 data.
    Cached with 10-minute TTL.
    """
    cache_key = make_rsv2_stats_key()
    
    async def fetch_stats_v2():
        try:
            stats_query = """
                SELECT 
                    COUNT(*) as total_records,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT symbol) as stocks_count,
                    AVG(rs_rating) as avg_rs
                FROM rs_daily_v2
            """
            result = db.execute(text(stats_query))
            row = result.fetchone()
            
            return RSV2StatsResponse(
                total_records=row[0],
                date_range={"start": str(row[1]), "end": str(row[2])},
                latest_date=row[2],
                stocks_count=row[3],
                avg_rs=float(row[4]) if row[4] else 50.0
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Error in get_rs_stats_v2: {e}")
            raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
    
    # Use cache read-through
    result = await cache_read_through(
        cache_key,
        CACHE_TTL_RS_V2_STATS,
        fetch_stats_v2
    )
    return result


@router.get("/industries")
@limiter.limit("30/minute")
async def get_industries_v2(
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Get all unique industry groups.
    Cached with 10-minute TTL.
    """
    cache_key = make_rsv2_industries_key()
    
    async def fetch_industries_v2():
        try:
            query = """
                SELECT DISTINCT industry_group, COUNT(*) as count
                FROM rs_daily_v2
                WHERE date = (SELECT latest_ready_date FROM update_status WHERE id = 1)
                AND industry_group IS NOT NULL
                GROUP BY industry_group
                ORDER BY count DESC
            """
            result = db.execute(text(query))
            rows = result.fetchall()
            
            return {"industries": [{"name": row[0], "count": row[1]} for row in rows]}
            
        except Exception as e:
            logger.error(f"Error in get_industries_v2: {e}")
            raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
    
    # Use cache read-through
    result = await cache_read_through(
        cache_key,
        CACHE_TTL_RS_V2_STATS,
        fetch_industries_v2
    )
    return result


@router.get("/top-movers")
@limiter.limit("30/minute")
async def get_top_movers_v2(
    request: Request,
    days: int = Query(5, ge=1, le=30),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db)
):
    """
    Get stocks with biggest RS changes over specified days.
    Cached with 10-minute TTL.
    """
    cache_key = make_rsv2_topmovers_key(days, limit)
    
    async def fetch_topmovers_v2():
        try:
            query = """
                WITH latest AS (
                    SELECT symbol, rs_rating as current_rs, date
                    FROM rs_daily_v2
                    WHERE date = (SELECT latest_ready_date FROM update_status WHERE id = 1)
                ),
                prev AS (
                    SELECT symbol, rs_rating as prev_rs
                    FROM rs_daily_v2
                    WHERE date = (SELECT latest_ready_date - :days FROM update_status WHERE id = 1)
                )
                SELECT l.symbol, l.current_rs, p.prev_rs, (l.current_rs - p.prev_rs) as change
                FROM latest l
                JOIN prev p ON l.symbol = p.symbol
                ORDER BY ABS(l.current_rs - p.prev_rs) DESC
                LIMIT :limit
            """
            result = db.execute(text(query), {"days": days, "limit": limit})
            rows = result.fetchall()
            
            return {
                "gainers": [{"symbol": r[0], "current": r[1], "previous": r[2], "change": r[3]} 
                           for r in rows if r[3] and r[3] > 0][:limit//2],
                "losers": [{"symbol": r[0], "current": r[1], "previous": r[2], "change": r[3]} 
                          for r in rows if r[3] and r[3] < 0][:limit//2]
            }
            
        except Exception as e:
            logger.error(f"Error in get_top_movers_v2: {e}")
            raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")
    
    # Use cache read-through
    result = await cache_read_through(
        cache_key,
        CACHE_TTL_RS_V2_STATS,
        fetch_topmovers_v2
    )
    return result
