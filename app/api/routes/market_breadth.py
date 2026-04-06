"""
Market Breadth API
Computes % of stocks above X-day Moving Average for each trading day.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, cast, Float
from typing import Optional
from datetime import date, timedelta

from app.core.database import get_db
from app.models.stock_indicators import StockIndicator

router = APIRouter(prefix="/market-breadth", tags=["Market Breadth"])


@router.get("/percent-above-ma")
async def get_percent_above_ma(
    db: Session = Depends(get_db),
    period: str = Query("ALL", description="5D, 1M, 6M, 1Y, 5Y, 10Y, ALL"),
):
    """
    Returns historical % of stocks above 20/50/100/200 day Moving Average.
    Uses market_breadth table for fast aggregated data.
    """
    try:
        from app.models import market_breadth as mb
        
        query = db.query(
            mb.MarketBreadth.date,
            mb.MarketBreadth.pct_above_20,
            mb.MarketBreadth.pct_above_50,
            mb.MarketBreadth.pct_above_100,
            mb.MarketBreadth.pct_above_200,
        )

        today = date.today()
        
        period = period.upper()
        if period == "5D":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=7))
        elif period == "1M":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=30))
        elif period == "6M":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=180))
        elif period == "1Y":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=365))
        elif period == "5Y":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=365 * 5))
        elif period == "10Y":
            query = query.filter(mb.MarketBreadth.date >= today - timedelta(days=365 * 10))

        results = query.order_by(mb.MarketBreadth.date.desc()).all()
        
        # لعرض الشارت من القديم للحديث، يجب أن نعكس القائمة
        results.reverse()

        data = []
        for row in results:
            data.append({
                "time": row.date.isoformat(),
                "pct_above_20": float(row.pct_above_20) if row.pct_above_20 else 0,
                "pct_above_50": float(row.pct_above_50) if row.pct_above_50 else 0,
                "pct_above_100": float(row.pct_above_100) if row.pct_above_100 else 0,
                "pct_above_200": float(row.pct_above_200) if row.pct_above_200 else 0,
            })

        return {"count": len(data), "data": data}

    except Exception as e:
        print(f"Error computing market breadth: {e}")
        raise HTTPException(status_code=500, detail=str(e))
