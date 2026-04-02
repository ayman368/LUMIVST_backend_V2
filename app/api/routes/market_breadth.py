"""
Market Breadth API
Computes % of stocks above X-day Moving Average for each trading day.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, cast, Float
from typing import Optional
from datetime import date

from app.core.database import get_db
from app.models.stock_indicators import StockIndicator

router = APIRouter(prefix="/market-breadth", tags=["Market Breadth"])


@router.get("/percent-above-ma")
async def get_percent_above_ma(
    db: Session = Depends(get_db),
    limit: int = Query(10000, le=10000),
):
    """
    Returns historical % of stocks above 20/50/100/200 day Moving Average.
    Uses market_breadth table for fast aggregated data.
    """
    try:
        from app.models import market_breadth as mb
        
        # Query the pre-computed market_breadth table for speed
        results = (
            db.query(
                mb.MarketBreadth.date,
                mb.MarketBreadth.pct_above_20,
                mb.MarketBreadth.pct_above_50,
                mb.MarketBreadth.pct_above_100,
                mb.MarketBreadth.pct_above_200,
            )
            .order_by(mb.MarketBreadth.date.desc())
            .limit(limit)
            .all()
        )
        
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
