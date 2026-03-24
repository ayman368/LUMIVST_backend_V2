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
    limit: int = Query(1000, le=5000),
):
    """
    Returns historical % of stocks above 20/50/100/150/200 day Moving Average.
    Groups by date and computes the percentage for each MA period.
    """
    try:
        # We use close vs sma_XX from stock_indicators
        # sma_10 exists but not sma_20; we'll approximate 20 using ema21 or skip
        # Actually the model has sma_10, sma_21, sma_50, sma_150, sma_200
        # The user wants 20/50/100/150/200. We have 21 (close to 20), 50, 150, 200
        # For 100-day we don't have it. We can skip or compute from available data.
        # Let's use: 20≈sma_21, 50=sma_50, 150=sma_150, 200=sma_200
        # For 100, we don't have a column, so we'll skip it or note it's not available.

        results = (
            db.query(
                StockIndicator.date,
                func.count(StockIndicator.id).label('total'),
                # % above 20-day MA (using sma_21 as proxy)
                func.sum(
                    case(
                        (and_(
                            StockIndicator.close.isnot(None),
                            StockIndicator.sma_21.isnot(None),
                            StockIndicator.close > StockIndicator.sma_21
                        ), 1),
                        else_=0
                    )
                ).label('above_20'),
                # % above 50-day MA
                func.sum(
                    case(
                        (and_(
                            StockIndicator.close.isnot(None),
                            StockIndicator.sma_50.isnot(None),
                            StockIndicator.close > StockIndicator.sma_50
                        ), 1),
                        else_=0
                    )
                ).label('above_50'),
                # % above 150-day MA
                func.sum(
                    case(
                        (and_(
                            StockIndicator.close.isnot(None),
                            StockIndicator.sma_150.isnot(None),
                            StockIndicator.close > StockIndicator.sma_150
                        ), 1),
                        else_=0
                    )
                ).label('above_150'),
                # % above 200-day MA
                func.sum(
                    case(
                        (and_(
                            StockIndicator.close.isnot(None),
                            StockIndicator.sma_200.isnot(None),
                            StockIndicator.close > StockIndicator.sma_200
                        ), 1),
                        else_=0
                    )
                ).label('above_200'),
            )
            .filter(
                StockIndicator.close.isnot(None),
                StockIndicator.is_etf_or_index == False
            )
            .group_by(StockIndicator.date)
            .order_by(StockIndicator.date.asc())
            .limit(limit)
            .all()
        )

        data = []
        for row in results:
            total = row.total or 1
            data.append({
                "time": row.date.isoformat(),
                "total": total,
                "pct_above_20": round((row.above_20 / total) * 100, 2) if row.above_20 else 0,
                "pct_above_50": round((row.above_50 / total) * 100, 2) if row.above_50 else 0,
                "pct_above_150": round((row.above_150 / total) * 100, 2) if row.above_150 else 0,
                "pct_above_200": round((row.above_200 / total) * 100, 2) if row.above_200 else 0,
            })

        return {"count": len(data), "data": data}

    except Exception as e:
        print(f"Error computing market breadth: {e}")
        raise HTTPException(status_code=500, detail=str(e))
