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
    Returns historical % of stocks above 20/50/150/200 day Moving Average.
    Uses market_breadth table for fast aggregated data.
    """
    try:
        from app.models import market_breadth as mb
        
        # Fetch all records to calculate rolling averages correctly
        results = db.query(mb.MarketBreadth).order_by(mb.MarketBreadth.date.asc()).all()

        data_all = []
        for i, row in enumerate(results):
            # Helper to calculate Simple Moving Average
            def get_sma(attr, n):
                if i < n - 1:
                    return 0
                subset = [getattr(r, attr) for r in results[i-n+1 : i+1]]
                valid = [x for x in subset if x is not None]
                if not valid: return 0
                return sum(valid) / len(valid)

            data_all.append({
                "time": row.date.isoformat(),
                "date_obj": row.date,
                "pct_above_20": float(row.pct_above_20) if row.pct_above_20 else 0,
                "pct_above_50": float(row.pct_above_50) if row.pct_above_50 else 0,
                "pct_above_150": float(row.pct_above_150) if getattr(row, 'pct_above_150', None) is not None else 0,
                "pct_above_200": float(row.pct_above_200) if row.pct_above_200 else 0,
                
                "ma50_20": float(get_sma('pct_above_20', 50)),
                "ma200_20": float(get_sma('pct_above_20', 200)),
                "ma50_50": float(get_sma('pct_above_50', 50)),
                "ma200_50": float(get_sma('pct_above_50', 200)),
                "ma50_150": float(get_sma('pct_above_150', 50)),
                "ma200_150": float(get_sma('pct_above_150', 200)),
                "ma50_200": float(get_sma('pct_above_200', 50)),
                "ma200_200": float(get_sma('pct_above_200', 200)),
            })

        # Apply date filter AFTER calculating the rolling averages
        today = date.today()
        period = period.upper()
        target_date = None
        
        if period == "5D":
            target_date = today - timedelta(days=7)
        elif period == "1M":
            target_date = today - timedelta(days=30)
        elif period == "6M":
            target_date = today - timedelta(days=180)
        elif period == "1Y":
            target_date = today - timedelta(days=365)
        elif period == "5Y":
            target_date = today - timedelta(days=365 * 5)
        elif period == "10Y":
            target_date = today - timedelta(days=365 * 10)

        if target_date:
            filtered_data = [d for d in data_all if d['date_obj'] >= target_date]
        else:
            filtered_data = data_all
            
            
        # Clean up date_obj before sending JSON
        for d in filtered_data:
            del d['date_obj']

        return {"count": len(filtered_data), "data": filtered_data}

    except Exception as e:
        print(f"Error computing market breadth: {e}")
        raise HTTPException(status_code=500, detail=str(e))
