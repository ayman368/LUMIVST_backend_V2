"""
Market Breadth API
Computes % of stocks above X-day Moving Average for each trading day.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date, timedelta
import pandas as pd

from app.core.database import get_db
from app.models.static_stock_info import StaticStockInfo
from app.models.price import Price

router = APIRouter(prefix="/market-breadth", tags=["Market Breadth"])


def _apply_period_filter(data_all: list, period: str) -> list:
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

    for d in filtered_data:
        del d['date_obj']

    return filtered_data


def _compute_breadth_from_prices_df(df: pd.DataFrame) -> list:
    if df.empty:
        return []

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['symbol', 'date'])

    for ma in [20, 50, 150, 200]:
        df[f'sma_{ma}'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(ma).mean())
        df[f'above_{ma}'] = (df['close'] > df[f'sma_{ma}']).astype(int)
        df[f'has_sma_{ma}'] = df[f'sma_{ma}'].notna().astype(int)

    grouped = df.groupby('date').agg(
        above_20=('above_20', 'sum'),
        has_sma_20=('has_sma_20', 'sum'),
        above_50=('above_50', 'sum'),
        has_sma_50=('has_sma_50', 'sum'),
        above_150=('above_150', 'sum'),
        has_sma_150=('has_sma_150', 'sum'),
        above_200=('above_200', 'sum'),
        has_sma_200=('has_sma_200', 'sum'),
    ).reset_index()

    for ma in [20, 50, 150, 200]:
        grouped[f'pct_above_{ma}'] = (
            grouped[f'above_{ma}'] / grouped[f'has_sma_{ma}'] * 100
        ).fillna(0).round(2)

    final_df = grouped[['date', 'pct_above_20', 'pct_above_50', 'pct_above_150', 'pct_above_200']]
    final_df = final_df[final_df['pct_above_20'] > 0]

    data_all = []
    for i, row in final_df.iterrows():
        subset = final_df.iloc[: i + 1]

        def get_sma(attr: str, n: int) -> float:
            if len(subset) < n:
                return 0.0
            vals = subset[attr].tail(n).dropna()
            return float(vals.mean()) if len(vals) else 0.0

        d = row['date'].date() if hasattr(row['date'], 'date') else row['date']
        data_all.append({
            'time': d.isoformat(),
            'date_obj': d,
            'total': 0,
            'pct_above_20': float(row['pct_above_20']),
            'pct_above_50': float(row['pct_above_50']),
            'pct_above_150': float(row['pct_above_150']),
            'pct_above_200': float(row['pct_above_200']),
            'ma50_20': get_sma('pct_above_20', 50),
            'ma200_20': get_sma('pct_above_20', 200),
            'ma50_50': get_sma('pct_above_50', 50),
            'ma200_50': get_sma('pct_above_50', 200),
            'ma50_150': get_sma('pct_above_150', 50),
            'ma200_150': get_sma('pct_above_150', 200),
            'ma50_200': get_sma('pct_above_200', 50),
            'ma200_200': get_sma('pct_above_200', 200),
        })

    return data_all


def _compute_filtered_breadth(db: Session, statuses: List[str], period: str) -> dict:
    symbol_rows = (
        db.query(StaticStockInfo.symbol)
        .filter(StaticStockInfo.approval_with_controls.in_(statuses))
        .all()
    )
    symbols = [str(r[0]) for r in symbol_rows]
    if not symbols:
        return {"count": 0, "data": []}

    rows = (
        db.query(Price.symbol, Price.date, Price.close)
        .filter(Price.symbol.in_(symbols))
        .order_by(Price.symbol, Price.date)
        .all()
    )
    if not rows:
        return {"count": 0, "data": []}

    df = pd.DataFrame(rows, columns=['symbol', 'date', 'close'])
    data_all = _compute_breadth_from_prices_df(df)
    filtered_data = _apply_period_filter(data_all, period)
    return {"count": len(filtered_data), "data": filtered_data}


@router.get("/percent-above-ma")
async def get_percent_above_ma(
    db: Session = Depends(get_db),
    period: str = Query("ALL", description="5D, 1M, 6M, 1Y, 5Y, 10Y, ALL"),
    approval_with_controls: Optional[str] = Query(
        None,
        description="Comma-separated Shariah statuses (Arabic labels)",
    ),
):
    """
    Returns historical % of stocks above 20/50/150/200 day Moving Average.
    Uses market_breadth table for fast aggregated data.
    """
    try:
        if approval_with_controls:
            statuses = [s.strip() for s in approval_with_controls.split(',') if s.strip()]
            if statuses:
                return _compute_filtered_breadth(db, statuses, period)

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

        filtered_data = _apply_period_filter(data_all, period)
        return {"count": len(filtered_data), "data": filtered_data}

    except Exception as e:
        print(f"Error computing market breadth: {e}")
        raise HTTPException(status_code=500, detail=str(e))
