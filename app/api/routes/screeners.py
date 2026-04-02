"""
Stock Screeners API Endpoints
روتر متخصص لكل نوع من أنواع الـ Stock Screeners
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func
from typing import List, Optional
from datetime import date

from app.core.database import get_db
from app.models.stock_indicators import StockIndicator
from app.models.rs_daily import RSDaily

router = APIRouter(prefix="/screeners", tags=["Stock Screeners"])


def safe_float(value):
    """تحويل آمن للأرقام"""
    return float(value) if value is not None else None


def safe_bool(value):
    """تحويل آمن للمنطقيات"""
    return bool(value) if value is not None else False


def screener_to_dict(ind: StockIndicator, rs_rating=None) -> dict:
    """تحويل بيانات المؤشر إلى قاموس"""
    return {
        'symbol': ind.symbol,
        'company_name': ind.company_name,
        'date': str(ind.date) if ind.date else None,

        # ============ Price & Moving Averages ============
        'close': safe_float(ind.close),
        'sma_10': safe_float(ind.sma_10),
        'sma_21': safe_float(ind.sma_21),
        'sma_50': safe_float(ind.sma_50),
        'sma_150': safe_float(ind.sma_150),
        'sma_200': safe_float(ind.sma_200),
        'sma_30w': safe_float(ind.sma_30w),
        'sma_40w': safe_float(ind.sma_40w),
        'ema10': safe_float(ind.ema10),
        'ema21': safe_float(ind.ema21),

        # ============ 52-Week Stats ============
        'fifty_two_week_high': safe_float(ind.fifty_two_week_high),
        'fifty_two_week_low': safe_float(ind.fifty_two_week_low),
        'percent_off_52w_high': safe_float(ind.percent_off_52w_high),
        'percent_off_52w_low': safe_float(ind.percent_off_52w_low),

        # ============ RS ============
        'rs_12m': rs_rating,

        # ============ Technical Indicators ============
        'rsi_14': safe_float(ind.rsi_14),
        'cci': safe_float(ind.cci),
        'aroon_up': safe_float(ind.aroon_up),
        'aroon_down': safe_float(ind.aroon_down),
        'beta': safe_float(ind.beta),

        # ============ Historical 200MA ============
        'sma_200_1m_ago': safe_float(ind.sma_200_1m_ago),
        'sma_200_2m_ago': safe_float(ind.sma_200_2m_ago),
        'sma_200_3m_ago': safe_float(ind.sma_200_3m_ago),
        'sma_200_4m_ago': safe_float(ind.sma_200_4m_ago),
        'sma_200_5m_ago': safe_float(ind.sma_200_5m_ago),

        # ============ Boolean Conditions ============
        'sma50_gt_sma150': safe_bool(ind.sma50_gt_sma150),
        'sma50_gt_sma200': safe_bool(ind.sma50_gt_sma200),
        'sma150_gt_sma200': safe_bool(ind.sma150_gt_sma200),
        'sma200_gt_sma200_1m_ago': safe_bool(ind.sma200_gt_sma200_1m_ago),
        'price_gt_sma18': safe_bool(ind.price_gt_sma18),
        'ema10_gt_sma50': safe_bool(ind.ema10_gt_sma50),
        'ema10_gt_sma200': safe_bool(ind.ema10_gt_sma200),
        'ema21_gt_sma50': safe_bool(ind.ema21_gt_sma50),
        'ema21_gt_sma200': safe_bool(ind.ema21_gt_sma200),
        'sma200_gt_sma200_2m_ago': safe_bool(ind.sma200_gt_sma200_2m_ago),
        'sma200_gt_sma200_3m_ago': safe_bool(ind.sma200_gt_sma200_3m_ago),
        'sma200_gt_sma200_4m_ago': safe_bool(ind.sma200_gt_sma200_4m_ago),
        'sma200_gt_sma200_5m_ago': safe_bool(ind.sma200_gt_sma200_5m_ago),
    }


def get_latest_date(db: Session) -> date:
    """الحصول على آخر تاريخ متاح في stock_indicators"""
    return db.query(func.max(StockIndicator.date)).scalar()


def get_rs_map(db: Session, target_date) -> dict:
    """
    جلب RS Rating من rs_daily_v2 لكل سهم في تاريخ محدد.
    Returns: dict {symbol: rs_rating}
    """
    rows = (
        db.query(RSDaily.symbol, RSDaily.rs_rating)
        .filter(RSDaily.date == target_date)
        .all()
    )
    return {row.symbol: row.rs_rating for row in rows}


# ============ SCREENER 1: TREND - 1 MONTH ============
@router.get("/trend-1-month")
def get_trend_1_month(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Trend - 1 Month Screener

    Criteria (matches reference screenshot exactly):
    ✅ 50 Day > 150 Day: Yes
    ✅ 50 Day > 200 Day: Yes
    ✅ 150 Day > 200 Day: Yes
    ✅ 200 Day > 200 Day 1 Month Ago: Yes
    ✅ RS 12M: > 69
    ✅ % Off 52 Wk Low: > 30.00%
    ✅ % Off 52 Wk High: > -25.00%
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 150d SMA: > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    ✅ Price Vs 30w SMA:  > 0.00%
    ✅ Price Vs 40w SMA:  > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    # Fetch RS map for the date
    rs_map = get_rs_map(db, latest)
    # Only keep symbols with RS > 69
    rs_symbols = {sym for sym, rating in rs_map.items() if rating is not None and rating > 69}

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    query = query.filter(
        and_(
            StockIndicator.symbol.in_(rs_symbols),          # RS 12M > 69
            StockIndicator.sma_50 > StockIndicator.sma_150, # SMA50 > SMA150
            StockIndicator.sma_50 > StockIndicator.sma_200, # SMA50 > SMA200
            StockIndicator.sma_150 > StockIndicator.sma_200,# SMA150 > SMA200
            StockIndicator.sma_200 > StockIndicator.sma_200_1m_ago,  # 200 > 200 1M ago
            StockIndicator.percent_off_52w_low > 30.0,      # Off Low > 30%
            StockIndicator.percent_off_52w_high > -25.0,    # Off High > -25%
            StockIndicator.price_vs_sma_50_percent > 0.0,   # Price > 50d SMA
            StockIndicator.price_vs_sma_150_percent > 0.0,  # Price > 150d SMA
            StockIndicator.price_vs_sma_200_percent > 0.0,  # Price > 200d SMA
            StockIndicator.sma_30w.isnot(None),
            StockIndicator.close > StockIndicator.sma_30w,  # Price > 30w SMA
            StockIndicator.sma_40w.isnot(None),
            StockIndicator.close > StockIndicator.sma_40w,  # Price > 40w SMA
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Trend - 1 Month',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
    }


# ============ SCREENER 2: TREND - 2 MONTHS ============
@router.get("/trend-2-months")
def get_trend_2_months(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Trend - 2 Months Screener

    Criteria (matches reference screenshot exactly):
    ✅ 50 Day > 150 Day: Yes
    ✅ 50 Day > 200 Day: Yes
    ✅ 150 Day > 200 Day: Yes
    ✅ 200 Day > 200 Day 2 Month Ago: Yes
    ✅ % Off 52 Wk High: > -25.00%
    ✅ RS 12M: > 69
    ✅ % Off 52 Wk Low: > 30.00%
    ✅ 200 Day 1 Month Ago > 200 Day 2 Months Ago: Yes  (sequential chain)
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 150d SMA: > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    ✅ Price Vs 30w SMA:  > 0.00%
    ✅ Price Vs 40w SMA:  > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    rs_map = get_rs_map(db, latest)
    rs_symbols = {sym for sym, rating in rs_map.items() if rating is not None and rating > 69}

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    query = query.filter(
        and_(
            StockIndicator.symbol.in_(rs_symbols),
            StockIndicator.sma_50 > StockIndicator.sma_150,
            StockIndicator.sma_50 > StockIndicator.sma_200,
            StockIndicator.sma_150 > StockIndicator.sma_200,
            StockIndicator.sma_200 > StockIndicator.sma_200_2m_ago,          # 200 > 200 2M ago
            StockIndicator.sma_200_1m_ago > StockIndicator.sma_200_2m_ago,   # 1M ago > 2M ago (chain)
            StockIndicator.percent_off_52w_high > -25.0,
            StockIndicator.percent_off_52w_low > 30.0,
            StockIndicator.price_vs_sma_50_percent > 0.0,
            StockIndicator.price_vs_sma_150_percent > 0.0,
            StockIndicator.price_vs_sma_200_percent > 0.0,
            StockIndicator.sma_30w.isnot(None),
            StockIndicator.close > StockIndicator.sma_30w,
            StockIndicator.sma_40w.isnot(None),
            StockIndicator.close > StockIndicator.sma_40w,
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Trend - 2 Months',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
    }


# ============ SCREENER 3: TREND - 4 MONTHS ============
@router.get("/trend-4-months")
def get_trend_4_months(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Trend - 4 Months Screener

    Criteria (matches reference screenshot exactly):
    ✅ 50 Day > 150 Day: Yes
    ✅ 50 Day > 200 Day: Yes
    ✅ 150 Day > 200 Day: Yes
    ✅ 200 Day > 200 Day 4 Month Ago: Yes
    ✅ % Off 52 Wk High: > -25.00%
    ✅ RS 12M: > 69
    ✅ 200 Day 1M Ago > 200 Day 2M Ago: Yes  (sequential chain)
    ✅ 200 Day 2M Ago > 200 Day 3M Ago: Yes
    ✅ 200 Day 3M Ago > 200 Day 4M Ago: Yes
    ✅ % Off 52 Wk Low: > 30.00%
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 150d SMA: > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    ✅ Price Vs 30w SMA:  > 0.00%
    ✅ Price Vs 40w SMA:  > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    rs_map = get_rs_map(db, latest)
    rs_symbols = {sym for sym, rating in rs_map.items() if rating is not None and rating > 69}

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    query = query.filter(
        and_(
            StockIndicator.symbol.in_(rs_symbols),
            StockIndicator.sma_50 > StockIndicator.sma_150,
            StockIndicator.sma_50 > StockIndicator.sma_200,
            StockIndicator.sma_150 > StockIndicator.sma_200,
            StockIndicator.sma_200 > StockIndicator.sma_200_1m_ago,           # 200 > 200 1M ago
            StockIndicator.sma_200 > StockIndicator.sma_200_2m_ago,           # 200 > 200 2M ago
            StockIndicator.sma_200 > StockIndicator.sma_200_3m_ago,           # 200 > 200 3M ago
            StockIndicator.sma_200 > StockIndicator.sma_200_4m_ago,           # 200 > 200 4M ago
            StockIndicator.sma_200_1m_ago > StockIndicator.sma_200_2m_ago,   # chain: 1M > 2M
            StockIndicator.sma_200_2m_ago > StockIndicator.sma_200_3m_ago,   # chain: 2M > 3M
            StockIndicator.sma_200_3m_ago > StockIndicator.sma_200_4m_ago,   # chain: 3M > 4M
            StockIndicator.percent_off_52w_high > -25.0,
            StockIndicator.percent_off_52w_low > 30.0,
            StockIndicator.price_vs_sma_50_percent > 0.0,
            StockIndicator.price_vs_sma_150_percent > 0.0,
            StockIndicator.price_vs_sma_200_percent > 0.0,
            StockIndicator.sma_30w.isnot(None),
            StockIndicator.close > StockIndicator.sma_30w,
            StockIndicator.sma_40w.isnot(None),
            StockIndicator.close > StockIndicator.sma_40w,
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Trend - 4 Months',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
    }


# ============ SCREENER 4: TREND - 5 MONTHS ============
@router.get("/trend-5-months")
def get_trend_5_months(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Trend - 5 Months Screener

    Criteria (matches reference screenshot exactly):
    ✅ 50 Day > 150 Day: Yes
    ✅ 50 Day > 200 Day: Yes
    ✅ 150 Day > 200 Day: Yes
    ✅ 200 Day > 200 Day 5 Month Ago: Yes
    ✅ % Off 52 Wk High: > -25.00%
    ✅ RS 12M: > 69
    ✅ % Off 52 Wk Low: > 30.00%
    ✅ 200 Day 1M Ago > 200 Day 2M Ago: Yes  (full sequential chain)
    ✅ 200 Day 2M Ago > 200 Day 3M Ago: Yes
    ✅ 200 Day 3M Ago > 200 Day 4M Ago: Yes
    ✅ 200 Day 4M Ago > 200 Day 5M Ago: Yes
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 150d SMA: > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    ✅ Price Vs 30w SMA:  > 0.00%
    ✅ Price Vs 40w SMA:  > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    rs_map = get_rs_map(db, latest)
    rs_symbols = {sym for sym, rating in rs_map.items() if rating is not None and rating > 69}

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    query = query.filter(
        and_(
            StockIndicator.symbol.in_(rs_symbols),
            StockIndicator.sma_50 > StockIndicator.sma_150,
            StockIndicator.sma_50 > StockIndicator.sma_200,
            StockIndicator.sma_150 > StockIndicator.sma_200,
            StockIndicator.sma_200 > StockIndicator.sma_200_5m_ago,          # 200 > 200 5M ago
            StockIndicator.sma_200_1m_ago > StockIndicator.sma_200_2m_ago,   # chain: 1M > 2M
            StockIndicator.sma_200_2m_ago > StockIndicator.sma_200_3m_ago,   # chain: 2M > 3M
            StockIndicator.sma_200_3m_ago > StockIndicator.sma_200_4m_ago,   # chain: 3M > 4M
            StockIndicator.sma_200_4m_ago > StockIndicator.sma_200_5m_ago,   # chain: 4M > 5M
            StockIndicator.percent_off_52w_high > -25.0,
            StockIndicator.percent_off_52w_low > 30.0,
            StockIndicator.price_vs_sma_50_percent > 0.0,
            StockIndicator.price_vs_sma_150_percent > 0.0,
            StockIndicator.price_vs_sma_200_percent > 0.0,
            StockIndicator.sma_30w.isnot(None),
            StockIndicator.close > StockIndicator.sma_30w,
            StockIndicator.sma_40w.isnot(None),
            StockIndicator.close > StockIndicator.sma_40w,
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Trend - 5 Months',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
    }


# ============ SCREENER 5: TREND - 5 MONTHS WIDE ============
@router.get("/trend-5-months-wide")
def get_trend_5_months_wide(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Trend - 5 Months Wide Screener

    Criteria (matches reference screenshot exactly):
    ✅ 50 Day > 200 Day: Yes  (relaxed - no 150 required)
    ✅ 200 Day > 200 Day 5 Month Ago: Yes
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 150d SMA: > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    ✅ Price Vs 30w SMA:  > 0.00%
    ✅ Price Vs 40w SMA:  > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    rs_map = get_rs_map(db, latest)

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    query = query.filter(
        and_(
            StockIndicator.sma_50 > StockIndicator.sma_200,
            StockIndicator.sma_200 > StockIndicator.sma_200_5m_ago,
            StockIndicator.price_vs_sma_50_percent > 0.0,
            StockIndicator.price_vs_sma_150_percent > 0.0,
            StockIndicator.price_vs_sma_200_percent > 0.0,
            StockIndicator.sma_30w.isnot(None),
            StockIndicator.close > StockIndicator.sma_30w,
            StockIndicator.sma_40w.isnot(None),
            StockIndicator.close > StockIndicator.sma_40w,
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Trend - 5 Months Wide',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
    }


# ============ SCREENER 6: POWER PLAY ============
@router.get("/power-play")
def get_power_play(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    target_date: Optional[str] = Query(None)
):
    """
    🎯 Power Play Screener

    Criteria (matches reference screenshot exactly):
    ✅ % Change 20d:  > -25.00%
    ✅ % Change 15d:  -15.00% to 5.00%
    ✅ % Change 126d: > 85.00%
    ✅ Price Vs 50d SMA:  > 0.00%
    ✅ Price Vs 200d SMA: > 0.00%
    """
    if target_date:
        latest = target_date
    else:
        latest = get_latest_date(db)

    rs_map = get_rs_map(db, latest)

    query = db.query(StockIndicator).filter(StockIndicator.date == latest)

    # Power Play uses price_vs_sma percentages and percent_change columns
    # NULL guards ensure we only filter stocks that have calculated change data
    query = query.filter(
        and_(
            StockIndicator.price_vs_sma_50_percent.isnot(None),
            StockIndicator.price_vs_sma_200_percent.isnot(None),
            StockIndicator.percent_change_20d.isnot(None),
            StockIndicator.percent_change_15d.isnot(None),
            StockIndicator.percent_change_126d.isnot(None),
            StockIndicator.price_vs_sma_50_percent > 0.0,    # Price > 50d SMA
            StockIndicator.price_vs_sma_200_percent > 0.0,   # Price > 200d SMA
            StockIndicator.percent_change_20d > -25.0,        # % Change 20d > -25%
            StockIndicator.percent_change_15d >= -15.0,       # % Change 15d >= -15%
            StockIndicator.percent_change_15d <= 5.0,         # % Change 15d <= +5%
            StockIndicator.percent_change_126d > 85.0,        # % Change 126d > +85%
        )
    )

    total = query.count()
    results = query.order_by(StockIndicator.symbol).offset(offset).limit(limit).all()

    return {
        'screener': 'Power Play',
        'data': [screener_to_dict(ind, rs_map.get(ind.symbol)) for ind in results],
        'total': total,
        'count': len(results),
        'description': 'Power Play: أسهم أعلى من SMA 50 وSMA 200 مع شروط تغير السعر',
    }
