from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from typing import List, Optional, Dict, Any
from datetime import date

from app.core.database import get_db
from app.models.stock_indicators import StockIndicator
from app.models.price import Price

router = APIRouter()

@router.get("/technical-screener/screener")
def get_technical_screener_data(
    db: Session = Depends(get_db),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = None,
    min_score: Optional[int] = Query(None, ge=0),
    passing_only: bool = Query(False),
    latest_only: bool = Query(True),
    target_date: Optional[str] = Query(None, description="Filter by specific date (YYYY-MM-DD). Defaults to latest date.")
):
    """
    Returns technical screener rows with optional filtering + MA values from prices table.
    Endpoint path matches frontend: `/api/technical-screener/screener`.
    By default returns ONLY the latest date's data.
    
    JOIN with prices table to get: ema_10, ema_21, sma_50, sma_150, sma_200
    """
    # Start with JOIN: stock_indicators + prices
    query = db.query(StockIndicator, Price).join(
        Price,
        (StockIndicator.symbol == Price.symbol) & (StockIndicator.date == Price.date)
    )

    # Determine target date: explicit param > latest in DB
    result_date = None
    if target_date:
        query = query.filter(StockIndicator.date == target_date)
        result_date = target_date
    elif latest_only:
        latest = db.query(func.max(StockIndicator.date)).scalar()
        if latest:
            query = query.filter(StockIndicator.date == latest)
            result_date = str(latest)

    if symbol:
        query = query.filter(StockIndicator.symbol == symbol)

    if min_score is not None:
        query = query.filter(StockIndicator.score >= min_score)

    if passing_only:
        query = query.filter(StockIndicator.final_signal == True)

    # Sort by symbol for consistent ordering
    query = query.order_by(StockIndicator.symbol)

    total = query.count()

    results = query.offset(offset).limit(limit).all()

    return {
        'data': [indicator_to_dict_with_prices(ind, price) for ind, price in results],
        'total': total,
        'date': result_date
    }

def indicator_to_dict_with_prices(ind: StockIndicator, price: Price) -> dict:
    """✅ تحويل المؤشرات + البيانات من prices - مع MA values من prices table"""
    
    def safe_float(value):
        return float(value) if value is not None else None
    
    def safe_int(value):
        return int(value) if value is not None else 0
    
    def safe_bool(value):
        return bool(value) if value is not None else False
    
    # Build base dict from indicator_to_dict
    result = {
        # ============ Basic Info ============
        'id': ind.id,
        'symbol': ind.symbol,
        'company_name': ind.company_name,
        'date': str(ind.date) if ind.date else None,
        'close': safe_float(ind.close),
        
        # ============ MA VALUES FROM PRICES TABLE (with underscore - legacy) ============
        'ema_10': safe_float(price.ema_10) if price else None,
        'ema_21': safe_float(price.ema_21) if price else None,
        'sma_50': safe_float(price.sma_50) if price else None,
        'sma_150': safe_float(price.sma_150) if price else None,
        'sma_200': safe_float(price.sma_200) if price else None,
        
        # ============ PineScript-exact EMA (from stock_indicators) ============
        'ema10': safe_float(ind.ema10),
        'ema21': safe_float(ind.ema21),
        
        # ============ DAILY: RSI Components ============
        'rsi_14': safe_float(ind.rsi_14),
        'rsi_3': safe_float(ind.rsi_3),
        'sma9_rsi': safe_float(ind.sma9_rsi),
        'wma45_rsi': safe_float(ind.wma45_rsi),
        'ema45_rsi': safe_float(ind.ema45_rsi),                # ✅ كان ناقص
        'sma3_rsi3': safe_float(ind.sma3_rsi3),
        'ema20_sma3': safe_float(ind.ema20_sma3),              # ✅ كان ناقص
        
        # ============ DAILY: The Number Components ============
        'sma9_close': safe_float(ind.sma9_close),
        'high_sma13': safe_float(ind.high_sma13),              # ✅ كان ناقص
        'low_sma13': safe_float(ind.low_sma13),                # ✅ كان ناقص
        'high_sma65': safe_float(ind.high_sma65),              # ✅ كان ناقص
        'low_sma65': safe_float(ind.low_sma65),                # ✅ كان ناقص
        'the_number': safe_float(ind.the_number),
        'the_number_hl': safe_float(ind.the_number_hl),
        'the_number_ll': safe_float(ind.the_number_ll),
        
        # ============ DAILY: STAMP Components ============
        'rsi_14_9days_ago': safe_float(ind.rsi_14_9days_ago),
        'stamp_a_value': safe_float(ind.stamp_a_value),
        'stamp_s9rsi': safe_float(ind.stamp_s9rsi),
        'stamp_e45cfg': safe_float(ind.stamp_e45cfg),
        'stamp_e45rsi': safe_float(ind.stamp_e45rsi),
        'stamp_e20sma3': safe_float(ind.stamp_e20sma3),
        
        # ============ DAILY: CFG Analysis ============
        'cfg_daily': safe_float(ind.cfg_daily),
        'cfg_sma4': safe_float(ind.cfg_sma4),
        'cfg_sma9': safe_float(ind.cfg_sma9),
        'cfg_sma20': safe_float(ind.cfg_sma20),                # ✅ كان ناقص
        'cfg_ema20': safe_float(ind.cfg_ema20),
        'cfg_ema45': safe_float(ind.cfg_ema45),
        'cfg_wma45': safe_float(ind.cfg_wma45),
        
        # ============ DAILY: Trend Screener ============
        'sma4': safe_float(ind.sma4),
        'sma9': safe_float(ind.sma9),
        'sma18': safe_float(ind.sma18),
        'wma45_close': safe_float(ind.wma45_close),
        'cci': safe_float(ind.cci),
        'cci_ema20': safe_float(ind.cci_ema20),
        'aroon_up': safe_float(ind.aroon_up),
        'aroon_down': safe_float(ind.aroon_down),
        
        # ============ WEEKLY: RSI Components ============
        'rsi_w': safe_float(ind.rsi_w),                        # ✅ كان ناقص
        'rsi_3_w': safe_float(ind.rsi_3_w),                    # ✅ كان ناقص
        'sma9_rsi_w': safe_float(ind.sma9_rsi_w),
        'wma45_rsi_w': safe_float(ind.wma45_rsi_w),
        'ema45_rsi_w': safe_float(ind.ema45_rsi_w),            # ✅ كان ناقص
        'sma3_rsi3_w': safe_float(ind.sma3_rsi3_w),            # ✅ كان ناقص
        'ema20_sma3_w': safe_float(ind.ema20_sma3_w),          # ✅ كان ناقص
        
        # ============ WEEKLY: The Number Components ============
        'sma9_close_w': safe_float(ind.sma9_close_w),          # ✅ كان ناقص
        'high_sma13_w': safe_float(ind.high_sma13_w),          # ✅ كان ناقص
        'low_sma13_w': safe_float(ind.low_sma13_w),            # ✅ كان ناقص
        'high_sma65_w': safe_float(ind.high_sma65_w),          # ✅ كان ناقص
        'low_sma65_w': safe_float(ind.low_sma65_w),            # ✅ كان ناقص
        'the_number_w': safe_float(ind.the_number_w),          # ✅ كان ناقص
        'the_number_hl_w': safe_float(ind.the_number_hl_w),    # ✅ كان ناقص
        'the_number_ll_w': safe_float(ind.the_number_ll_w),    # ✅ كان ناقص
        
        # ============ WEEKLY: CFG Analysis ============
        'cfg_w': safe_float(ind.cfg_w),
        'cfg_sma4_w': safe_float(ind.cfg_sma4_w),              # ✅ كان ناقص
        'cfg_sma9_w': safe_float(ind.cfg_sma9_w),              # ✅ كان ناقص
        'cfg_ema20_w': safe_float(ind.cfg_ema20_w),
        'cfg_ema45_w': safe_float(ind.cfg_ema45_w),
        'cfg_wma45_w': safe_float(ind.cfg_wma45_w),
        
        # ============ WEEKLY: STAMP Components ============
        'rsi_14_9days_ago_w': safe_float(ind.rsi_14_9days_ago_w),
        'stamp_a_value_w': safe_float(ind.stamp_a_value_w),
        'stamp_s9rsi_w': safe_float(ind.stamp_s9rsi_w),
        'stamp_e45cfg_w': safe_float(ind.stamp_e45cfg_w),
        'stamp_e45rsi_w': safe_float(ind.stamp_e45rsi_w),
        'stamp_e20sma3_w': safe_float(ind.stamp_e20sma3_w),
        
        # ============ WEEKLY: Trend Screener ============
        'close_w': safe_float(ind.close_w),                    # ✅ كان ناقص
        'sma4_w': safe_float(ind.sma4_w),
        'sma9_w': safe_float(ind.sma9_w),
        'sma18_w': safe_float(ind.sma18_w),
        'wma45_close_w': safe_float(ind.wma45_close_w),
        'cci_w': safe_float(ind.cci_w),
        'cci_ema20_w': safe_float(ind.cci_ema20_w),
        'aroon_up_w': safe_float(ind.aroon_up_w),
        'aroon_down_w': safe_float(ind.aroon_down_w),
        
        # ============ Signal & Condition Flags ============
        'rsi_55_70': safe_bool(ind.rsi_55_70),
        'cfg_gt_50_daily': safe_bool(ind.cfg_gt_50_daily),
        'cfg_ema45_gt_50': safe_bool(ind.cfg_ema45_gt_50),
        'cfg_ema20_gt_50': safe_bool(ind.cfg_ema20_gt_50),
        'cfg_gt_50_w': safe_bool(ind.cfg_gt_50_w),
        'cfg_ema45_gt_50_w': safe_bool(ind.cfg_ema45_gt_50_w),
        'cfg_ema20_gt_50_w': safe_bool(ind.cfg_ema20_gt_50_w),
        'sma9_gt_tn_daily': safe_bool(ind.sma9_gt_tn_daily),
        'sma9_gt_tn_weekly': safe_bool(ind.sma9_gt_tn_weekly),
        'rsi_lt_80_d': safe_bool(ind.rsi_lt_80_d),
        'rsi_lt_80_w': safe_bool(ind.rsi_lt_80_w),
        'sma9_rsi_lte_75_d': safe_bool(ind.sma9_rsi_lte_75_d),
        'sma9_rsi_lte_75_w': safe_bool(ind.sma9_rsi_lte_75_w),
        'ema45_rsi_lte_70_d': safe_bool(ind.ema45_rsi_lte_70_d),
        'ema45_rsi_lte_70_w': safe_bool(ind.ema45_rsi_lte_70_w),
        'rsi_gt_wma45_d': safe_bool(ind.rsi_gt_wma45_d),
        'rsi_gt_wma45_w': safe_bool(ind.rsi_gt_wma45_w),
        'sma9rsi_gt_wma45rsi_d': safe_bool(ind.sma9rsi_gt_wma45rsi_d),
        'sma9rsi_gt_wma45rsi_w': safe_bool(ind.sma9rsi_gt_wma45rsi_w),
        'price_gt_sma18': safe_bool(ind.price_gt_sma18),
        'price_gt_sma9_weekly': safe_bool(ind.price_gt_sma9_weekly),
        'sma_trend_daily': safe_bool(ind.sma_trend_daily),
        'sma_trend_weekly': safe_bool(ind.sma_trend_weekly),
        'cci_gt_100': safe_bool(ind.cci_gt_100),
        'cci_ema20_gt_0_daily': safe_bool(ind.cci_ema20_gt_0_daily),
        'cci_ema20_gt_0_weekly': safe_bool(ind.cci_ema20_gt_0_weekly),
        'aroon_up_gt_70': safe_bool(ind.aroon_up_gt_70),
        'aroon_down_lt_30': safe_bool(ind.aroon_down_lt_30),
        'is_etf_or_index': safe_bool(ind.is_etf_or_index),
        'has_gap': safe_bool(ind.has_gap),
        'trend_signal': safe_bool(ind.trend_signal),
        
        # ============ MA COMPARISON CONDITIONS ============
        'ema10_gt_sma50': safe_bool(ind.ema10_gt_sma50),
        'ema10_gt_sma200': safe_bool(ind.ema10_gt_sma200),
        'ema21_gt_sma50': safe_bool(ind.ema21_gt_sma50),
        'ema21_gt_sma200': safe_bool(ind.ema21_gt_sma200),
        'sma50_gt_sma150': safe_bool(ind.sma50_gt_sma150),
        'sma50_gt_sma200': safe_bool(ind.sma50_gt_sma200),
        'sma150_gt_sma200': safe_bool(ind.sma150_gt_sma200),
        
        # ============ SMA200 TREND CONDITIONS ============
        'sma200_gt_sma200_1m_ago': safe_bool(ind.sma200_gt_sma200_1m_ago),
        'sma200_gt_sma200_2m_ago': safe_bool(ind.sma200_gt_sma200_2m_ago),
        'sma200_gt_sma200_3m_ago': safe_bool(ind.sma200_gt_sma200_3m_ago),
        'sma200_gt_sma200_4m_ago': safe_bool(ind.sma200_gt_sma200_4m_ago),
        'sma200_gt_sma200_5m_ago': safe_bool(ind.sma200_gt_sma200_5m_ago),
        
        # ============ Screener Results ============
        'stamp_daily': safe_bool(ind.stamp_daily),
        'stamp_weekly': safe_bool(ind.stamp_weekly),
        'stamp': safe_bool(ind.stamp),
        'final_signal': safe_bool(ind.final_signal),
        'score': safe_int(ind.score),
    }
    
    return result