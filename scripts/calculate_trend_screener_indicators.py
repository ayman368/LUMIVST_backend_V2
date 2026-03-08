"""
Trend Screener Indicators - مطابق تماماً لـ Pine Script
يشمل: المتوسطات المتحركة، CCI، Aroon
"""

import sys
import os
import numpy as np
import pandas as pd
from decimal import Decimal
from datetime import date
from typing import List, Optional, Any, Dict, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# استيراد الدوال المساعدة من ملف RSI
from scripts.calculate_rsi_indicators import convert_to_float, get_val, calculate_sma, calculate_wma, calculate_ema


def calculate_cci_pinescript_exact(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[Optional[float]]:
    """
    ✅ CCI مطابق تماماً لـ Pine Script
    formula: (tp - sma(tp, period)) / (0.015 * mean(abs(tp - sma(tp, period))))
    """
    if not highs or not lows or not closes or len(highs) < period:
        return [None] * len(highs) if highs else []
    
    # تحويل إلى numpy arrays
    highs_arr = np.array([float(h) if h is not None else np.nan for h in highs])
    lows_arr = np.array([float(l) if l is not None else np.nan for l in lows])
    closes_arr = np.array([float(c) if c is not None else np.nan for c in closes])
    
    # Typical Price
    tp = (highs_arr + lows_arr + closes_arr) / 3
    
    cci_values = []
    
    for i in range(len(tp)):
        if i < period - 1 or np.isnan(tp[i]):
            cci_values.append(None)
            continue
        
        # SMA of TP for the period
        tp_window = tp[i-period+1:i+1]
        if np.any(np.isnan(tp_window)):
            cci_values.append(None)
            continue
            
        sma_tp = np.mean(tp_window)
        
        # Mean Deviation
        mean_dev = np.mean(np.abs(tp_window - sma_tp))
        
        if mean_dev == 0:
            cci_values.append(0.0)
        else:
            cci = (tp[i] - sma_tp) / (0.015 * mean_dev)
            cci_values.append(float(cci))
    
    return cci_values


def calculate_aroon_pinescript_exact(highs: List[float], lows: List[float], period: int = 25) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    """
    ✅ Aroon مطابق تماماً لـ Pine Script:
    aroonUp = 100 * (period - barssince(high == highest(high, period))) / period
    aroonDown = 100 * (period - barssince(low == lowest(low, period))) / period
    
    - نبحث عن أحدث ظهور (من الحديث للقديم) للقيمة العليا/الدنيا في النطاق
    - argmax على المصفوفة المعكوسة يعطينا عدد الأيام منذ الحدوث الأخير
    """
    if not highs or not lows or len(highs) < period:
        return [None] * len(highs) if highs else [], [None] * len(highs) if highs else []
    
    # Convert to numpy arrays for better performance
    highs_arr = np.array([float(h) if h is not None else np.nan for h in highs])
    lows_arr = np.array([float(l) if l is not None else np.nan for l in lows])
    
    aroon_up = []
    aroon_down = []
    
    for i in range(len(highs_arr)):
        # Aroon window needs to be 'period + 1' elements to reach exactly 'period' bars back
        if i < period:
            aroon_up.append(None)
            aroon_down.append(None)
            continue
        
        # Window of period + 1 bars (e.g. 26 bars for period 25, indices i-period to i)
        window_high = highs_arr[i - period: i + 1]
        window_low  = lows_arr[i - period: i + 1]
        
        if np.any(np.isnan(window_high)) or np.any(np.isnan(window_low)):
            aroon_up.append(None)
            aroon_down.append(None)
            continue
        
        # Aroon Up: Find offset of HIGHEST high. If tie, take the MOST RECENT (rightmost)
        max_val = np.max(window_high)
        max_indices = np.where(window_high == max_val)[0]
        recent_occurrence = max_indices[-1]  # rightmost index
        days_since_high = (len(window_high) - 1) - recent_occurrence
        
        # Aroon Down: Find offset of LOWEST low. If tie, take the MOST RECENT (rightmost)
        min_val = np.min(window_low)
        min_indices = np.where(window_low == min_val)[0]
        recent_occurrence_low = min_indices[-1]
        days_since_low = (len(window_low) - 1) - recent_occurrence_low
        
        aroon_up_val = 100.0 * (period - days_since_high) / period
        aroon_down_val = 100.0 * (period - days_since_low) / period
        
        aroon_up.append(float(aroon_up_val))
        aroon_down.append(float(aroon_down_val))
    
    return aroon_up, aroon_down


def calculate_price_moving_averages(closes: List[float]) -> Dict[str, Any]:
    """
    حساب جميع المتوسطات المتحركة للسعر
    """
    return {
        # Short Term
        'ema10': calculate_ema(closes, 10),      # ✅ EMA10
        'ema21': calculate_ema(closes, 21),      # ✅ EMA21
        'sma4': calculate_sma(closes, 4),
        'sma9': calculate_sma(closes, 9),
        'sma18': calculate_sma(closes, 18),
        'wma45': calculate_wma(closes, 45),      # ✅ WMA صحيحة
        # Medium & Long Term
        'sma50': calculate_sma(closes, 50),      # ✅ SMA50
        'sma150': calculate_sma(closes, 150),    # ✅ SMA150
        'sma200': calculate_sma(closes, 200),    # ✅ SMA200
    }


def calculate_trend_components(highs: List[float], lows: List[float], closes: List[float], symbol: str = "") -> Dict[str, Any]:
    """
    حساب جميع مكونات مؤشر التريند
    
    Args:
        highs: قائمة الأسعار العليا اليومية
        lows: قائمة الأسعار الدنيا اليومية
        closes: قائمة أسعار الإغلاق اليومية
        symbol: رمز السهم (للتأكد من عدم وجود أخطاء)
    
    Returns:
        قاموس يحتوي على جميع المكونات المحسوبة
    """
    
    # 1. المتوسطات المتحركة للسعر (Daily)
    price_mas = calculate_price_moving_averages(closes)
    
    # 2. CCI (14) مع EMA20
    cci = calculate_cci_pinescript_exact(highs, lows, closes, 14)
    cci_ema20 = calculate_ema(cci, 20)
    
    # 3. Aroon (25)
    aroon_up, aroon_down = calculate_aroon_pinescript_exact(highs, lows, 25)
    
    return {
        # Daily Moving Averages
        'ema10': price_mas['ema10'],           # ✅ EMA10
        'ema21': price_mas['ema21'],           # ✅ EMA21
        'sma50': price_mas['sma50'],           # ✅ SMA50
        'sma150': price_mas['sma150'],         # ✅ SMA150
        'sma200': price_mas['sma200'],         # ✅ SMA200
        'sma4': price_mas['sma4'],
        'sma9': price_mas['sma9'],
        'sma18': price_mas['sma18'],
        'wma45_close': price_mas['wma45'],
        
        # CCI
        'cci': cci,
        'cci_ema20': cci_ema20,
        
        # Aroon
        'aroon_up': aroon_up,
        'aroon_down': aroon_down,
    }


def calculate_weekly_components(df_weekly: pd.DataFrame) -> Dict[str, Any]:
    """
    حساب المؤشرات الأسبوعية (قيم مفردة للشمعة الأخيرة + سلاسل كاملة)
    
    Args:
        df_weekly: DataFrame أسبوعي يحتوي على open, high, low, close
    
    Returns:
        قاموس يحتوي على القيم الأسبوعية (مفردة وسلاسل)
    """
    
    # استخراج السلاسل الزمنية
    closes_w = df_weekly['close'].tolist()
    highs_w = df_weekly['high'].tolist()
    lows_w = df_weekly['low'].tolist()
    
    # 1. المتوسطات المتحركة الأسبوعية
    sma4_w_series = calculate_sma(closes_w, 4)
    sma9_w_series = calculate_sma(closes_w, 9)
    sma18_w_series = calculate_sma(closes_w, 18)
    wma45_w_series = calculate_wma(closes_w, 45)
    
    # 2. CCI الأسبوعي
    cci_w_series = calculate_cci_pinescript_exact(highs_w, lows_w, closes_w, 14)
    cci_ema20_w_series = calculate_ema(cci_w_series, 20)
    
    # 3. Aroon الأسبوعي
    aroon_up_w_series, aroon_down_w_series = calculate_aroon_pinescript_exact(highs_w, lows_w, 25)
    
    # الحصول على آخر قيمة (القيمة الحالية)
    last_idx = len(closes_w) - 1 if closes_w else -1
    
    return {
        # القيم المفردة (للشمعة الحالية)
        'close_w': closes_w[-1] if closes_w else None,
        'sma4_w': sma4_w_series[-1] if sma4_w_series and len(sma4_w_series) > last_idx else None,
        'sma9_w': sma9_w_series[-1] if sma9_w_series and len(sma9_w_series) > last_idx else None,
        'sma18_w': sma18_w_series[-1] if sma18_w_series and len(sma18_w_series) > last_idx else None,
        'wma45_close_w': wma45_w_series[-1] if wma45_w_series and len(wma45_w_series) > last_idx else None,
        'cci_w': cci_w_series[-1] if cci_w_series and len(cci_w_series) > last_idx else None,
        'cci_ema20_w': cci_ema20_w_series[-1] if cci_ema20_w_series and len(cci_ema20_w_series) > last_idx else None,
        'aroon_up_w': aroon_up_w_series[-1] if aroon_up_w_series and len(aroon_up_w_series) > last_idx else None,
        'aroon_down_w': aroon_down_w_series[-1] if aroon_down_w_series and len(aroon_down_w_series) > last_idx else None,
        
        # السلاسل الكاملة (للحسابات المستقبلية)
        'sma4_w_series': sma4_w_series,
        'sma9_w_series': sma9_w_series,
        'sma18_w_series': sma18_w_series,
        'wma45_w_series': wma45_w_series,
        'cci_w_series': cci_w_series,
        'cci_ema20_w_series': cci_ema20_w_series,
        'aroon_up_w_series': aroon_up_w_series,
        'aroon_down_w_series': aroon_down_w_series,
    }


def calculate_trend_conditions(
    daily_components: Dict[str, Any],
    weekly_components: Dict[str, Any],
    idx: int,
    w_idx: int,
    symbol: str,
    df: pd.DataFrame
) -> Dict[str, Any]:
    """
    حساب شروط مؤشر التريند
    
    Args:
        daily_components: مكونات المؤشرات اليومية
        weekly_components: مكونات المؤشرات الأسبوعية
        idx: المؤشر الحالي في البيانات اليومية
        w_idx: المؤشر الحالي في البيانات الأسبوعية
        symbol: رمز السهم
        df: DataFrame البيانات اليومية
    
    Returns:
        قاموس يحتوي على جميع الشروط المحسوبة
    """
    
    # التحقق من أن المؤشرات ضمن النطاق
    closes = df['close'].tolist() if 'close' in df.columns else []
    
    # 1. شرط السعر > SMA18 اليومي
    sma18_val = get_val(daily_components.get('sma18', []), idx)
    close_val = get_val(closes, idx)
    price_gt_sma18 = False
    if close_val is not None and sma18_val is not None:
        price_gt_sma18 = close_val > sma18_val
    
    # 2. شرط السعر > SMA9 الأسبوعي
    close_w_val = weekly_components.get('close_w')
    sma9_w_val = weekly_components.get('sma9_w')
    price_gt_sma9_weekly = False
    if close_w_val is not None and sma9_w_val is not None:
        price_gt_sma9_weekly = close_w_val > sma9_w_val
    
    # 3. شرط ترتيب المتوسطات اليومي: SMA4 > SMA9 > SMA18
    sma4_val = get_val(daily_components.get('sma4', []), idx)
    sma9_val = get_val(daily_components.get('sma9', []), idx)
    sma18_val = get_val(daily_components.get('sma18', []), idx)
    sma_trend_daily = False
    if all(v is not None for v in [sma4_val, sma9_val, sma18_val]):
        sma_trend_daily = sma4_val > sma9_val and sma9_val > sma18_val
    
    # 4. شرط ترتيب المتوسطات الأسبوعي
    sma4_w_val = weekly_components.get('sma4_w')
    sma9_w_val2 = weekly_components.get('sma9_w')
    sma18_w_val = weekly_components.get('sma18_w')
    sma_trend_weekly = False
    if all(v is not None for v in [sma4_w_val, sma9_w_val2, sma18_w_val]):
        sma_trend_weekly = sma4_w_val > sma9_w_val2 and sma9_w_val2 > sma18_w_val
    
    # 5. شرط CCI > 100
    cci_val = get_val(daily_components.get('cci', []), idx)
    cci_gt_100 = cci_val is not None and cci_val > 100
    
    # 6. شرط CCI EMA20 > 0 (Daily)
    cci_ema20_val = get_val(daily_components.get('cci_ema20', []), idx)
    cci_ema20_gt_0_daily = cci_ema20_val is not None and cci_ema20_val > 0
    
    # 7. شرط CCI EMA20 > 0 (Weekly)
    cci_ema20_w_val = weekly_components.get('cci_ema20_w')
    cci_ema20_gt_0_weekly = cci_ema20_w_val is not None and cci_ema20_w_val > 0
    
    # 8. شرط Aroon Up > 70%
    aroon_up_val = get_val(daily_components.get('aroon_up', []), idx)
    aroon_up_gt_70 = aroon_up_val is not None and aroon_up_val > 70
    
    # 9. شرط Aroon Down < 30%
    aroon_down_val = get_val(daily_components.get('aroon_down', []), idx)
    aroon_down_lt_30 = aroon_down_val is not None and aroon_down_val < 30
    
    # 10. اكتشاف ETF/Index
    is_etf_or_index = 'INDEX' in symbol or 'ETF' in symbol
    
    # 11. اكتشاف الفجوات السعرية
    has_gap = False
    if idx > 0 and idx < len(df):
        current_open = df.iloc[idx]['open'] if 'open' in df.columns else None
        if current_open is not None and current_open > 0:
            prev_close = df.iloc[idx - 1]['close'] if idx - 1 < len(df) else None
            if prev_close and prev_close > 0:
                gap_percent = abs((current_open - prev_close) / prev_close)
                has_gap = gap_percent > 0.03  # 3% gap
    
    # 12. الإشارة النهائية
    valid_signal = (
        price_gt_sma18 and 
        price_gt_sma9_weekly and
        sma_trend_daily and 
        sma_trend_weekly and
        cci_gt_100 and 
        cci_ema20_gt_0_daily and 
        cci_ema20_gt_0_weekly and
        aroon_up_gt_70 and 
        aroon_down_lt_30
    )
    
    # تطبيق الفلاتر
    trend_signal = valid_signal and not is_etf_or_index and not has_gap
    
    # ============ MA COMPARISON CONDITIONS ============
    # الحصول على قيم MA في الموضع الحالي
    ema10_val = get_val(daily_components.get('ema10', []), idx)
    ema21_val = get_val(daily_components.get('ema21', []), idx)
    sma50_val = get_val(daily_components.get('sma50', []), idx)
    sma150_val = get_val(daily_components.get('sma150', []), idx)
    sma200_val = get_val(daily_components.get('sma200', []), idx)
    
    # MA Comparisons
    ema10_gt_sma50 = ema10_val is not None and sma50_val is not None and ema10_val > sma50_val
    ema10_gt_sma200 = ema10_val is not None and sma200_val is not None and ema10_val > sma200_val
    ema21_gt_sma50 = ema21_val is not None and sma50_val is not None and ema21_val > sma50_val
    ema21_gt_sma200 = ema21_val is not None and sma200_val is not None and ema21_val > sma200_val
    sma50_gt_sma150 = sma50_val is not None and sma150_val is not None and sma50_val > sma150_val
    sma50_gt_sma200 = sma50_val is not None and sma200_val is not None and sma50_val > sma200_val
    sma150_gt_sma200 = sma150_val is not None and sma200_val is not None and sma150_val > sma200_val
    
    # ============ 200SMA TREND CONDITIONS (مقارنة مع الأشهر السابقة) ============
    sma200_gt_sma200_1m_ago = False
    sma200_gt_sma200_2m_ago = False
    sma200_gt_sma200_3m_ago = False
    sma200_gt_sma200_4m_ago = False
    sma200_gt_sma200_5m_ago = False
    
    if sma200_val is not None:
        sma200_array = daily_components.get('sma200', [])
        # كل شهر ≈ 21 يوم
        if idx >= 21:
            sma200_1m = get_val(sma200_array, idx - 21)
            sma200_gt_sma200_1m_ago = sma200_1m is not None and sma200_val > sma200_1m
        if idx >= 42:
            sma200_2m = get_val(sma200_array, idx - 42)
            sma200_gt_sma200_2m_ago = sma200_2m is not None and sma200_val > sma200_2m
        if idx >= 63:
            sma200_3m = get_val(sma200_array, idx - 63)
            sma200_gt_sma200_3m_ago = sma200_3m is not None and sma200_val > sma200_3m
        if idx >= 84:
            sma200_4m = get_val(sma200_array, idx - 84)
            sma200_gt_sma200_4m_ago = sma200_4m is not None and sma200_val > sma200_4m
        if idx >= 105:
            sma200_5m = get_val(sma200_array, idx - 105)
            sma200_gt_sma200_5m_ago = sma200_5m is not None and sma200_val > sma200_5m
    
    return {
        # الشروط الأساسية
        'price_gt_sma18': price_gt_sma18,
        'price_gt_sma9_weekly': price_gt_sma9_weekly,
        'sma_trend_daily': sma_trend_daily,
        'sma_trend_weekly': sma_trend_weekly,
        'cci_gt_100': cci_gt_100,
        'cci_ema20_gt_0_daily': cci_ema20_gt_0_daily,
        'cci_ema20_gt_0_weekly': cci_ema20_gt_0_weekly,
        'aroon_up_gt_70': aroon_up_gt_70,
        'aroon_down_lt_30': aroon_down_lt_30,
        
        # ✅ MA COMPARISON CONDITIONS
        'ema10_gt_sma50': ema10_gt_sma50,
        'ema10_gt_sma200': ema10_gt_sma200,
        'ema21_gt_sma50': ema21_gt_sma50,
        'ema21_gt_sma200': ema21_gt_sma200,
        'sma50_gt_sma150': sma50_gt_sma150,
        'sma50_gt_sma200': sma50_gt_sma200,
        'sma150_gt_sma200': sma150_gt_sma200,
        
        # ✅ 200SMA TREND CONDITIONS
        'sma200_gt_sma200_1m_ago': sma200_gt_sma200_1m_ago,
        'sma200_gt_sma200_2m_ago': sma200_gt_sma200_2m_ago,
        'sma200_gt_sma200_3m_ago': sma200_gt_sma200_3m_ago,
        'sma200_gt_sma200_4m_ago': sma200_gt_sma200_4m_ago,
        'sma200_gt_sma200_5m_ago': sma200_gt_sma200_5m_ago,
        
        # الفلاتر
        'is_etf_or_index': is_etf_or_index,
        'has_gap': has_gap,
        
        # الإشارة النهائية
        'trend_signal': trend_signal,
        'valid_signal': valid_signal,
    }


def get_trend_current_values(
    daily_components: Dict[str, Any],
    weekly_components: Dict[str, Any],
    idx: int
) -> Dict[str, Any]:
    """
    الحصول على القيم الحالية لمؤشرات التريند
    
    Args:
        daily_components: مكونات المؤشرات اليومية
        weekly_components: مكونات المؤشرات الأسبوعية
        idx: المؤشر الحالي
    
    Returns:
        قاموس بالقيم الحالية
    """
    return {
        # Daily values
        'ema10': get_val(daily_components.get('ema10', []), idx),      # ✅ EMA10
        'ema21': get_val(daily_components.get('ema21', []), idx),      # ✅ EMA21
        'sma50': get_val(daily_components.get('sma50', []), idx),      # ✅ SMA50
        'sma150': get_val(daily_components.get('sma150', []), idx),    # ✅ SMA150
        'sma200': get_val(daily_components.get('sma200', []), idx),    # ✅ SMA200
        'sma4': get_val(daily_components.get('sma4', []), idx),
        'sma9': get_val(daily_components.get('sma9', []), idx),
        'sma18': get_val(daily_components.get('sma18', []), idx),
        'wma45_close': get_val(daily_components.get('wma45_close', []), idx),
        'cci': get_val(daily_components.get('cci', []), idx),
        'cci_ema20': get_val(daily_components.get('cci_ema20', []), idx),
        'aroon_up': get_val(daily_components.get('aroon_up', []), idx),
        'aroon_down': get_val(daily_components.get('aroon_down', []), idx),
        
        # Weekly values (singles - already extracted)
        'sma4_w': weekly_components.get('sma4_w'),
        'sma9_w': weekly_components.get('sma9_w'),
        'sma18_w': weekly_components.get('sma18_w'),
        'wma45_close_w': weekly_components.get('wma45_close_w'),
        'close_w': weekly_components.get('close_w'),
        'cci_w': weekly_components.get('cci_w'),
        'cci_ema20_w': weekly_components.get('cci_ema20_w'),
        'aroon_up_w': weekly_components.get('aroon_up_w'),
        'aroon_down_w': weekly_components.get('aroon_down_w'),
    }