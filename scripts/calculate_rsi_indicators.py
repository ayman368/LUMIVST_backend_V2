import sys
import os
import numpy as np
import pandas as pd
from decimal import Decimal
from datetime import date
from typing import List, Optional, Any, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def convert_to_float(value):
    """Convert value to float, handling Decimal and None values"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_val(lst, i):
    """Safely get value from list or return the value itself if it's a scalar"""
    # إذا كانت القيمة None
    if lst is None:
        return None
    
    # إذا كانت القيمة مفردة (float, int, str, bool) وليس قائمة
    if not isinstance(lst, (list, tuple, np.ndarray, pd.Series)):
        return lst  # رجع القيمة كما هي
    
    # إذا كانت قائمة
    if i < 0 or i >= len(lst):
        return None
    val = lst[i]
    if val is None:
        return None
    if isinstance(val, (float, np.floating)):
        if np.isnan(val):
            return None
    return val


def calculate_rsi_pinescript(values: List[float], period: int = 14) -> List[Optional[float]]:
    """✅ RSI مطابق تماماً لـ PineScript باستخدام RMA (Wilder's Smoothing)"""
    if not values or len(values) < period + 1:
        return [None] * len(values) if values else []
    
    prices = np.array(values, dtype=float)
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    
    avg_gain = np.full(len(prices), np.nan)
    avg_loss = np.full(len(prices), np.nan)
    
    avg_gain[period] = np.mean(gains[:period])
    avg_loss[period] = np.mean(losses[:period])
    
    alpha = 1.0 / period
    for i in range(period + 1, len(prices)):
        avg_gain[i] = avg_gain[i-1] * (1 - alpha) + gains[i-1] * alpha
        avg_loss[i] = avg_loss[i-1] * (1 - alpha) + losses[i-1] * alpha
    
    rsi_values = []
    for i in range(len(prices)):
        if i < period:
            rsi_values.append(None)
        elif np.isnan(avg_gain[i]) or np.isnan(avg_loss[i]):
            rsi_values.append(None)
        elif avg_loss[i] == 0:
            rsi_values.append(100.0)
        else:
            rs = avg_gain[i] / avg_loss[i]
            rsi = 100.0 - (100.0 / (1.0 + rs))
            rsi_values.append(rsi)
    
    return rsi_values


def calculate_sma(values: List[float], period: int) -> List[Optional[float]]:
    """Simple Moving Average"""
    if not values or len(values) < period:
        return [None] * len(values) if values else []
    s = pd.Series(values)
    sma = s.rolling(window=period, min_periods=period).mean()
    return [float(x) if not pd.isna(x) else None for x in sma.tolist()]


def calculate_wma(values: List[float], period: int) -> List[Optional[float]]:
    """Weighted Moving Average"""
    if not values or len(values) < period:
        return [None] * len(values) if values else []
    
    s = pd.Series(values)
    weights = np.arange(1, period + 1)
    
    def wma_calc(x):
        if len(x) < period:
            return np.nan
        return np.dot(x, weights[:len(x)]) / weights[:len(x)].sum()
        
    wma = s.rolling(window=period, min_periods=period).apply(wma_calc, raw=True)
    return [float(x) if not pd.isna(x) else None for x in wma.tolist()]


def calculate_ema(values: List[float], period: int) -> List[Optional[float]]:
    """Exponential Moving Average"""
    alpha = 2.0 / (period + 1.0)
    ema_vals = []
    
    vals = [v if v is not None and not pd.isna(v) else np.nan for v in values]
    ema_prev = np.nan
    
    for i in range(len(vals)):
        # لا يمكن الحساب إذا كانت القيمة الحالية NaN
        if np.isnan(vals[i]):
            ema_vals.append(None)
            continue
            
        if np.isnan(ema_prev):
            # TradingView يبدأ حساب EMA باستخدام SMA كأول نقطة أساس عندما تكتمل فترة period
            # نتحقق من النافذة السابقة
            window = vals[max(0, i - period + 1) : i + 1]
            if len(window) == period and not np.any(np.isnan(window)):
                ema_prev = np.mean(window)
                ema_vals.append(float(ema_prev))
            else:
                ema_vals.append(None)
        else:
            ema_prev = alpha * vals[i] + (1 - alpha) * ema_prev
            ema_vals.append(float(ema_prev))
            
    return ema_vals


def calculate_rsi_components(closes: List[float]) -> Dict[str, Any]:
    """Calculate all RSI related components"""
    
    # RSI Calculations
    rsi_14 = calculate_rsi_pinescript(closes, 14)
    rsi_3 = calculate_rsi_pinescript(closes, 3)
    
    # Moving Averages of RSI
    sma9_rsi = calculate_sma(rsi_14, 9)
    wma45_rsi = calculate_wma(rsi_14, 45)
    ema45_rsi = calculate_ema(rsi_14, 45)
    
    # SMA3 of RSI3
    sma3_rsi3 = calculate_sma(rsi_3, 3)
    ema20_sma3 = calculate_ema(sma3_rsi3, 20)
    
    return {
        'rsi_14': rsi_14,
        'rsi_3': rsi_3,
        'sma9_rsi': sma9_rsi,
        'wma45_rsi': wma45_rsi,
        'ema45_rsi': ema45_rsi,
        'sma3_rsi3': sma3_rsi3,
        'ema20_sma3': ema20_sma3
    }


def get_rsi_current_values(components: Dict[str, Any], idx: int) -> Dict[str, Any]:
    """Get current RSI values at specified index"""
    return {
        'rsi_14': get_val(components['rsi_14'], idx),
        'rsi_3': get_val(components['rsi_3'], idx),
        'sma9_rsi': get_val(components['sma9_rsi'], idx),
        'wma45_rsi': get_val(components['wma45_rsi'], idx),
        'ema45_rsi': get_val(components['ema45_rsi'], idx),
        'sma3_rsi3': get_val(components['sma3_rsi3'], idx),
        'ema20_sma3': get_val(components['ema20_sma3'], idx),
    }