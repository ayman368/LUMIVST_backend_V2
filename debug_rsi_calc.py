#!/usr/bin/env python
"""
فحص البيانات والحساب اليدوي للـ RSI
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text
import numpy as np

db = SessionLocal()

try:
    # جلب آخر 30 شمعة للسهم 1321
    result = db.execute(text('''
    SELECT date, close FROM prices
    WHERE symbol = '1321'
    ORDER BY date ASC
    '''))
    
    rows = result.fetchall()
    dates = [row[0] for row in rows]
    closes = [float(row[1]) for row in rows]
    
    print("="*80)
    print(f"📊 إجمالي الشمعات: {len(closes)}")
    print("="*80)
    print()
    
    # عرض آخر 20 شمعة
    print("آخر 20 شمعة:")
    print("="*80)
    for i in range(max(0, len(closes)-20), len(closes)):
        print(f"{i+1:3d} | {dates[i]} | Close: {closes[i]:.4f}")
    
    print()
    print("="*80)
    print("🔍 حساب RSI(14) يدويً:")
    print("="*80)
    
    # حساب RSI يدوي
    prices = np.array(closes, dtype=float)
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    
    period = 14
    
    # أول RSI في الموضع 15 (index 14)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    print(f"أول {period} شمعات:")
    print(f"  Average Gain: {avg_gain:.6f}")
    print(f"  Average Loss: {avg_loss:.6f}")
    
    if avg_loss == 0:
        rsi_1 = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_1 = 100.0 - (100.0 / (1.0 + rs))
    
    print(f"  RSI(1st): {rsi_1:.2f}")
    print()
    
    # حساب RSI الأخير (آخر شمعة - 2026-03-05)
    alpha = 1.0 / period
    avg_gain_current = avg_gain
    avg_loss_current = avg_loss
    
    for i in range(period, len(prices)-1):
        avg_gain_current = avg_gain_current * (1 - alpha) + gains[i] * alpha
        avg_loss_current = avg_loss_current * (1 - alpha) + losses[i] * alpha
    
    if avg_loss_current == 0:
        rsi_last = 100.0
    else:
        rs = avg_gain_current / avg_loss_current
        rsi_last = 100.0 - (100.0 / (1.0 + rs))
    
    print(f"آخر شمعة (2026-03-05):")
    print(f"  Average Gain: {avg_gain_current:.6f}")
    print(f"  Average Loss: {avg_loss_current:.6f}")
    print(f"  RSI(Last): {rsi_last:.2f}")
    print()
    
    # جلب القيمة من DB
    result = db.execute(text('''
    SELECT rsi_14 FROM stock_indicators
    WHERE symbol = '1321' AND date = '2026-03-05'
    '''))
    db_rsi = result.fetchone()
    
    if db_rsi and db_rsi[0]:
        print(f"✅ RSI من DB: {float(db_rsi[0]):.2f}")
        print(f"❌ RSI يدوي: {rsi_last:.2f}")
        print(f"❓ الفرق: {abs(float(db_rsi[0]) - rsi_last):.2f}")
    
finally:
    db.close()
