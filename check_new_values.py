#!/usr/bin/env python
"""
تحقق من القيم الجديدة بعد إعادة الحساب
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    result = db.execute(text('''
    SELECT rsi_14, rsi_w, sma9_rsi, sma9_rsi_w
    FROM stock_indicators
    WHERE symbol = '1321' AND date = '2026-03-05'
    '''))
    
    row = result.fetchone()
    
    print("="*60)
    print("📊 النتائج الجديدة:")
    print("="*60)
    
    if row:
        print(f"RSI(14) Daily:     {row[0] or 'NULL'}")
        print(f"RSI(14) Weekly:    {row[1] or 'NULL'}")
        print(f"SMA9(RSI) Daily:   {row[2] or 'NULL'}")
        print(f"SMA9(RSI) Weekly:  {row[3] or 'NULL'}")
    else:
        print("❌ لا توجد بيانات")
    
    print()
    print("="*60)
    print("📊 TradingView المتوقع:")
    print("="*60)
    print(f"RSI(14):           37.14")
    print(f"SMA9(RSI):         38.53")
    
finally:
    db.close()
