#!/usr/bin/env python
"""
التحقق من البيانات المحفوظة ليوم 2026-03-05
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text
from datetime import date

db = SessionLocal()

try:
    target_date = date(2026, 3, 5)
    
    print("="*80)
    print(f"✅ التحقق من البيانات المحفوظة ليوم {target_date}")
    print("="*80)
    print()
    
    # عدد الأسهم المحفوظة
    result = db.execute(text(f"SELECT COUNT(*) FROM prices WHERE date = '{target_date}'"))
    count = result.fetchone()[0]
    print(f"1️⃣  عدد الأسهم المحفوظة: {count}")
    print()
    
    # عرض أول 5 أسهم
    print(f"2️⃣  أول 5 أسهم:")
    print("-"*80)
    result = db.execute(text(f"""
    SELECT symbol, company_name, open, high, low, close, volume_traded
    FROM prices
    WHERE date = '{target_date}'
    ORDER BY symbol
    LIMIT 5
    """))
    
    for symbol, company, open_p, high, low, close, volume in result:
        print(f"   {symbol:6s} | {company:30s} | O:{open_p:8} H:{high:8} L:{low:8} C:{close:8}")
    
    print()
    
    # تحقق من السهم 1321 (الذي نختبر عليه)
    print(f"3️⃣  السهم 1321 تحديداً:")
    print("-"*80)
    result = db.execute(text(f"""
    SELECT symbol, company_name, open, high, low, close
    FROM prices
    WHERE date = '{target_date}' AND symbol = '1321'
    """))
    
    row = result.fetchone()
    if row:
        symbol, company, open_p, high, low, close = row
        print(f"   ✅ موجود: {symbol} | {company}")
        print(f"   Open: {open_p}  High: {high}  Low: {low}  Close: {close}")
    else:
        print(f"   ❌ السهم 1321 غير موجود!")
    
    print()
    print("="*80)
    print("✅ التحقق اكتمل - البيانات جاهزة لحساب المؤشرات")
    print("="*80)
    
finally:
    db.close()
