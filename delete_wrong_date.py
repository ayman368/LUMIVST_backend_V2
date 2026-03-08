#!/usr/bin/env python
"""
حذف البيانات ليوم 8 (الخطأ) من prices و stock_indicators
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text
from datetime import date

db = SessionLocal()

try:
    target_date = date(2026, 3, 8)
    
    print("="*80)
    print(f"🗑️  حذف البيانات ليوم {target_date}")
    print("="*80)
    print()
    
    # حذف من prices
    print(f"1️⃣  حذف من جدول prices...")
    result = db.execute(text(f"DELETE FROM prices WHERE date = '{target_date}'"))
    db.commit()
    deleted_prices = result.rowcount
    print(f"   ✅ حذف {deleted_prices} سجل من prices")
    print()
    
    # حذف من stock_indicators
    print(f"2️⃣  حذف من جدول stock_indicators...")
    result = db.execute(text(f"DELETE FROM stock_indicators WHERE date = '{target_date}'"))
    db.commit()
    deleted_indicators = result.rowcount
    print(f"   ✅ حذف {deleted_indicators} سجل من stock_indicators")
    print()
    
    print("="*80)
    print("✅ تم الحذف بنجاح!")
    print("="*80)
    
finally:
    db.close()
