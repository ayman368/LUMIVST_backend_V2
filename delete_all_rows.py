#!/usr/bin/env python
"""
حذف جميع الصفوف من stock_indicators (Fresh Start - لا قيم خالص)
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    print("="*80)
    print("🗑️  DELETING ALL ROWS FROM stock_indicators (بدون أي قيم)")
    print("="*80)
    print()
    
    # عد الصفوف قبل الحذف
    result = db.execute(text('SELECT COUNT(*) FROM stock_indicators'))
    count_before = result.fetchone()[0]
    print(f"  قبل الحذف: {count_before} صفوف")
    
    # حذف جميع الصفوف
    print(f"\n  ⏳ جاري الحذف...")
    result = db.execute(text('DELETE FROM stock_indicators'))
    db.commit()
    
    deleted = result.rowcount
    print(f"  ✅ تم حذف: {deleted} صفوف")
    print()
    
    # تحقق من الحذف
    result = db.execute(text('SELECT COUNT(*) FROM stock_indicators'))
    count_after = result.fetchone()[0]
    print(f"  بعد الحذف: {count_after} صفوف")
    
    print()
    print("="*80)
    print("✅ DONE! الجدول فارغ تماماً - جاهز للبيانات الجديدة من الصفر")
    print("="*80)
    
finally:
    db.close()
