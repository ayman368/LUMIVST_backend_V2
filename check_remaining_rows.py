#!/usr/bin/env python
"""
التحقق من عدد الصفوف الموجودة
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    # عدد الصفوف الكلي
    result = db.execute(text('SELECT COUNT(*) FROM stock_indicators'))
    total = result.fetchone()[0]
    
    print(f'✅ إجمالي الصفوف في stock_indicators: {total}')
    
    # نطاق التواريخ
    result = db.execute(text('SELECT MIN(date), MAX(date) FROM stock_indicators'))
    min_date, max_date = result.fetchone()
    print(f'✅ نطاق التواريخ: {min_date} إلى {max_date}')
    
    # عدد الأسهم
    result = db.execute(text('SELECT COUNT(DISTINCT symbol) FROM stock_indicators'))
    symbols = result.fetchone()[0]
    print(f'✅ عدد الأسهم الفريدة: {symbols}')
    
    # آخر 5 تواريخ
    result = db.execute(text('SELECT DISTINCT date FROM stock_indicators ORDER BY date DESC LIMIT 5'))
    dates = [row[0] for row in result.fetchall()]
    print(f'✅ آخر 5 تواريخ: {dates}')
    
finally:
    db.close()
