#!/usr/bin/env python
"""
عرض البيانات المحفوظة لفهم المشكلة
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    # تحقق من البيانات المحفوظة
    result = db.execute(text('''
    SELECT date, open, high, low, close 
    FROM prices 
    WHERE symbol = '1321' 
    ORDER BY date DESC 
    LIMIT 5
    '''))
    
    print("="*80)
    print("📊 آخر 5 بيانات للسهم 1321")
    print("="*80)
    
    for date, open_p, high, low, close in result:
        print(f"{date} | O: {open_p:8} H: {high:8} L: {low:8} C: {close:8}")
        
finally:
    db.close()
