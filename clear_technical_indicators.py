#!/usr/bin/env python
"""
مسح جميع المؤشرات التقنية من جدول technical_indicators
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    print("="*70)
    print("🗑️  CLEARING ALL TECHNICAL INDICATORS")
    print("="*70)
    print()
    
    # احصل على عدد السجلات قبل الحذف
    result = db.execute(text("SELECT COUNT(*) FROM technical_indicators"))
    count_before = result.fetchone()[0]
    print(f"Records before deletion: {count_before}")
    print()
    
    # احذف كل البيانات
    print("🧹 Deleting all records from technical_indicators...")
    result = db.execute(text("DELETE FROM technical_indicators"))
    db.commit()
    
    print(f"✅ Deleted {result.rowcount} records")
    print()
    
    # تحقق من النتيجة
    result = db.execute(text("SELECT COUNT(*) FROM technical_indicators"))
    count_after = result.fetchone()[0]
    print(f"Records after deletion: {count_after}")
    
    print()
    print("="*70)
    print(f"✅ DONE! All {result.rowcount} technical indicators cleared")
    print("="*70)
    
except Exception as e:
    print(f"❌ Error: {e}")
    db.rollback()
    import traceback
    traceback.print_exc()
finally:
    db.close()
