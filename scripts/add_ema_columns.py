"""
إضافة أعمدة ema10 و ema21 إلى جدول stock_indicators
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from sqlalchemy import create_engine, text

def add_columns():
    engine = create_engine(str(settings.DATABASE_URL))
    
    columns = {
        'ema10': 'NUMERIC(12, 4)',
        'ema21': 'NUMERIC(12, 4)',
    }
    
    for col_name, col_type in columns.items():
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE stock_indicators ADD COLUMN {col_name} {col_type}"))
                print(f"✅ تم إضافة العمود: {col_name}")
        except Exception as e:
            if "duplicate" in str(e).lower() or "already exists" in str(e).lower():
                print(f"✓ العمود {col_name} موجود بالفعل")
            else:
                print(f"⚠️ خطأ في {col_name}: {e}")

if __name__ == "__main__":
    add_columns()
    print("\n🎉 تم الانتهاء!")
