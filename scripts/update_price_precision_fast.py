"""
تحديث دقة أعمدة الأسعار - نسخة محسّنة مع USING
"""

import sys
from pathlib import Path
from sqlalchemy import text, create_engine
import os

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.config import settings

def update_price_precision_fast():
    """تحديث دقة أعمدة الأسعار بشكل أسرع"""
    
    # الاتصال المباشر بـ PostgreSQL
    db_url = settings.DATABASE_URL
    engine = create_engine(db_url)
    
    try:
        print("=" * 70)
        print("🔧 Fast Update: Price Precision NUMERIC(12, 2) → NUMERIC(12, 4)")
        print("=" * 70)
        
        columns = ['open', 'high', 'low', 'close']
        
        with engine.connect() as conn:
            for col in columns:
                print(f"\n📝 Updating column 'prices.{col}'...")
                
                # استخدام USING للتحويل الأسرع
                alter_query = text(f"""
                    ALTER TABLE prices
                    ALTER COLUMN {col} TYPE NUMERIC(12, 4) USING {col}::NUMERIC(12, 4);
                """)
                
                try:
                    conn.execute(alter_query)
                    conn.commit()
                    print(f"   ✅ Column '{col}' updated successfully")
                except Exception as e:
                    conn.rollback()
                    print(f"   ⚠️  Error (may already be correct type): {e}")
        
        print("\n" + "=" * 70)
        print("✅ Update Complete!")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    update_price_precision_fast()
