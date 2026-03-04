"""
إضافة أعمدة MA Comparison Filters إلى جدول stock_indicators
"""
import sys
from pathlib import Path
from sqlalchemy import Column, Numeric, Boolean, text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal, engine
from app.models.stock_indicators import StockIndicator

def add_ma_filter_columns():
    """إضافة الأعمدة الجديدة للفلاتر"""
    
    db = SessionLocal()
    
    try:
        print("=" * 70)
        print("🔧 ADDING MA COMPARISON FILTER COLUMNS")
        print("=" * 70)
        
        # قائمة الأعمدة المطلوب إضافتها
        columns_to_add = [
            # Price Moving Averages
            ('ema10', 'NUMERIC(10, 2)', 'NULL'),
            ('ema21', 'NUMERIC(10, 2)', 'NULL'),
            ('sma50', 'NUMERIC(10, 2)', 'NULL'),
            ('sma150', 'NUMERIC(10, 2)', 'NULL'),
            ('sma200', 'NUMERIC(10, 2)', 'NULL'),
            
            # MA Comparison Conditions
            ('ema10_gt_sma50', 'BOOLEAN', 'FALSE'),
            ('ema10_gt_sma200', 'BOOLEAN', 'FALSE'),
            ('ema21_gt_sma50', 'BOOLEAN', 'FALSE'),
            ('ema21_gt_sma200', 'BOOLEAN', 'FALSE'),
            ('sma50_gt_sma150', 'BOOLEAN', 'FALSE'),
            ('sma50_gt_sma200', 'BOOLEAN', 'FALSE'),
            ('sma150_gt_sma200', 'BOOLEAN', 'FALSE'),
            
            # 200SMA Trend Conditions
            ('sma200_gt_sma200_1m_ago', 'BOOLEAN', 'FALSE'),
            ('sma200_gt_sma200_2m_ago', 'BOOLEAN', 'FALSE'),
            ('sma200_gt_sma200_3m_ago', 'BOOLEAN', 'FALSE'),
            ('sma200_gt_sma200_4m_ago', 'BOOLEAN', 'FALSE'),
            ('sma200_gt_sma200_5m_ago', 'BOOLEAN', 'FALSE'),
        ]
        
        added_count = 0
        skipped_count = 0
        
        for col_name, col_type, default_val in columns_to_add:
            try:
                # التحقق من وجود العمود
                check_query = text(f"""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'stock_indicators' 
                    AND column_name = '{col_name}'
                """)
                result = db.execute(check_query).scalar()
                
                if result is None:
                    # العمود غير موجود، أضفه
                    alter_query = text(f"""
                        ALTER TABLE stock_indicators 
                        ADD COLUMN {col_name} {col_type} DEFAULT {default_val}
                    """)
                    db.execute(alter_query)
                    db.commit()
                    print(f"✅ Added column: {col_name} ({col_type})")
                    added_count += 1
                else:
                    print(f"⏭️  Column already exists: {col_name}")
                    skipped_count += 1
                    
            except Exception as e:
                print(f"⚠️  Error adding {col_name}: {e}")
                db.rollback()
        
        print("\n" + "=" * 70)
        print(f"✅ SUMMARY")
        print("=" * 70)
        print(f"   ✅ Added: {added_count} columns")
        print(f"   ⏭️  Skipped: {skipped_count} columns (already exist)")
        print(f"   📊 Total: {len(columns_to_add)} columns")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return False
        
    finally:
        db.close()

if __name__ == "__main__":
    success = add_ma_filter_columns()
    sys.exit(0 if success else 1)
