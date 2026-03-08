#!/usr/bin/env python
"""
تحديث دقة جميع أعمدة stock_indicators من NUMERIC(5,2) و NUMERIC(10,2) إلى NUMERIC(6,4) و NUMERIC(12,4)
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    print("="*80)
    print("🔧 تحديث دقة جميع أعمدة stock_indicators")
    print("="*80)
    print()
    
    # أعمدة RSI/STAMP/CFG: Numeric(5,2) → Numeric(6,4)
    rsi_type_cols = [
        'rsi_14', 'rsi_3',
        'sma9_rsi', 'wma45_rsi', 'ema45_rsi',
        'sma3_rsi3', 'ema20_sma3',
        'rsi_w', 'rsi_3_w', 'sma3_rsi3_w', 'sma9_rsi_w', 'wma45_rsi_w', 'ema45_rsi_w', 'ema20_sma3_w',
        'rsi_14_9days_ago', 'stamp_a_value',
        'stamp_s9rsi', 'stamp_e45cfg', 'stamp_e45rsi', 'stamp_e20sma3',
        'rsi_14_9days_ago_w', 'stamp_a_value_w',
        'stamp_s9rsi_w', 'stamp_e45cfg_w', 'stamp_e45rsi_w', 'stamp_e20sma3_w',
        'cfg_daily', 'cfg_sma4', 'cfg_sma9', 'cfg_sma20', 'cfg_ema20', 'cfg_ema45', 'cfg_wma45',
        'cfg_w', 'cfg_sma4_w', 'cfg_sma9_w', 'cfg_ema20_w', 'cfg_ema45_w', 'cfg_wma45_w',
        'rsi_14_9days_ago_cfg', 'rsi_14_minus_9', 'rsi_14_minus_9_w', 'rsi_14_w_shifted',
        'aroon_up', 'aroon_down', 'aroon_up_w', 'aroon_down_w'
    ]
    
    # أعمدة The Number/MA/CCI: Numeric(10,2) → Numeric(12,4)
    price_type_cols = [
        'close', 'sma9_close', 'high_sma13', 'low_sma13', 'high_sma65', 'low_sma65',
        'the_number', 'the_number_hl', 'the_number_ll',
        'sma9_close_w', 'the_number_w', 'the_number_hl_w', 'the_number_ll_w',
        'high_sma13_w', 'low_sma13_w', 'high_sma65_w', 'low_sma65_w',
        'sma4', 'sma9', 'sma18', 'wma45_close',
        'close_w', 'sma4_w', 'sma9_w', 'sma18_w', 'wma45_close_w',
        'cci', 'cci_ema20', 'cci_w', 'cci_ema20_w'
    ]
    
    # تحديث RSI/STAMP/CFG columns
    print("📝 تحديث أعمدة RSI/STAMP/CFG من Numeric(5,2) إلى Numeric(6,4)...")
    for col in rsi_type_cols:
        try:
            query = f"""
                ALTER TABLE stock_indicators
                ALTER COLUMN {col} TYPE NUMERIC(6, 4) USING {col}::NUMERIC(6, 4);
            """
            db.execute(text(query))
            db.commit()
            print(f"   ✅ {col}")
        except Exception as e:
            print(f"   ❌ {col}: {str(e)[:60]}")
            db.rollback()
    
    print()
    
    # تحديث Price/MA/CCI columns
    print("📝 تحديث أعمدة Price/MA/CCI من Numeric(10,2) إلى Numeric(12,4)...")
    for col in price_type_cols:
        try:
            query = f"""
                ALTER TABLE stock_indicators
                ALTER COLUMN {col} TYPE NUMERIC(12, 4) USING {col}::NUMERIC(12, 4);
            """
            db.execute(text(query))
            db.commit()
            print(f"   ✅ {col}")
        except Exception as e:
            print(f"   ❌ {col}: {str(e)[:60]}")
            db.rollback()
    
    print()
    print("="*80)
    print("✅ DONE! جميع الأعمدة تم تحديثها بنجاح")
    print("="*80)
    
finally:
    db.close()
