#!/usr/bin/env python
"""
مسح فقط مؤشرات technical-screener (RSI, CCI, STAMP, CFG, The Number)
الحفاظ على: SMA, EMA, المتوسطات المتحركة
"""
import sys
sys.path.insert(0, '.')

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

try:
    print("="*80)
    print("🗑️  CLEARING TECHNICAL-SCREENER INDICATORS ONLY")
    print("="*80)
    print()
    
    # ====== حذف من جدول stock_indicators ======
    print("1️⃣  Clearing technical-screener indicators from stock_indicators...")
    result = db.execute(text("SELECT COUNT(*) FROM stock_indicators"))
    count_before = result.fetchone()[0]
    print(f"   Before: {count_before} records")
    
    # الأعمدة التي سيتم مسحها (technical-screener فقط)
    tech_screener_cols = [
        # ===== RSI Components =====
        'rsi_14', 'rsi_3',
        'sma9_rsi', 'wma45_rsi', 'ema45_rsi',
        'sma3_rsi3', 'ema20_sma3',
        'rsi_w', 'rsi_3_w', 'sma3_rsi3_w', 'sma9_rsi_w', 'wma45_rsi_w', 'ema45_rsi_w', 'ema20_sma3_w',
        
        # ===== The Number Components =====
        'sma9_close', 'high_sma13', 'low_sma13', 'high_sma65', 'low_sma65',
        'the_number', 'the_number_hl', 'the_number_ll',
        'sma9_close_w', 'the_number_w', 'the_number_hl_w', 'the_number_ll_w',
        'high_sma13_w', 'low_sma13_w', 'high_sma65_w', 'low_sma65_w',
        
        # ===== STAMP Components =====
        'rsi_14_9days_ago', 'stamp_a_value',
        'stamp_s9rsi', 'stamp_e45cfg', 'stamp_e45rsi', 'stamp_e20sma3',
        'rsi_14_9days_ago_w', 'stamp_a_value_w',
        'stamp_s9rsi_w', 'stamp_e45cfg_w', 'stamp_e45rsi_w', 'stamp_e20sma3_w',
        
        # ===== CFG Analysis =====
        'cfg_daily', 'cfg_sma4', 'cfg_sma9', 'cfg_sma20', 'cfg_ema20', 'cfg_ema45', 'cfg_wma45',
        'cfg_w', 'cfg_sma4_w', 'cfg_sma9_w', 'cfg_ema20_w', 'cfg_ema45_w', 'cfg_wma45_w',
        'rsi_14_9days_ago_cfg', 'rsi_14_minus_9', 'rsi_14_minus_9_w', 'rsi_14_w_shifted',
        
        # ===== CCI Components =====
        'cci', 'cci_ema20', 'cci_w', 'cci_ema20_w',
        
        # ===== Aroon Components =====
        'aroon_up', 'aroon_down', 'aroon_up_w', 'aroon_down_w',
        
        # ===== Boolean Conditions =====
        'cfg_gt_50_daily', 'cfg_ema45_gt_50', 'cfg_ema20_gt_50', 'cfg_gt_50_w', 
        'cfg_ema45_gt_50_w', 'cfg_ema20_gt_50_w',
        'price_gt_sma18', 'price_gt_sma9_weekly', 'sma_trend_daily', 'sma_trend_weekly',
        'cci_gt_100', 'cci_ema20_gt_0_daily', 'cci_ema20_gt_0_weekly',
        'aroon_up_gt_70', 'aroon_down_lt_30',
        'is_etf_or_index', 'has_gap', 'trend_signal',
        'ema10_gt_sma50', 'ema10_gt_sma200', 'ema21_gt_sma50', 'ema21_gt_sma200',
        'sma50_gt_sma150', 'sma50_gt_sma200', 'sma150_gt_sma200',
        'sma200_gt_sma200_1m_ago', 'sma200_gt_sma200_2m_ago', 'sma200_gt_sma200_3m_ago',
        'sma200_gt_sma200_4m_ago', 'sma200_gt_sma200_5m_ago',
        'sma9_gt_tn_daily', 'sma9_gt_tn_weekly',
        'rsi_lt_80_d', 'rsi_lt_80_w', 'sma9_rsi_lte_75_d', 'sma9_rsi_lte_75_w',
        'ema45_rsi_lte_70_d', 'ema45_rsi_lte_70_w',
        'rsi_55_70', 'rsi_gt_wma45_d', 'rsi_gt_wma45_w',
        'sma9rsi_gt_wma45rsi_d', 'sma9rsi_gt_wma45rsi_w',
        'stamp_daily', 'stamp_weekly', 'stamp', 'final_signal'
    ]
    
    # Build SET clause
    set_clause = ', '.join([f'{col} = NULL' for col in tech_screener_cols])
    update_query = f"UPDATE stock_indicators SET {set_clause}"
    
    result = db.execute(text(update_query))
    db.commit()
    
    print(f"   ✅ Reset {len(tech_screener_cols)} technical-screener columns to NULL")
    print(f"   ✅ Updated {result.rowcount} records")
    print()
    
    # ====== حذف كل البيانات من جدول technical_indicators ======
    print("2️⃣  Clearing technical_indicators table...")
    result = db.execute(text("SELECT COUNT(*) FROM technical_indicators"))
    count_ti = result.fetchone()[0]
    print(f"   Before: {count_ti} records")
    
    db.execute(text("DELETE FROM technical_indicators"))
    db.commit()
    
    result = db.execute(text("SELECT COUNT(*) FROM technical_indicators"))
    count_after = result.fetchone()[0]
    print(f"   After: {count_after} records")
    print(f"   ✅ Deleted {count_ti} records")
    print()
    
    print("="*80)
    print("✅ DONE! Technical-screener indicators cleared")
    print("="*80)
    print()
    print("✅ تم مسح:")
    print("  ✅ RSI components (all variants)")
    print("  ✅ The Number components")
    print("  ✅ STAMP indicator")
    print("  ✅ CFG analysis")
    print("  ✅ CCI components")
    print("  ✅ Aroon indicator")
    print("  ✅ Screener conditions")
    print()
    print("🛡️  محفوظ (لم يتم مسحه):")
    print("  ✓ SMA (10, 21, 50, 150, 200)")
    print("  ✓ EMA (10, 21)")
    print("  ✓ المتوسطات المتحركة الأخرى (SMA4, 9, 18, WMA45)")
    print()
    
except Exception as e:
    print(f"❌ Error: {e}")
    db.rollback()
    import traceback
    traceback.print_exc()
finally:
    db.close()
