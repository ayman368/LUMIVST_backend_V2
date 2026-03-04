import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

from sqlalchemy import text, inspect
from app.core.database import SessionLocal

db = SessionLocal()

print("=" * 80)
print("🔍 CHECKING EXISTING COLUMNS IN PRICES TABLE")
print("=" * 80)

# Get all columns from prices table
inspector = inspect(db.bind)
cols = inspector.get_columns('prices')

# Filter MA-related columns
ma_cols = sorted([col['name'] for col in cols if 'ema' in col['name'].lower() or 'sma' in col['name'].lower()])

print("\n✅ MA COLUMNS IN PRICES TABLE (مع underscore):")
for col in ma_cols:
    print(f"   - {col}")

print("\n" + "=" * 80)
print("🔍 CHECKING COLUMNS IN STOCK_INDICATORS TABLE")
print("=" * 80)

# Get all columns from stock_indicators table
cols_indicators = inspector.get_columns('stock_indicators')
ma_cols_indicators = sorted([col['name'] for col in cols_indicators if 'ema' in col['name'].lower() or 'sma' in col['name'].lower() or 'comparison' in col['name'].lower() or 'gt_' in col['name'].lower()])

print("\n📊 MA/COMPARISON COLUMNS IN STOCK_INDICATORS TABLE:")
for col in ma_cols_indicators:
    print(f"   - {col}")

print("\n" + "=" * 80)
print("📋 ANALYSIS")
print("=" * 80)

# Check for duplicates
new_cols = [c for c in ma_cols_indicators if 'underscore' not in c and c not in ['sma4', 'sma9', 'sma18', 'wma45_close']]
print(f"\n❌ UNNECESSARY NEW COLUMNS (without underscore): {len([c for c in ma_cols_indicators if c in ['ema10', 'ema21', 'sma50', 'sma150', 'sma200']])}")
print(f"   (Should use ema_10, ema_21, sma_50, sma_150, sma_200 from prices table instead)")

print(f"\n✅ GOOD COLUMNS in stock_indicators: sma4, sma9, sma18, wma45_close")

print(f"\n❓ COMPARISON COLUMNS in stock_indicators: {len([c for c in ma_cols_indicators if 'comparison' in c or 'gt_' in c])}")

db.close()
