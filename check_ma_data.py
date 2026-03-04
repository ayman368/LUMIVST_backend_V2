import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

from app.core.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# تحقق من عدد الحقول المحسوبة
result = db.execute(text('''
SELECT 
    COUNT(*) as total_records,
    COUNT(ema10) as ema10_count,
    COUNT(ema21) as ema21_count,
    COUNT(sma50) as sma50_count,
    COUNT(sma150) as sma150_count,
    COUNT(sma200) as sma200_count,
    COUNT(CASE WHEN ema10_gt_sma50 = true THEN 1 END) as ema10_gt_sma50_true,
    COUNT(CASE WHEN ema10_gt_sma200 = true THEN 1 END) as ema10_gt_sma200_true
FROM stock_indicators 
WHERE date = '2026-03-03'
'''))

row = result.fetchone()
print('📊 STOCK INDICATORS DATA CHECK')
print('=' * 60)
print(f'Total Records: {row[0]}')
print(f'EMA10 (non-null): {row[1]}')
print(f'EMA21 (non-null): {row[2]}')
print(f'SMA50 (non-null): {row[3]}')
print(f'SMA150 (non-null): {row[4]}')
print(f'SMA200 (non-null): {row[5]}')
print(f'EMA10 > SMA50 (TRUE): {row[6]}')
print(f'EMA10 > SMA200 (TRUE): {row[7]}')

# عرض عينة
result2 = db.execute(text('''
SELECT symbol, ema10, ema21, sma50, sma150, sma200,
       ema10_gt_sma50, ema10_gt_sma200, ema21_gt_sma50, ema21_gt_sma200
FROM stock_indicators 
WHERE date = '2026-03-03'
LIMIT 5
'''))

print('\n📄 SAMPLE DATA:')
print('=' * 60)
for row in result2:
    print(f'{row[0]:8} | EMA10: {row[1]}, EMA21: {row[2]}, SMA50: {row[3]}, SMA150: {row[4]}, SMA200: {row[5]}')
    print(f'         | Conditions: {row[6]}, {row[7]}, {row[8]}, {row[9]}')

db.close()
