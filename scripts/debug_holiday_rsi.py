"""
Debug RSI with holiday removal
"""
import sys, os, numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.database import SessionLocal
from sqlalchemy import text
from scripts.calculate_rsi_indicators import calculate_rsi_pinescript, calculate_sma

SYMBOL = "1321"

db = SessionLocal()
result = db.execute(text("""
    SELECT date, open, high, low, close
    FROM prices
    WHERE symbol = :s
    ORDER BY date ASC
"""), {"s": SYMBOL})
rows = result.fetchall()
db.close()

# 1. Normal RSI calculation
closes = [float(r[4]) for r in rows]
rsi_normal = calculate_rsi_pinescript(closes, 14)
sma9_rsi_normal = calculate_sma(rsi_normal, 9)

# 2. Filtered RSI calculation (remove consecutive duplicates)
filtered_rows = [rows[0]]
for i in range(1, len(rows)):
    prev = rows[i-1]
    curr = rows[i]
    if curr[1] == prev[1] and curr[2] == prev[2] and curr[3] == prev[3] and curr[4] == prev[4]:
        # skip identical day (likely holiday)
        pass
    else:
        filtered_rows.append(curr)

filtered_closes = [float(r[4]) for r in filtered_rows]
rsi_filtered = calculate_rsi_pinescript(filtered_closes, 14)
sma9_rsi_filtered = calculate_sma(rsi_filtered, 9)

print(f"📊 النتائج:")
print(f"1. RSI(14) العادي    : {rsi_normal[-1]:.4f} (الكود الحالي: 36.6119, المستهدف: 37.14)")
print(f"2. SMA9(RSI) العادي  : {sma9_rsi_normal[-1]:.4f} (الكود الحالي: 37.9474, المستهدف: 38.53)")
print()
print(f"3. RSI(14) مفلتر     : {rsi_filtered[-1]:.4f} (المستهدف: 37.14)")
print(f"4. SMA9(RSI) مفلتر   : {sma9_rsi_filtered[-1]:.4f} (المستهدف: 38.53)")

# Checking diff
diff_rsi = abs(37.14 - rsi_filtered[-1])
diff_sma = abs(38.53 - sma9_rsi_filtered[-1])
print(f"\nالفرق بعد الفلترة:")
print(f"RSI diff: {diff_rsi:.4f}")
print(f"SMA diff: {diff_sma:.4f}")
