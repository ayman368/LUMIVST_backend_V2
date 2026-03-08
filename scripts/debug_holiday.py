"""
Debug TV mismatch by checking for holidays / duplicates
"""
import sys, os, numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.database import SessionLocal
from sqlalchemy import text

SYMBOL = "1321"

db = SessionLocal()
result = db.execute(text("""
    SELECT date, open, high, low, close
    FROM prices
    WHERE symbol = :s
    ORDER BY date DESC
    LIMIT 30
"""), {"s": SYMBOL})
rows = list(reversed(result.fetchall()))
db.close()

tps = []
dates = []
for r in rows:
    d, o, h, l, c = r
    d, o, h, l, c = str(d), float(o), float(h), float(l), float(c)
    tp = (h + l + c) / 3
    tps.append(tp)
    dates.append(d)

def calc_cci(tp_arr):
    tp_window = np.array(tp_arr)
    mean_tp = np.mean(tp_window)
    mean_dev = np.mean(np.abs(tp_window - mean_tp))
    if mean_dev == 0: return 0.0
    return (tp_window[-1] - mean_tp) / (0.015 * mean_dev)

print("🔍 1. حساب CCI العادي بـ 14 شمعة الأخيرة:")
normal_tps = tps[-14:]
print(f"CCI_14 = {calc_cci(normal_tps):.4f}")

print("\n🔍 2. حساب CCI بعد إزالة 2026-02-22 (يوم التأسيس / عطلة محتملة):")
filtered_tps = []
filtered_dates = []
for i in range(len(tps)):
    if dates[i] != "2026-02-22":
        filtered_tps.append(tps[i])
        filtered_dates.append(dates[i])

filtered_14_tps = filtered_tps[-14:]
filtered_14_dates = filtered_dates[-14:]
print(f"Dates used: {filtered_14_dates}")
cci_no_holiday = calc_cci(filtered_14_tps)
print(f"CCI_14 (بدون عطلة) = {cci_no_holiday:.4f} (TradingView المستهدف هو -77.28)")

print("\n🔍 3. تصفية جميع الأيام المكررة بالكامل (نفس OHLC كاليوم السابق) وحساب CCI:")
unique_tps = [tps[0]]
unique_dates = [dates[0]]

for i in range(1, len(rows)):
    prev_r = rows[i-1]
    curr_r = rows[i]
    # Check if OHLC matches prev day
    if curr_r[1] == prev_r[1] and curr_r[2] == prev_r[2] and curr_r[3] == prev_r[3] and curr_r[4] == prev_r[4]:
        print(f"⚠️ تجاهل الشمعة المطابقة {curr_r[0]} (مكررة عن يوم {prev_r[0]})")
        continue
    unique_tps.append(tps[i])
    unique_dates.append(dates[i])

unique_14_tps = unique_tps[-14:]
unique_14_dates = unique_dates[-14:]
print(f"\nDates used: {unique_14_dates}")
cci_unique = calc_cci(unique_14_tps)
print(f"CCI_14 (بدون تكرار) = {cci_unique:.4f} (TradingView المستهدف هو -77.28)")

