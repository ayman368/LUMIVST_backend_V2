"""
Debug CCI v2: نجرّب تقريب TP بعد الحساب لنرى أي يطابق TradingView
"""
import sys, os, numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.database import SessionLocal
from sqlalchemy import text

SYMBOL = "1321"
PERIOD = 14
TV_CCI  = -77.28      # القيمة الصحيحة من TradingView
TV_CCI_W = -58.04    # CCI Weekly الصحيح

db = SessionLocal()
rows = list(reversed(db.execute(text("""
    SELECT date, high, low, close FROM prices
    WHERE symbol = :s ORDER BY date DESC LIMIT 20
"""), {"s": SYMBOL}).fetchall()))
db.close()

def cci_from_window(tp_arr):
    mean_tp  = np.mean(tp_arr)
    mean_dev = np.mean(np.abs(tp_arr - mean_tp))
    if mean_dev == 0: return 0.0
    return (tp_arr[-1] - mean_tp) / (0.015 * mean_dev)

print("🔍 نجرب تقريب Typical Price بعد الحساب:")
print(f"{'دقة TP':<15} {'CCI':<12} {'الفرق عن TV'}")
print("-" * 45)

for decimals in [6, 4, 3, 2]:
    tps = []
    for r in rows:
        h, l, c = float(r[1]), float(r[2]), float(r[3])
        tp = round((h + l + c) / 3, decimals)
        tps.append(tp)
    tp_window = np.array(tps[-PERIOD:])
    cci = cci_from_window(tp_window)
    diff = abs(TV_CCI - cci)
    marker = " ✅" if diff < 0.01 else ""
    print(f"round(TP, {decimals}):    {cci:>8.4f}    {diff:.4f}{marker}")

print()
print("🔍 بدون تقريب للـ TP:")
tps_raw = [(float(r[1]) + float(r[2]) + float(r[3])) / 3 for r in rows]
cci_raw = cci_from_window(np.array(tps_raw[-PERIOD:]))
print(f"TP بدون تقريب:    {cci_raw:>8.4f}    {abs(TV_CCI-cci_raw):.4f}")

print()
print("=" * 45)
print("📋 قيم TP المحسوبة لآخر 14 شمعة:")
for r in rows[-PERIOD:]:
    h, l, c = float(r[1]), float(r[2]), float(r[3])
    tp = (h + l + c) / 3
    tp2 = round(tp, 2)
    print(f"  {r[0]}  H:{h:.1f} L:{l:.1f} C:{c:.1f}  TP={tp:.4f}  round2={tp2}")

# جرّب أيضاً المتوسط المقرّب
print()
print("🔍 نجرب تقريب Mean TP:")
tps_raw = [(float(r[1]) + float(r[2]) + float(r[3])) / 3 for r in rows]
tp_window = np.array(tps_raw[-PERIOD:])
for rnd_mean in [2, 4, 6]:
    mean_tp = round(np.mean(tp_window), rnd_mean)
    mean_dev = np.mean(np.abs(tp_window - mean_tp))
    cci = (tp_window[-1] - mean_tp) / (0.015 * mean_dev)
    print(f"round(mean, {rnd_mean}):    {cci:>8.4f}    {abs(TV_CCI-cci):.4f}")
