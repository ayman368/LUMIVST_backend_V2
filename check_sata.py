"""
check_sata.py — Compare SATA bands with TradingView
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import argparse
from app.core.database import SessionLocal
from app.services.sata import calculate_sata, BAND_LABELS, STAGE_LABELS

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="2222", help="Stock symbol")
    parser.add_argument("--weeks", type=int, default=5)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        print(f"\n[SATA] Calculating for {args.symbol} vs ^TASI.SR...\n")
        df = calculate_sata(db, symbol=args.symbol, start_date="2018-01-01")

        last_n = df.tail(args.weeks)
        band_cols = [
            "b10_breakout", "b09_above_30w", "b08_above_40w",
            "b07_30w_rising", "b06_40w_rising", "b05_mrs_positive",
            "b04_macd_bull", "b03_rsi_above50", "b02_vol_expand",
            "b01_resistance_clear",
        ]

        print(f"Last {args.weeks} weeks:")
        print("-" * 100)
        for dt, row in last_n.iterrows():
            score = int(row["sata_score"])
            stage = row["stage"]
            bands_str = " ".join(["[+]" if int(row[c]) == 1 else "[-]" for c in band_cols])
            print(f"  {dt.date()} | Close={row['close']:8.2f} | Score={score:2d} | {stage:8s} | {bands_str}")

        print("-" * 100)
        last = df.iloc[-1]
        print(f"\nDetail for {df.index[-1].date()} (Score={int(last['sata_score'])}):")
        for col in band_cols:
            val = int(last[col])
            status = "[+] ON " if val == 1 else "[-] OFF"
            print(f"   [{col[1:3]}] {BAND_LABELS[col]:<25} -> {status}")

        print(f"\n   Mansfield RS = {last['mansfield_rs']:.2f}")
        print(f"   RSI(14w)     = {last['rsi']:.1f}")
        print(f"   30w SMA      = {last['sma_30w']:.2f}")
        print(f"   40w SMA      = {last['sma_40w']:.2f}")
        print(f"   Close        = {last['close']:.2f}")

    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    main()
