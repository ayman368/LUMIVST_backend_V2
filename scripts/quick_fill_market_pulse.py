"""
quick_fill_market_pulse.py
===========================
Fast script to fill missing dates in market_pulse from historical_reports.
Only inserts OHLCV data — no heavy signal computation.
This is enough for Mansfield RS and other indicators that only need Close prices.

Run from backend/:
    ..\venv\Scripts\python.exe scripts\quick_fill_market_pulse.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import text
from app.core.database import SessionLocal


def parse_num(val):
    """Parse string like '11,031.32' → float."""
    if not val:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def parse_int(val):
    if not val:
        return None
    try:
        return int(str(val).replace(",", "").split(".")[0])
    except (ValueError, TypeError):
        return None


def main():
    db = SessionLocal()
    try:
        # 1. Find missing dates (in historical_reports but NOT in market_pulse)
        missing = db.execute(text("""
            SELECT hr.report_date, hr.open_price, hr.high_price, hr.low_price, 
                   hr.close_price, hr.volume_traded, hr.value_traded, hr.no_of_trades
            FROM historical_reports hr
            WHERE hr.report_date NOT IN (SELECT date FROM market_pulse)
            ORDER BY hr.report_date ASC
        """)).fetchall()

        print(f"📊 Found {len(missing)} missing dates to fill")

        if not missing:
            print("✅ market_pulse is already up to date!")
            return

        inserted = 0
        for row in missing:
            report_date, open_p, high_p, low_p, close_p, vol, val_traded, trades = row

            o = parse_num(open_p)
            h = parse_num(high_p)
            lo = parse_num(low_p)
            c = parse_num(close_p)
            v = parse_num(vol)

            if None in (o, h, lo, c, v):
                print(f"  ⏭️  {report_date} — missing OHLCV, skipping")
                continue

            vt = parse_num(val_traded)
            nt = parse_int(trades)

            db.execute(text("""
                INSERT INTO market_pulse (date, open, high, low, close, volume_traded, value_traded, no_of_trades)
                VALUES (:dt, :o, :h, :lo, :c, :v, :vt, :nt)
                ON CONFLICT (date) DO NOTHING
            """), {
                "dt": report_date, "o": o, "h": h, "lo": lo,
                "c": c, "v": v, "vt": vt, "nt": nt
            })
            inserted += 1

        db.commit()
        print(f"✅ Done! Inserted {inserted} rows in market_pulse (OHLCV only, no signals)")

        # Show latest 5 dates
        latest = db.execute(text("""
            SELECT date, close FROM market_pulse ORDER BY date DESC LIMIT 5
        """)).fetchall()
        print("\n📅 Latest 5 dates in market_pulse:")
        for r in latest:
            print(f"   {r[0]} → Close: {r[1]}")

    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
