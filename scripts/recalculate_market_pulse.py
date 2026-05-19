import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date as dt_date
from app.core.database import SessionLocal
from app.models.market_reports import HistoricalReport
from app.models.market_pulse import MarketPulse
from app.services.market_pulse_calc import (
    OHLCVInput, HistoryRow, compute_signals, build_record, get_calc_settings,
)

# Start date for calculations — pre-2010 data has quality issues
START_DATE = dt_date(2010, 1, 1)

def parse_num(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None

def parse_int(val: str | None) -> int | None:
    if not val:
        return None
    try:
        return int(val.replace(",", "").split(".")[0])
    except (ValueError, TypeError):
        return None

def main():
    db = SessionLocal()
    try:
        print("Deleting all existing market_pulse records...")
        db.query(MarketPulse).delete()
        db.commit()

        print(f"Fetching historical reports from {START_DATE} onwards...")
        reports = (
            db.query(HistoricalReport)
            .filter(HistoricalReport.report_date >= START_DATE)
            .order_by(HistoricalReport.report_date.asc())
            .all()
        )
        print(f"Found {len(reports)} reports to process.")

        calc_settings = get_calc_settings(db)
        print(f"Loaded TASI settings: {calc_settings}")

        inserted = 0
        skipped = 0
        history_buffer = []  # We will keep a running list of history in memory (much faster than querying DB inside the loop)

        for report in reports:
            o = parse_num(report.open_price)
            h = parse_num(report.high_price)
            lo = parse_num(report.low_price)
            c = parse_num(report.close_price)
            vol = parse_num(report.volume_traded)

            if None in (o, h, lo, c, vol):
                skipped += 1
                continue

            today_in = OHLCVInput(
                date=report.report_date,
                open=o,
                high=h,
                low=lo,
                close=c,
                volume_traded=vol,
                value_traded=parse_num(report.value_traded),
                no_of_trades=parse_int(report.no_of_trades),
            )

            # history_buffer has oldest first, but compute_signals expects newest first
            # We take the last 200 items, and reverse them.
            recent_history = list(reversed(history_buffer[-200:]))

            signals = compute_signals(today_in, recent_history, settings=calc_settings)
            record_dict = build_record(today_in, signals)
            
            record = MarketPulse(**record_dict)
            db.add(record)
            
            # Add to history buffer for the next day's calculations
            history_buffer.append(
                HistoryRow(
                    close=record_dict["close"],
                    volume_traded=record_dict["volume_traded"],
                    high=record_dict["high"],
                    low=record_dict["low"],
                    ema_21=record_dict.get("ema_21"),
                    atr=record_dict.get("atr"),
                    rd_count=record_dict.get("rd_count"),
                    ftd=record_dict.get("ftd"),
                    dd_sd=record_dict.get("dd_sd"),
                    current_outlook=record_dict.get("current_outlook"),
                    change_pct=record_dict.get("change_pct"),
                )
            )
            
            # Prune buffer to prevent memory bloat
            if len(history_buffer) > 201:
                history_buffer = history_buffer[-201:]

            inserted += 1
            if inserted % 500 == 0:
                db.commit()
                print(f"  ✅ {inserted} records recalculated and inserted...")

        db.commit()
        print(f"🎉 Recalculation Complete! Total Inserted: {inserted}, Skipped: {skipped}")

    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
