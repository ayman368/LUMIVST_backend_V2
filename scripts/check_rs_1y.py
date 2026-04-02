import sys
import os
from sqlalchemy import create_engine, text

# Add parent dir to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings

def check_screener_data(symbol):
    print(f"🔍 Checking Screener Data Database for Symbol: {symbol}")
    print("=" * 60)
    
    engine = create_engine(settings.DATABASE_URL)
    with engine.connect() as conn:
        # 1. Fetch Latest Data from stock_indicators (For SMAs and OFF 52W parameters)
        print("📊 1. Technical Indicators (stock_indicators table):")
        tech_query = text("""
            SELECT date, close, sma_50, sma_150, sma_200, percent_off_52w_high, percent_off_52w_low
            FROM stock_indicators
            WHERE symbol = :sym
            ORDER BY date DESC
            LIMIT 1
        """)
        tech_result = conn.execute(tech_query, {"sym": symbol}).fetchone()

        if tech_result:
            print(f"  - Date: {tech_result[0]}")
            print(f"  - Price (Close): {tech_result[1]}")
            print(f"  - SMA 50: {tech_result[2]}")
            print(f"  - SMA 150: {tech_result[3]}")
            print(f"  - SMA 200: {tech_result[4]}")
            print(f"  - OFF 52W HIGH: {tech_result[5]}%")
            print(f"  - OFF 52W LOW: {tech_result[6]}%")
        else:
            print("  ❌ No data found for this symbol in stock_indicators.")


        print("\n📈 2. RS Rating (rs_daily_v2 table):")
        # 2. Fetch Latest RS from rs_daily_v2
        rs_dates = conn.execute(text("""
            SELECT DISTINCT date FROM rs_daily_v2 ORDER BY date DESC LIMIT 300
        """)).fetchall()
        
        dates_v2 = [str(r[0]) for r in rs_dates]
        
        if not dates_v2:
            print("  ❌ No data found in rs_daily_v2 table.")
            return
            
        latest_v2 = dates_v2[0]
        year_ago_v2 = dates_v2[252] if len(dates_v2) > 252 else dates_v2[-1]
        
        rs_latest = conn.execute(text("SELECT rs_rating FROM rs_daily_v2 WHERE symbol = :sym AND date = :dt"), 
                                   {"sym": symbol, "dt": latest_v2}).scalar()
        rs_1y = conn.execute(text("SELECT rs_rating FROM rs_daily_v2 WHERE symbol = :sym AND date = :dt"), 
                               {"sym": symbol, "dt": year_ago_v2}).scalar()

        print(f"  - Current RS 12M (Date: {latest_v2}): {rs_latest}")
        print(f"  - 1 Year Ago RS  (Date: {year_ago_v2}): {rs_1y}")
        
        print("\n" + "=" * 60)

if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "3010"
    check_screener_data(sym)
