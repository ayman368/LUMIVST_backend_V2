import sys
import pandas as pd
from sqlalchemy import create_engine
from app.core.config import settings
from app.services.mansfield_rs import calculate_mansfield_rs
from sqlalchemy.orm import sessionmaker

def main():
    engine = create_engine(settings.DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    symbol = "1321"
    benchmark = "^TASI.SR"
    
    try:
        print(f"📊 Calculating Mansfield RS for {symbol} vs {benchmark}...")
        df = calculate_mansfield_rs(db, symbol=symbol, benchmark=benchmark, ma_length=52)
        
        print("\n🔍 Last 5 Weeks Data:")
        print("Date       | Stock  | Bench   | Base (S/B*100) | SMA(52) | Mansfield RS")
        print("-" * 75)
        
        last_5 = df.tail(5)
        for dt, row in last_5.iterrows():
            date_str = dt.strftime("%Y-%m-%d")
            stock = row['stock']
            bench = row['bench']
            base = row['stock_div_bench']
            sma = row['zero_line']
            mrs = row['mansfield_rs']
            
            print(f"{date_str} | {stock:<6.2f} | {bench:<7.2f} | {base:<14.4f} | {sma:<7.4f} | {mrs:+.4f}")
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
