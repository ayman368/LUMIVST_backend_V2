import sys
import os
from datetime import date
from sqlalchemy import func

# Add backend directory to sys.path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from app.core.database import SessionLocal
from app.models.price import Price
from app.models.stock_indicators import StockIndicator

def check_history():
    db = SessionLocal()
    try:
        print("="*50)
        print("🔍 DATABASE HISTORY CHECK")
        print("="*50)

        # 1. Total Stocks in DB
        total_symbols_prices = db.query(func.count(func.distinct(Price.symbol))).scalar()
        total_symbols_indicators = db.query(func.count(func.distinct(StockIndicator.symbol))).scalar()
        print(f"🏢 Total unique companies in `prices` table: {total_symbols_prices}")
        print(f"🏢 Total unique companies in `stock_indicators` table: {total_symbols_indicators}")
        print("-" * 50)

        # 2. History of Prices (OHLCV)
        print("📊 PRICES HISTORY (open, high, low, close):")
        total_price_rows = db.query(func.count(Price.id)).scalar()
        first_price_date = db.query(func.min(Price.date)).scalar()
        last_price_date = db.query(func.max(Price.date)).scalar()
        
        print(f"   • Total rows (trading days logged): {total_price_rows:,}")
        if first_price_date and last_price_date:
             print(f"   • Earliest Date: {first_price_date}")
             print(f"   • Latest Date:   {last_price_date}")
        else:
             print("   • No price data found!")
        print("-" * 50)

        # 3. History of Calculated Indicators (SMA, etc)
        print("📈 INDICATORS HISTORY (sma_50, sma_200, etc):")
        total_ind_rows = db.query(func.count(StockIndicator.id)).scalar()
        
        # We specifically check rows where sma_50 is NOT NULL
        valid_sma_count = db.query(func.count(StockIndicator.id)).filter(StockIndicator.sma_50.isnot(None)).scalar()
        
        first_ind_date = db.query(func.min(StockIndicator.date)).filter(StockIndicator.sma_50.isnot(None)).scalar()
        last_ind_date = db.query(func.max(StockIndicator.date)).filter(StockIndicator.sma_50.isnot(None)).scalar()

        print(f"   • Total indicator rows: {total_ind_rows:,}")
        print(f"   • Rows with actual calculated SMA 50: {valid_sma_count:,}")
        
        if first_ind_date and last_ind_date:
            print(f"   • Earliest Date with SMA 50 calculated: {first_ind_date}")
            print(f"   • Latest Date with SMA 50 calculated:   {last_ind_date}")
        else:
            print("   • No calculated SMA 50 data found!")
            
        print("="*50)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_history()
