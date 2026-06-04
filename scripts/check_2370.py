import sys
import os

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal
from app.models.stock_indicators import StockIndicator

def main():
    db = SessionLocal()
    try:
        latest_date_tuple = db.query(StockIndicator.date).order_by(StockIndicator.date.desc()).first()
        if not latest_date_tuple:
            print("No data found in stock_indicators.")
            return
            
        latest = latest_date_tuple[0]
        
        stock = db.query(StockIndicator).filter(StockIndicator.date == latest, StockIndicator.symbol == '2370').first()
        if not stock:
            print("Stock 2370 not found for date", latest)
        else:
            print(f"Symbol: {stock.symbol} ({stock.company_name})")
            print(f"Price Vs 50d SMA: {stock.price_vs_sma_50_percent}% (Needs > 0)")
            print(f"Price Vs 200d SMA: {stock.price_vs_sma_200_percent}% (Needs > 0)")
            print(f"Change 20d: {stock.percent_change_20d}% (Needs > -25%)")
            print(f"Change 15d: {stock.percent_change_15d}% (Needs >= -15% AND <= 5%)")
            print(f"Change 126d: {stock.percent_change_126d}% (Needs > 85%)")
            
    finally:
        db.close()

if __name__ == "__main__":
    main()
