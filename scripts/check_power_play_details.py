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
        q = db.query(StockIndicator).filter(StockIndicator.date == latest)
        
        # Test combined
        test_q = q.filter(StockIndicator.percent_change_126d > 85.0).all()
        for stock in test_q:
            print(f"Symbol: {stock.symbol}, 126d: {stock.percent_change_126d}%, 15d: {stock.percent_change_15d}%, 20d: {stock.percent_change_20d}%")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()
