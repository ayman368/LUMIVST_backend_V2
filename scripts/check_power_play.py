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
        print(f"Latest date: {latest}")

        q = db.query(StockIndicator).filter(StockIndicator.date == latest)
        total_rows = q.count()
        print(f"Total rows on latest date: {total_rows}")
        
        null_change_126d = q.filter(StockIndicator.percent_change_126d == None).count()
        print(f"Rows with null percent_change_126d: {null_change_126d}")
        
        power_play_q = q.filter(
            StockIndicator.price_vs_sma_50_percent != None,
            StockIndicator.price_vs_sma_200_percent != None,
            StockIndicator.percent_change_20d != None,
            StockIndicator.percent_change_15d != None,
            StockIndicator.percent_change_126d != None,
            StockIndicator.price_vs_sma_50_percent > 0.0,
            StockIndicator.price_vs_sma_200_percent > 0.0,
            StockIndicator.percent_change_20d > -25.0,
            StockIndicator.percent_change_15d >= -15.0,
            StockIndicator.percent_change_15d <= 5.0,
            StockIndicator.percent_change_126d > 85.0
        )
        print(f"Power play matching rows: {power_play_q.count()}")
        
        print("\n--- Breakdown of Conditions ---")
        print(f"> 50d SMA (>0%): {q.filter(StockIndicator.price_vs_sma_50_percent > 0.0).count()}")
        print(f"> 200d SMA (>0%): {q.filter(StockIndicator.price_vs_sma_200_percent > 0.0).count()}")
        print(f"Change 20d (> -25%): {q.filter(StockIndicator.percent_change_20d > -25.0).count()}")
        print(f"Change 15d (>= -15%): {q.filter(StockIndicator.percent_change_15d >= -15.0).count()}")
        print(f"Change 15d (<= 5%): {q.filter(StockIndicator.percent_change_15d <= 5.0).count()}")
        print(f"Change 126d (> 85%): {q.filter(StockIndicator.percent_change_126d > 85.0).count()}")
        
        # Test combined
        test_q = q.filter(StockIndicator.percent_change_126d > 85.0)
        test_q2 = test_q.filter(StockIndicator.percent_change_15d <= 5.0)
        print(f"\nCondition: > 85% (126d) AND <= 5% (15d): {test_q2.count()}")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()
