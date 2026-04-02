import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from app.core.database import SessionLocal

def main():
    db = SessionLocal()
    try:
        query = """
            SELECT symbol, date, beta 
            FROM stock_indicators 
            WHERE beta IS NOT NULL 
            ORDER BY date DESC 
            LIMIT 10;
        """
        result = db.execute(text(query)).fetchall()
        
        print("====== VEFIFYING BETA IN DB ======")
        if len(result) == 0:
            print("❌ NO DATA FOUND! 'beta' is completely NULL across the entire table.")
        else:
            print("✅ Beta values are stored successfully! Here is a sample:")
            for row in result:
                print(f"  - Symbol: {row[0]}, Date: {row[1]}, Beta: {row[2]}")
                
        # Check total rows with beta
        total_beta = db.execute(text("SELECT COUNT(*) FROM stock_indicators WHERE beta IS NOT NULL")).scalar()
        print(f"\nTotal rows with Beta updated: {total_beta}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
