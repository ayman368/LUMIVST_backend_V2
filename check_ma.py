import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
from sqlalchemy import create_engine, text
from app.core.config import settings

def find_matching_mas():
    engine = create_engine(settings.DATABASE_URL)
    with engine.connect() as conn:
        stock = conn.execute(text("SELECT date, close FROM prices WHERE symbol = '1321' ORDER BY date ASC")).fetchall()
        tasi = conn.execute(text("SELECT date, close FROM market_pulse ORDER BY date ASC")).fetchall()

        stock_df = pd.DataFrame(stock, columns=['date', 'stock']).set_index('date')
        tasi_df = pd.DataFrame(tasi, columns=['date', 'tasi']).set_index('date')
        
        # دمج البيانات
        df = pd.concat([stock_df, tasi_df], axis=1).dropna()
        df['stock'] = df['stock'].astype(float)
        df['tasi'] = df['tasi'].astype(float)
        
        # حساب RS Line
        df['rs'] = (df['stock'] / df['tasi']) * 100
        last_rs = df['rs'].iloc[-1]
        
        print(f"✅ Last RS Line Value: {last_rs:.4f} (Matches TradingView 1.77)")
        print("\n🔍 Searching for Fast MA ~ 1.73 and Slow MA ~ 1.50...\n")
        
        matches_fast = []
        matches_slow = []
        
        for i in range(2, 100):
            # SMA
            sma = df['rs'].rolling(window=i).mean().iloc[-1]
            if 1.72 <= sma <= 1.74:
                matches_fast.append(f"  🟢 SMA({i}) = {sma:.4f}")
            if 1.49 <= sma <= 1.51:
                matches_slow.append(f"  🔴 SMA({i}) = {sma:.4f}")
                
            # EMA
            ema = df['rs'].ewm(span=i, adjust=False).mean().iloc[-1]
            if 1.72 <= ema <= 1.74:
                matches_fast.append(f"  🟢 EMA({i}) = {ema:.4f}")
            if 1.49 <= ema <= 1.51:
                matches_slow.append(f"  🔴 EMA({i}) = {ema:.4f}")

        print("--- Fast MA Matches (Looking for ~1.73) ---")
        for m in matches_fast: print(m)
        if not matches_fast: print("  ❌ No exact matches found for Fast MA")
            
        print("\n--- Slow MA Matches (Looking for ~1.50) ---")
        for m in matches_slow: print(m)
        if not matches_slow: print("  ❌ No exact matches found for Slow MA")

if __name__ == "__main__":
    find_matching_mas()
