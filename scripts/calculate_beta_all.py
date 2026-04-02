import sys
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import text

import requests
from datetime import datetime, timedelta

# Add backend directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal, engine

def fetch_tasi_yahoo(years=1):
    """Fetch exact TASI (^TASI.SR) closing prices from Yahoo Finance API"""
    try:
        print(f"🌐 Fetching exact ^TASI.SR data from Yahoo Finance for the last {years} years...")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/^TASI.SR?interval=1d&range={years}y"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        result = data.get('chart', {}).get('result')
        if not result:
            print(f"❌ Yahoo API returned no data for ^TASI.SR.")
            return pd.DataFrame()
            
        timestamps = result[0].get('timestamp', [])
        quote = result[0].get('indicators', {}).get('quote', [{}])[0]
        closes = quote.get('close', [])
        
        # Filter out null closes if any and assign accurate Riyadh Timezone
        dates = []
        clean_closes = []
        for ts, c in zip(timestamps, closes):
            if c is not None:
                # convert exactly to Saudi timezone to guarantee the date perfectly matches the user's view
                dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                dates.append(dt.date())
                clean_closes.append(c)
                
        # ⚡ CRITICAL FIX: Yahoo Finance Chart API sometimes delays appending TODAY's close to the historical array.
        # But it always provides real-time today's data in the 'meta' object!
        meta = result[0].get('meta', {})
        latest_time = meta.get('regularMarketTime')
        latest_close = meta.get('regularMarketPrice')
        
        if latest_time is not None and latest_close is not None:
            dt_latest = pd.to_datetime(latest_time, unit='s', utc=True).tz_convert('Asia/Riyadh').date()
            if dt_latest not in dates:
                print(f"   ⚡ Fixing Yahoo Delay: Appending real-time today's data from Meta -> {dt_latest}")
                dates.append(dt_latest)
                clean_closes.append(latest_close)
                
        tasi_df = pd.DataFrame({'date': dates, 'market_close': clean_closes})
        tasi_df['date'] = pd.to_datetime(tasi_df['date'])
        
        print(f"✅ Successfully fetched {len(tasi_df)} daily TASI records.")
        if not tasi_df.empty:
            print(f"   📅 Last fetched TASI date from Yahoo: {tasi_df['date'].max().date()}")
            
        return tasi_df
    except Exception as e:
        print(f"❌ Failed to fetch TASI from Yahoo Finance: {e}")
        return pd.DataFrame()

def main():
    print("⏳ Loading max 1 year of price data from database...")
    
    with engine.connect() as conn:
        query = """
            SELECT symbol, date, close 
            FROM prices 
            WHERE close > 0 
              AND date IS NOT NULL 
              AND date >= CURRENT_DATE - INTERVAL '1 years'
            ORDER BY symbol, date
        """
        df = pd.read_sql(text(query), conn)
    
    if df.empty:
        print("⚠️ No data found.")
        return

    df['date'] = pd.to_datetime(df['date'])
    
    print("📉 Calculating EXTREMELY ACCURATE Beta vs actual TASI index...")
    
    # 1. Fetch Exact TASI Market Index Data (Value-Weighted Benchmark)
    market_df = fetch_tasi_yahoo(years=1)
    
    if market_df.empty:
        print("⚠️ Could not fetch exact TASI, using overall market average as fallback.")
        market_df = df.groupby('date')['close'].mean().reset_index()
        market_df.rename(columns={'close': 'market_close'}, inplace=True)
    
    # Sort securely
    df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
    market_df = market_df.sort_values('date').reset_index(drop=True)

    # 1) Calculate daily returns
    df['stock_return'] = df.groupby('symbol')['close'].pct_change()
    market_df['market_return'] = market_df['market_close'].pct_change()
    
    # 2) Calculate rolling market variance (260 trading days ~ 1 year, min 130)
    market_df['market_var'] = market_df['market_return'].rolling(window=260, min_periods=130).var()
    
    # 3) Merge market returns & variance to main DF aligned by date
    df = df.merge(market_df[['date', 'market_return', 'market_var']], on='date', how='left')

    # 4) Compute rolling covariance between stock & market
    def compute_covariance(sub_df):
        return sub_df['stock_return'].rolling(window=260, min_periods=130).cov(sub_df['market_return'])

    print("⏱️ Computing covariances (this might take a moment)...")
    df['cov_stock_market'] = df.groupby('symbol', group_keys=False).apply(compute_covariance)
    
    # 5) Compute Beta: Covariance / Variance
    df['beta'] = df['cov_stock_market'] / df['market_var']
    
    # Handle infinities
    df['beta'] = df['beta'].replace([np.inf, -np.inf], np.nan)
    
    # Filter only rows with valid beta
    valid_beta_df = df.dropna(subset=['beta']).copy()
    
    if valid_beta_df.empty:
        print("⚠️ No valid beta values calculated! Most stocks probably have less than 130 days of data.")
        return
        
    print(f"📊 Calculated valid Beta for {len(valid_beta_df)} daily record points.")
    
    # Reformat date back to pure date or datetime safely
    valid_beta_df['date'] = valid_beta_df['date'].dt.date

    # --------------------------------------------
    # BLAZING FAST BULK UPDATE using Temp Table
    # --------------------------------------------
    print("💾 Saving Beta to database 'stock_indicators' table using Fast Bulk Update...")
    try:
        # Write to temporary table
        print("   -> Writing valid data to temporary database table...")
        valid_beta_df[['symbol', 'date', 'beta']].to_sql('temp_beta_updates', con=engine, index=False, if_exists='replace')
        
        # Execute UPDATE FROM temp_table
        print("   -> Merging exact Beta values into stock_indicators...")
        db = SessionLocal()
        update_query = text("""
            UPDATE stock_indicators s
            SET beta = t.beta 
            FROM temp_beta_updates t 
            WHERE s.symbol = t.symbol AND s.date = t.date::date;
        """)
        db.execute(update_query)
        db.commit()
        
        # Drop temp table
        db.execute(text("DROP TABLE temp_beta_updates;"))
        db.commit()
        
        print("🎉 Beta calculation and historical backfill complete seamlessly!")
    except Exception as e:
        db.rollback()
        print(f"❌ Error saving to database: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
