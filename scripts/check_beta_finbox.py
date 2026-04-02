import requests
import pandas as pd
import numpy as np
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

def fetch_data_yahoo(symbol, years=1):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range={years}y"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        result = data.get('chart', {}).get('result')
        if not result:
            return pd.Series(name=symbol, dtype=float)
            
        timestamps = result[0].get('timestamp', [])
        quote = result[0].get('indicators', {}).get('quote', [{}])[0]
        closes = quote.get('close', [])
        
        dates = []
        clean_closes = []
        for ts, c in zip(timestamps, closes):
            if c is not None:
                dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                dates.append(dt.date())
                clean_closes.append(c)

        meta = result[0].get('meta', {})
        latest_time = meta.get('regularMarketTime')
        latest_close = meta.get('regularMarketPrice')
        
        if latest_time is not None and latest_close is not None:
            dt_latest = pd.to_datetime(latest_time, unit='s', utc=True).tz_convert('Asia/Riyadh').date()
            if dt_latest not in dates:
                dates.append(dt_latest)
                clean_closes.append(latest_close)
                
        series = pd.Series(clean_closes, index=pd.to_datetime(dates), name=symbol)
        series = series[series.index >= pd.Timestamp.now() - pd.DateOffset(years=years)]
        return series
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.Series(name=symbol, dtype=float)

def main():
    print("🔍 Fetching exact 1-Year historical data for 1321.SR and ^TASI.SR from Yahoo Finance...")
    try:
        stock_series = fetch_data_yahoo('1321.SR', years=1)
        market_series = fetch_data_yahoo('^TASI.SR', years=1)
        
        if stock_series.empty or market_series.empty:
            print("❌ No data fetched. Check internet connection.")
            return

        # Combine into DataFrame
        closes = pd.concat([stock_series, market_series], axis=1).dropna()
        
        if closes.empty:
            print("❌ No data fetched. Check internet connection.")
            return

        print(f"✅ Data fetched successfully! ({len(closes)} trading days)")
        
        # ---------------------------------------------------------
        # 1. DAILY BETA (What our platform currently uses)
        # ---------------------------------------------------------
        daily_ret = closes.pct_change().dropna()
        daily_cov = daily_ret['1321.SR'].cov(daily_ret['^TASI.SR'])
        daily_var = daily_ret['^TASI.SR'].var()
        daily_beta = daily_cov / daily_var
        
        # ---------------------------------------------------------
        # 2. WEEKLY BETA (What Finbox most likely uses for 1-Year)
        # ---------------------------------------------------------
        # Resample closing prices to Weekly (End of Week)
        weekly_closes = closes.resample('W-THU').last().dropna() # Saudi market closes on Thursday
        weekly_ret = weekly_closes.pct_change().dropna()
        weekly_cov = weekly_ret['1321.SR'].cov(weekly_ret['^TASI.SR'])
        weekly_var = weekly_ret['^TASI.SR'].var()
        weekly_beta = weekly_cov / weekly_var
        
        # ---------------------------------------------------------
        # 3. MONTHLY BETA (Standard traditional Beta, normally 3Y or 5Y)
        # ---------------------------------------------------------
        monthly_closes = closes.resample('M').last().dropna()
        monthly_ret = monthly_closes.pct_change().dropna()
        monthly_cov = monthly_ret['1321.SR'].cov(monthly_ret['^TASI.SR'])
        monthly_var = monthly_ret['^TASI.SR'].var()
        monthly_beta = monthly_cov / monthly_var
        
        print("\n📊 === RESULT COMPARISON (1-YEAR BETA) ===")
        print(f"1. Daily Beta (Our Script):  {daily_beta:.2f}   (High sensitivity to daily noise)")
        print(f"2. Weekly Beta (Finbox style): {weekly_beta:.2f}   (Smoothed, common for 1y)")
        print(f"3. Monthly Beta:             {monthly_beta:.2f}   (Traditional finance standard)")
        
        print("\n💡 Conclusion:")
        print("- There is no 'wrong' way mathematically; it depends purely on the timeframe used for returns.")
        print("- If you want your platform's Beta to look like Finbox (0.45-0.64), you should calculate it on WEEKLY closing prices.")
        print("- MarketSmith generally uses DAILY returns for 260 days, which is why your initial request had me build it using Daily logic (1.32).")

    except Exception as e:
        print(f"❌ Error during calculation: {e}")

if __name__ == "__main__":
    main()
