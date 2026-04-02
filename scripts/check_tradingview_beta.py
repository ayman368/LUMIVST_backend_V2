import requests
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

def fetch_data_yahoo(symbol, years=2):
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
        
        return series
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.Series(name=symbol, dtype=float)

def main():
    print("🔍 Fetching exact historical data for 1321.SR and ^TASI.SR from Yahoo Finance...")
    try:
        stock_series = fetch_data_yahoo('1321.SR', years=2)
        market_series = fetch_data_yahoo('^TASI.SR', years=2)
        
        if stock_series.empty or market_series.empty:
            print("❌ No data fetched. Check internet connection.")
            return

        # Combine into DataFrame
        df = pd.concat([stock_series, market_series], axis=1).dropna()
        
        if df.empty:
            print("❌ No data fetched. Check internet connection.")
            return
            
        # 1. MARKET SURGE METHOD: 260 days, Simple Returns
        # We need the last 261 days to calculate 260 returns
        df_260 = df.tail(261).copy()
        ret_260 = df_260.pct_change().dropna()
        cov_260 = ret_260['1321.SR'].cov(ret_260['^TASI.SR'])
        var_260 = ret_260['^TASI.SR'].var()
        beta_260_simple = cov_260 / var_260
        
        # 2. STANDARD TRADINGVIEW METHOD: 252 days, Simple Returns
        df_252 = df.tail(253).copy()
        ret_252 = df_252.pct_change().dropna()
        cov_252 = ret_252['1321.SR'].cov(ret_252['^TASI.SR'])
        var_252 = ret_252['^TASI.SR'].var()
        beta_252_simple = cov_252 / var_252
        
        # 3. TRADINGVIEW (Log Returns Method): Some platforms use logarithmic returns
        log_ret_252 = np.log(df_252 / df_252.shift(1)).dropna()
        log_cov_252 = log_ret_252['1321.SR'].cov(log_ret_252['^TASI.SR'])
        log_var_252 = log_ret_252['^TASI.SR'].var()
        beta_252_log = log_cov_252 / log_var_252

        print("\n📊 === RESULT COMPARISON (DAILY BETA for 1321) ===")
        print(f"1. MarketSurge (260 Days, Simple Returns): {beta_260_simple:.2f}")
        print(f"2. TradingView (252 Days, Simple Returns): {beta_252_simple:.2f}")
        print(f"3. TradingView (252 Days, Log Returns):    {beta_252_log:.2f}   <-- This might match 1.58 perfectly")
        
        print("\n💡 Conclusion:")
        print("Our current code uses MarketSurge (260 Days) successfully.")
        print("Changing to TradingView simply means changing 'window=260' to 252 in the code, and using np.log if preferred.")

    except Exception as e:
        print(f"❌ Error during calculation: {e}")

if __name__ == "__main__":
    main()
