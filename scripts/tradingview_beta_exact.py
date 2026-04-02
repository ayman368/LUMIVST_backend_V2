"""
TradingView Beta Calculator - Exact Method
حساب بيتا بالضبط كما تحسبها منصة TradingView
β = Covariance(Stock Returns, Market Returns) / Variance(Market Returns)
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import requests
import warnings

warnings.filterwarnings('ignore')

sys.path.append(str(Path(__file__).resolve().parent.parent))
from app.core.database import engine
from sqlalchemy import text

# Manual linear regression function (no scipy needed)
def manual_linregress(x, y):
    """Calculate linear regression manually without scipy"""
    x = np.array(x)
    y = np.array(y)
    
    n = len(x)
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    
    # Calculate slope (beta)
    numerator = np.sum((x - x_mean) * (y - y_mean))
    denominator = np.sum((x - x_mean) ** 2)
    slope = numerator / denominator if denominator != 0 else 0
    
    # Calculate intercept
    intercept = y_mean - slope * x_mean
    
    # Calculate R-squared
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
    
    # Calculate standard error
    residuals = y - y_pred
    std_error = np.sqrt(np.sum(residuals ** 2) / (n - 2)) / np.sqrt(np.sum((x - x_mean) ** 2))
    
    return {
        'slope': slope,
        'intercept': intercept,
        'rvalue': np.sqrt(r_squared),
        'pvalue': 0,  # Not calculating p-value manually
        'stderr': std_error
    }

def fetch_tasi_yahoo(years=20):
    """Fetch TASI from Yahoo Finance - use all available data"""
    try:
        # Use max period to get all available data
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/^TASI.SR?interval=1d&range=max"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=15)
        data = res.json()
        result = data.get('chart', {}).get('result')
        if not result:
            return pd.DataFrame()
        
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        
        dates, vals = [], []
        for ts, c in zip(timestamps, closes):
            if c is not None:
                dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                dates.append(dt.date())
                vals.append(c)

        df = pd.DataFrame({'date': dates, 'close': vals})
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').drop_duplicates('date')
    except Exception as e:
        print(f"❌ Error fetching TASI: {e}")
        return pd.DataFrame()

def load_stock_from_db(symbol, years=20):
    """Load stock price data from database - use all available data"""
    query = text(f"""
        SELECT date, close FROM prices
        WHERE symbol = '{symbol}' AND close > 0
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values('date').drop_duplicates('date')

def calculate_monthly_returns(df, col='close'):
    """Calculate monthly returns from daily prices (End of Month)"""
    df_sorted = df.sort_values('date').copy()
    
    # Group by Year-Month and take last value
    monthly = df_sorted.set_index('date').resample('M').last().dropna()
    
    if len(monthly) < 3:
        return pd.Series(dtype=float)
    
    # Calculate monthly returns (TradingView style)
    returns = monthly[col].pct_change().dropna()
    return returns


def calculate_weekly_returns(df, col='close', week='W-THU'):
    """Calculate weekly returns using end-of-week (Thursday) prices."""
    df_sorted = df.sort_values('date').copy()
    weekly = df_sorted.set_index('date').resample(week).last().dropna()

    if len(weekly) < 3:
        return pd.Series(dtype=float)

    returns = weekly[col].pct_change().dropna()
    return returns

def calculate_tradingview_beta(stock_df, market_df, period='monthly'):
    """
    Calculate Beta using TradingView method.
    period: 'monthly' or 'weekly'
    """
    if period == 'weekly':
        stock_returns = calculate_weekly_returns(stock_df, 'close', week='W-THU')
        market_returns = calculate_weekly_returns(market_df, 'close', week='W-THU')
        period_name = 'weekly'
    else:
        stock_returns = calculate_monthly_returns(stock_df, 'close')
        market_returns = calculate_monthly_returns(market_df, 'close')
        period_name = 'monthly'

    if len(stock_returns) < 3 or len(market_returns) < 3:
        return None

    merged = pd.concat([stock_returns, market_returns], axis=1, keys=['stock', 'market']).dropna()
    if len(merged) < 3:
        return None

    covariance = merged['stock'].cov(merged['market'])
    market_variance = merged['market'].var()
    if market_variance == 0:
        return None

    beta = covariance / market_variance
    correlation = merged['stock'].corr(merged['market'])
    r_squared = correlation ** 2

    return {
        'beta': beta,
        'r_squared': r_squared,
        'correlation': correlation,
        'data_points': len(merged),
        'covariance': covariance,
        'market_variance': market_variance,
        'period': period_name
    }

def calculate_linear_regression_beta(stock_df, market_df):
    """Alternative: Using linear regression (another TradingView method)"""
    stock_returns = calculate_monthly_returns(stock_df, 'close')
    market_returns = calculate_monthly_returns(market_df, 'close')
    
    merged = pd.concat([stock_returns, market_returns], axis=1, keys=['stock', 'market']).dropna()
    
    if len(merged) < 3:
        return None
    
    # Linear regression: Y = α + β*X
    res = manual_linregress(merged['market'], merged['stock'])
    
    return {
        'beta': res['slope'],
        'alpha': res['intercept'],
        'r_squared': res['rvalue'] ** 2,
        'p_value': res['pvalue'],
        'std_error': res['stderr'],
        'months_of_data': len(merged)
    }

def main():
    print("=" * 90)
    print("📊 TradingView Beta Calculator (Exact Method - All Available Data)")
    print("=" * 90)
    
    # Test symbols from TradingView
    test_symbols = ['1321', '2222', '1120', '2350', '2010']
    
    print("\n📡 Fetching TASI from Yahoo Finance (ALL available data)...")
    market_df = fetch_tasi_yahoo(years=20)  # Use all available data
    
    if market_df.empty:
        print("❌ Failed to fetch market data")
        return
    
    print(f"✅ TASI data: {len(market_df)} daily records ({market_df['date'].min().date()} to {market_df['date'].max().date()})")
    
    print("\n" + "=" * 90)
    print("📋 Beta Calculation Results (TradingView Method - Monthly Returns)")
    print("=" * 90)
    print(f"{'Symbol':<10} {'Beta':<10} {'R²':<10} {'Correlation':<12} {'Data Points':<12} {'Method'}")
    print("-" * 90)
    
    for symbol in test_symbols:
        print(f"\n📥 Loading {symbol} from database...")
        stock_df = load_stock_from_db(symbol, years=20)  # Use all available data
        
        if stock_df.empty:
            print(f"❌ {symbol}: No data found")
            continue
        
        print(f"   Data: {len(stock_df)} daily records ({stock_df['date'].min().date()} to {stock_df['date'].max().date()})")
        
        # Method 1: Covariance/Variance (Monthly)
        result_cov_monthly = calculate_tradingview_beta(stock_df, market_df, period='monthly')
        # Method 2: Covariance/Variance (Weekly Thu)
        result_cov_weekly = calculate_tradingview_beta(stock_df, market_df, period='weekly')
        # Method 3: Linear Regression (Monthly)
        result_reg = calculate_linear_regression_beta(stock_df, market_df)

        if result_cov_monthly:
            print(f"{symbol:<10} {result_cov_monthly['beta']:<10.4f} {result_cov_monthly['r_squared']:<10.4f} {result_cov_monthly['correlation']:<12.4f} {result_cov_monthly['data_points']:<12} Monthly")
        if result_cov_weekly:
            print(f"{symbol:<10} {result_cov_weekly['beta']:<10.4f} {result_cov_weekly['r_squared']:<10.4f} {result_cov_weekly['correlation']:<12.4f} {result_cov_weekly['data_points']:<12} Weekly")
        if result_reg:
            print(f"{'(Reg)':<10} {result_reg['beta']:<10.4f} {result_reg['r_squared']:<10.4f} {'-':<12} {result_reg['months_of_data']:<12} LinearReg")
    
    print("\n" + "=" * 90)
    print("💡 Notes:")
    print("   • Beta > 1.0 : Stock is more volatile than market")
    print("   • Beta = 1.0 : Stock moves with market")
    print("   • Beta < 1.0 : Stock is less volatile than market")
    print("   • R² shows how well returns align (closer to 1.0 is better)")
    print("   • Using ALL available historical data (like TradingView)")
    print("=" * 90)

if __name__ == "__main__":
    main()
