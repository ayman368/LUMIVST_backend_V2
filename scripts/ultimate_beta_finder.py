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

TARGETS = {
    '1321': 1.58,
    '2222': 0.68,
    '1120': 1.05,
    '2350': 1.60,
    '2010': 0.97,
    '1050': 1.00,
    '1180': 1.15,
}

def fetch_yahoo_adjclose(symbol, years=6):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range={years}y"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=15)
        data = res.json()
        result = data.get('chart', {}).get('result')
        if not result:
            return pd.Series(dtype=float)
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        adjclose_arr = result[0].get('indicators', {}).get('adjclose', [{}])
        adjcloses = adjclose_arr[0].get('adjclose', []) if adjclose_arr and adjclose_arr[0] else closes
        
        dates, vals = [], []
        for i, ts in enumerate(timestamps):
            c = adjcloses[i] if i < len(adjcloses) else None
            if c is not None:
                dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                dates.append(dt.date())
                vals.append(float(c))
        
        s = pd.Series(vals, index=pd.to_datetime(dates), name=symbol).sort_index().dropna()
        return s[~s.index.duplicated(keep='last')]
    except Exception as e:
        return pd.Series(dtype=float)

def load_db_close(symbols, years=6):
    symbol_list = "', '".join(symbols)
    query = text(f"""
        SELECT symbol, date, close FROM prices
        WHERE symbol IN ('{symbol_list}') AND close > 0
          AND date >= CURRENT_DATE - INTERVAL '{years} years'
        ORDER BY symbol, date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    return df

def calc_beta(stock, market, period='W-THU', lookback_units=52, ret_type='simple'):
    merged = pd.concat([stock.rename('s'), market.rename('m')], axis=1).dropna()
    if len(merged) < 20:
        return np.nan
        
    if period == 'D':
        resampled = merged
    else:
        resampled = merged.resample(period, label='right', closed='right').last().dropna()
        
    if ret_type == 'simple':
        ret = resampled.pct_change().dropna()
    else:
        ret = np.log(resampled / resampled.shift(1)).dropna()
        
    ret = ret.tail(lookback_units)
    if len(ret) < lookback_units * 0.8:  # Require at least 80% valid data points
        return np.nan
        
    cov = ret['s'].cov(ret['m'])
    var = ret['m'].var()
    return cov / var if var != 0 else np.nan

def main():
    print("=" * 110)
    print("🎯 THE MASTER GRID SEARCH FOR TRADINGVIEW BETA")
    print("=" * 110)

    print("Fetching Yahoo TASI data...")
    tasi_yahoo = fetch_yahoo_adjclose('^TASI.SR', years=6)
    
    print("Fetching Yahoo Stocks (AdjClose)...")
    yahoo_stocks = {}
    for sym in TARGETS:
        yahoo_stocks[sym] = fetch_yahoo_adjclose(f"{sym}.SR", years=6)
        
    print("Loading DB Stocks (Raw Close)...")
    db_df = load_db_close(list(TARGETS.keys()), years=6)
    db_stocks = {}
    for sym in TARGETS:
        s = db_df[db_df['symbol'] == sym].set_index('date')['close'].astype(float)
        db_stocks[sym] = s[~s.index.duplicated(keep='last')]
        
    scenarios = []
    
    for src_name, stocks in [('DB Raw', db_stocks), ('Yahoo Adj', yahoo_stocks)]:
        for ret in ['simple', 'log']:
            # Daily Lengths (Trade days)
            for d in [20, 63, 126, 252, 504, 756]: # 1M, 3M, 6M, 1Y, 2Y, 3Y
                scenarios.append((f"{src_name}|Day|{d}d|{ret}", 'D', d, ret, stocks))
            
            # Weekly Lengths 
            for w in ['W-THU', 'W-TUE', 'W-SUN']:
                for wn in [26, 52, 53, 104, 156, 260]: # 6M, 1Y, 1Y+1, 2Y, 3Y, 5Y
                    scenarios.append((f"{src_name}|{w}|{wn}w|{ret}", w, wn, ret, stocks))
                
            # Monthly Lengths
            for mn in [12, 24, 36, 48, 60]: # 1Y to 5Y
                scenarios.append((f"{src_name}|Mon|{mn}m|{ret}", 'M', mn, ret, stocks))

    print(f"\nEvaluating {len(scenarios)} mathematical combinations...")
    
    results = []
    for label, period, lookback, ret, stocks in scenarios:
        betas = {}
        for sym in TARGETS:
            betas[sym] = calc_beta(stocks[sym], tasi_yahoo, period=period, lookback_units=lookback, ret_type=ret)
            
        err = sum(abs(betas[s] - TARGETS[s]) for s in TARGETS if not np.isnan(betas[s]))
        valid = sum(1 for s in TARGETS if not np.isnan(betas[s]))
        if valid == len(TARGETS):
            results.append({'Label': label, 'Err': err, 'Betas': betas})

    results.sort(key=lambda x: x['Err'])
    
    syms = list(TARGETS.keys())
    header_syms = " ".join([f"{s:>6}" for s in syms])
    header_targ = " ".join([f"({TARGETS[s]:.2f})" for s in syms])
    
    print("\n" + "="*110)
    print("🏆 TOP 15 EXACT MATCHES IN THE WORLD:")
    print("="*110)
    print(f"{'Algorithm Settings':<35} {header_syms} {'TotErr':>8}")
    print(f"{'':35} {header_targ}")
    print("-" * 110)
    
    for r in results[:15]:
        b = r['Betas']
        v_str = " ".join([f"{b[s]:>6.2f}" for s in syms])
        print(f"{r['Label']:<35} {v_str} {r['Err']:>8.4f}")

    print("\n" + "="*110)
    best = results[0]
    print(f"🥇 THE ABSOLUTE BEST: {best['Label']}")
    for s in syms:
        diff = best['Betas'][s] - TARGETS[s]
        icon = "✅" if abs(diff) <= 0.05 else "⚠️"
        print(f"   {icon} {s}: {best['Betas'][s]:.4f} (TV: {TARGETS[s]}) -> Diff: {diff:+.4f}")

if __name__ == "__main__":
    main()
