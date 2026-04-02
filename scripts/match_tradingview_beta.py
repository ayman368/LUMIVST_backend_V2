"""
OFFICIAL ALGORITHMS TEST (Based on Research)
Tests Monthly vs Weekly returns over 1-year, 3-year, and 5-year horizons.
Targets: 1321=1.58, 2222=0.68, 1120=1.05, 2350=1.60, 2010=0.97
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import requests
import warnings
import time

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

def fetch_tasi_yahoo(years=6):
    """Fetch TASI from Yahoo for up to 6 years"""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/^TASI.SR?interval=1d&range={years}y"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=15)
        data = res.json()
        result = data.get('chart', {}).get('result')
        if not result:
            return pd.Series(dtype=float)
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        
        dates, vals = [], []
        for ts, c in zip(timestamps, closes):
            if c is not None:
                dt = pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Riyadh')
                dates.append(dt.date())
                vals.append(c)

        s = pd.Series(vals, index=pd.to_datetime(dates), name='market')
        return s.sort_index()
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

def calc_beta_formula(stock_series, market_series, rule='ME', months_window=12, ret_type='simple'):
    # Merge on daily index
    merged = pd.concat([
        stock_series.rename('stock'),
        market_series.rename('market')
    ], axis=1).dropna()

    if len(merged) < 30:
        return np.nan

    # Cutoff to Exact Window
    cutoff = merged.index.max() - pd.DateOffset(months=months_window)
    merged = merged[merged.index >= cutoff]

    # Resample
    resampled = merged.resample(rule, label='right', closed='right').last().dropna()
    
    if len(resampled) < 3:
        return np.nan

    # Returns
    if ret_type == 'simple':
        ret = resampled.pct_change().dropna()
    else:
        ret = np.log(resampled / resampled.shift(1)).dropna()

    cov = ret['stock'].cov(ret['market'])
    var = ret['market'].var()
    return cov / var if var != 0 else np.nan

def main():
    print("=" * 85)
    print("🎯 OFFICIAL ALGORITHM FINDER (Monthly vs Weekly | 1y vs 3y vs 5y)")
    print("=" * 85)

    print("\n📡 Fetching TASI from Yahoo (6 Years)…")
    tasi = fetch_tasi_yahoo(years=6)
    
    print("\n🗄️  Loading DB prices (6 Years)…")
    db_df = load_db_close(list(TARGETS.keys()), years=6)
    
    print("\n🔎 Testing formulas (Based on Research):")
    syms = list(TARGETS.keys())
    header_syms = " ".join([f"{s:>7}" for s in syms])
    header_targ = " ".join([f"({TARGETS[s]:.2f})" for s in syms])
    print(f"\n{'Formula':<35} {header_syms} {'TotErr':>8}")
    print(f"{'':35} {header_targ}")
    print("-" * 105)

    scenarios = [
        # (Rule, Months, RetType, Label Name)
        ('M', 60, 'simple', '5 Years (MarketSurge) Monthly'),
        ('M', 36, 'simple', '3 Years (GuruFocus) Monthly'),
        ('M', 12, 'simple', '1 Year Monthly'),
        ('M', 60, 'log',    '5 Years Monthly Log'),
        ('M', 36, 'log',    '3 Years Monthly Log'),
        ('M', 12, 'log',    '1 Year Monthly Log'),
        
        ('W-THU', 60, 'simple', '5 Years Weekly (Thu)'),
        ('W-THU', 36, 'simple', '3 Years Weekly (Thu)'),
        ('W-THU', 12, 'simple', '1 Year Weekly (Thu)'),
    ]

    all_res = []
    
    for rule, months, ret_type, name in scenarios:
        betas = {}
        for sym in TARGETS:
            stock_s = db_df[db_df['symbol'] == sym].set_index('date')['close'].astype(float)
            betas[sym] = calc_beta_formula(stock_s, tasi, rule=rule, months_window=months, ret_type=ret_type)
            
        total_err = sum(abs(betas[s] - TARGETS[s]) for s in TARGETS if not np.isnan(betas[s]))
        label = f"{name}"
        
        vals_str = " ".join([f"{betas[s]:>7.2f}" if not np.isnan(betas[s]) else "    N/A" for s in syms])
        star = " ⭐" if total_err < 0.35 else ""
        print(f"{label:<35} {vals_str} {total_err:>8.4f}{star}")
        
        all_res.append((label, total_err, betas))

    best = sorted(all_res, key=lambda x: x[1])[0]
    print("\n" + "=" * 85)
    print(f"🥇 BEST RESEARCH MATCH: {best[0]}")
    print(f"   Total Error: {best[1]:.4f}")
    for sym in TARGETS:
        diff = best[2][sym] - TARGETS[sym]
        status = "✅" if abs(diff) < 0.05 else ("⚠️" if abs(diff) < 0.15 else "❌")
        print(f"   {status} {sym} 계산값: {best[2][sym]:.4f} vs TradingView: {TARGETS[sym]} (الفرق: {diff:+.4f})")
    
if __name__ == "__main__":
    main()