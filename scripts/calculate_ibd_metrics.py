import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import date, timedelta
from sqlalchemy import text, update
from sqlalchemy.orm import Session

# Setup Paths
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.models.rs_daily import RSDaily
from app.models.price import Price

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IBDMetricsCalculator:
    def __init__(self, db: Session):
        self.db = db

    def get_letter_grade(self, percentile: float) -> str:
        """
        Convert percentile (0-100) to Letter Grade (A+ to E).
        Top 7% -> A+, Next 8% -> A, etc.
        """
        if percentile >= 93: return 'A+'
        if percentile >= 85: return 'A'
        if percentile >= 77: return 'A-'
        if percentile >= 70: return 'B+'
        if percentile >= 63: return 'B'
        if percentile >= 56: return 'B-'
        if percentile >= 49: return 'C+'
        if percentile >= 42: return 'C'
        if percentile >= 35: return 'C-'
        if percentile >= 28: return 'D+'
        if percentile >= 21: return 'D'
        if percentile >= 14: return 'D-'
        return 'E'

    def load_data(self, lookback_days=200):
        """Load Price data for calculation."""
        logger.info("📡 Loading Price Data...")
        query = text("""
            SELECT symbol, date, close, high, low, volume_traded, industry_group, sector, industry, sub_industry
            FROM prices
            WHERE date >= :start_date
            ORDER BY symbol, date ASC
        """)
        start_date = date.today() - timedelta(days=lookback_days)
        
        # Use connection explicitly for Pandas/SQLAlchemy 2.0 compatibility
        with self.db.bind.connect() as connection:
            df = pd.read_sql(query, connection, params={"start_date": start_date})
        
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"📊 Loaded {len(df)} price records.")
        return df

    def calculate_group_rs(self, df_prices, target_date=None):
        """
        Calculate RS for Sector, Industry Group, Industry, Sub-Industry.
        metric: 6-month price performance.
        """
        if target_date is None:
            target_date = date.today()
        
        target_date = pd.to_datetime(target_date)
        
        # 1. Calculate 6-Month % Change for each stock
        # We need price at target_date and price 6 months ago (approx 126 trading days or 180 calendar days)
        
        # Filter for data up to target_date
        df = df_prices[df_prices['date'] <= target_date].copy()
        
        # Get latest close for each symbol (Current Price)
        latest_prices = df.sort_values('date').groupby('symbol').tail(1)[['symbol', 'close', 'sector', 'industry_group', 'industry', 'sub_industry']]
        latest_prices = latest_prices.rename(columns={'close': 'current_close'})
        
        # Get price ~6 months ago (Look back 180 days)
        six_months_ago = target_date - timedelta(days=180)
        
        # Find the closest date to 6 months ago for each symbol
        # Strategy: Get all records >= 180 days ago, take the first one
        old_prices_df = df[df['date'] >= six_months_ago].sort_values('date').groupby('symbol').head(1)[['symbol', 'close']]
        old_prices_df = old_prices_df.rename(columns={'close': 'old_close'})
        
        # Merge
        merged = pd.merge(latest_prices, old_prices_df, on='symbol', how='inner')
        
        # Calculate Check: Ensure the "old" price is actually old enough (e.g. at least 5 months difference)
        # For simplicity, we assume the query filter worked.
        
        merged['pct_change_6m'] = (merged['current_close'] - merged['old_close']) / merged['old_close']
        
        logger.info(f"Computing Group RS for {len(merged)} stocks...")

        results = {} # Map symbol -> { 'sector_rs': 'A', ... }

        # Helper to process each hierarchy level
        hierarchy_cols = {
            'sector': 'sector_rs_rating', 
            'industry_group': 'industry_group_rs_rating',
            'industry': 'industry_rs_rating', 
            'sub_industry': 'sub_industry_rs_rating'
        }

        for col, result_col in hierarchy_cols.items():
            # Group by hierarchy column
            # Check if column exists and not null
            valid_groups = merged[merged[col].notna() & (merged[col] != '')]
            
            # Avg return per group
            group_perf = valid_groups.groupby(col)['pct_change_6m'].mean().reset_index()
            
            if group_perf.empty:
                continue

            # Rank Groups (Percentile)
            # rank(pct=True) returns 0.0 to 1.0. We want 1.0 to be best.
            # pct_change is higher = better.
            group_perf['percentile'] = group_perf['pct_change_6m'].rank(pct=True) * 100
            
            # Apply Grading
            group_perf['grade'] = group_perf['percentile'].apply(self.get_letter_grade)
            
            # Map grade back to each symbol — vectorized (no iterrows)
            grade_map = dict(zip(group_perf[col], group_perf['grade']))

            # Map back to symbols — fully vectorized with zip (أسرع من iterrows)
            sym_grade_df = valid_groups[['symbol', col]].copy()
            sym_grade_df['grade'] = sym_grade_df[col].map(grade_map)
            sym_grade_df = sym_grade_df.dropna(subset=['grade'])

            for sym, grade in zip(sym_grade_df['symbol'], sym_grade_df['grade']):
                if sym not in results:
                    results[sym] = {}
                results[sym][result_col] = grade

        return results

    def calculate_acc_dis(self, df_prices, target_date=None):
        """
        Calculate Accumulation/Distribution Rating (13-Weeks).
        """
        if target_date is None:
            target_date = date.today()
        
        target_date = pd.to_datetime(target_date)
        start_date = target_date - timedelta(weeks=13) # Approx 90 days
        
        # Filter data for last 13 weeks
        mask = (df_prices['date'] >= start_date) & (df_prices['date'] <= target_date)
        df = df_prices.loc[mask].copy()
        
        if df.empty:
            return {}

        # 1. Calculate CLV (Close Location Value)
        # CLV = ((C - L) - (H - C)) / (H - L)
        # Range -1 to +1
        denom = df['high'] - df['low']
        # Avoid division by zero
        df['clv'] = np.where(denom == 0, 0, ((df['close'] - df['low']) - (df['high'] - df['close'])) / denom)
        
        # 2. Money Flow Volume
        df['mfv'] = df['clv'] * df['volume_traded']
        
        # 3. Sum for each symbol
        acc_dis_scores = df.groupby('symbol')['mfv'].sum().reset_index()
        acc_dis_scores.rename(columns={'mfv': 'total_mfv'}, inplace=True)
        
        # 4. Rank and Grade
        acc_dis_scores['percentile'] = acc_dis_scores['total_mfv'].rank(pct=True) * 100
        acc_dis_scores['grade'] = acc_dis_scores['percentile'].apply(self.get_letter_grade)
        
        # Result Map
        return dict(zip(acc_dis_scores['symbol'], acc_dis_scores['grade']))

    def save_results(self, group_rs_results, acc_dis_results, target_date):
        """Update DB records."""
        logger.info(f"💾 Saving IBD Metrics to DB for {target_date}...")
        
        # We need to update existing rows in rs_daily_v2
        # Or check if they exist. Usually daily_market_update runs first, so rows exist.
        
        count = 0
        
        # Merge results by symbol
        all_symbols = set(group_rs_results.keys()) | set(acc_dis_results.keys())
        
        updates = []
        for sym in all_symbols:
            grp = group_rs_results.get(sym, {})
            ad = acc_dis_results.get(sym, None)
            
            update_data = {
                'symbol': sym,
                'date': target_date,
                'sector_rs_rating': grp.get('sector_rs_rating'),
                'industry_group_rs_rating': grp.get('industry_group_rs_rating'),
                'industry_rs_rating': grp.get('industry_rs_rating'),
                'sub_industry_rs_rating': grp.get('sub_industry_rs_rating'),
                'acc_dis_rating': ad
            }
            updates.append(update_data)
        
        if not updates:
            logger.warning("No updates to save.")
            return

        # Bulk Update
        # SQLAlchemy Core bulk update is tricky because we need to match WHERE symbol=X AND date=Y
        # The easiest way for a batch this size (hundreds/thousands) is individual updates or bindparams?
        # Let's try to map over the existing objects if possible, or use raw SQL for speed.
        
        # Using raw SQL for performance on update
        # Postgres supports update from values? Or just simple loop for now (safest).
        # We assume rows exist. IF they don't, we skip? 
        # Typically the RS Calculation script runs BEFORE this, creating the rows.
        
        total = len(updates)
        logger.info(f"Preparing to update {total} records...")
        
        # We will use sqlalchemy 2.0 style updates
        # But looping is easiest for stability right now.
        
        try:
            for batch in chunk(updates, 500):
                # We can't easily bulk update with composite key in standard ORM bulk_update_mappings without fetching.
                # Let's try raw SQL execute with case/binds? 
                # Actually, standard loop with commit every 500 is fine for 2000 stocks (approx 4 batches).
                
                for item in batch:
                    stmt = update(RSDaily).where(
                        RSDaily.symbol == item['symbol'],
                        RSDaily.date == item['date']
                    ).values(
                        sector_rs_rating=item['sector_rs_rating'],
                        industry_group_rs_rating=item['industry_group_rs_rating'],
                        industry_rs_rating=item['industry_rs_rating'],
                        sub_industry_rs_rating=item['sub_industry_rs_rating'],
                        acc_dis_rating=item['acc_dis_rating']
                    )
                    self.db.execute(stmt)
                
                self.db.commit()
                count += len(batch)
                logger.info(f"Updated {count}/{total}...")
                
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            self.db.rollback()
            
        logger.info("✅ IBD Metrics Saved.")

def chunk(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def main():
    db = SessionLocal()
    try:
        calc = IBDMetricsCalculator(db)
        
        # Load Data (Last 200 days approx 6-7 months to cover both 6m RS and 13w Acc/Dis)
        df_prices = calc.load_data(lookback_days=230) 
        
        # Target Date (Today or specific)
        target_date = date.today()
        # Ensure we have price data for today? If run after market close.
        # If running on weekend, maybe take last available date?
        # Let's grab the max date from DF to be safe
        if not df_prices.empty:
            max_date = df_prices['date'].max().date()
            if max_date < target_date:
                logger.warning(f"Using latest data date: {max_date} instead of {target_date}")
                target_date = max_date

        logger.info(f"🚀 Calculating Metrics for {target_date}...")

        # 1. Group RS
        group_rs_map = calc.calculate_group_rs(df_prices, target_date)
        
        # 2. Acc/Dis
        acc_dis_map = calc.calculate_acc_dis(df_prices, target_date)
        
        # 3. Save
        calc.save_results(group_rs_map, acc_dis_map, target_date)

    except Exception as e:
        logger.error(f"Failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    main()