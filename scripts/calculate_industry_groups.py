import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import date, timedelta, datetime
from sqlalchemy import text, func, desc
from sqlalchemy.orm import Session

# Setup Paths
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.core.database import SessionLocal
from app.models.industry_group import IndustryGroupHistory

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IndustryGroupCalculator:
    def __init__(self, db: Session):
        self.db = db
    
    def calculate_group_index_prices(self, target_date: date):
        """
        Calculate industry group index prices (market cap weighted)
        for current day and historical periods (3 and 6 months)
        """
        logger.info(f"📊 Calculating group indices for {target_date}")
        
        # Step 1: Find 3 and 6 months ago dates (Trading Days)
        query_dates = text("""
            WITH trading_dates AS (
                SELECT DISTINCT date 
                FROM prices 
                WHERE date <= :target_date 
                AND date IS NOT NULL
                ORDER BY date DESC
            ),
            date_rows AS (
                SELECT date, ROW_NUMBER() OVER (ORDER BY date DESC) as rn
                FROM trading_dates
            )
            SELECT 
                MAX(CASE WHEN rn = 1 THEN date END) as current_date,
                MAX(CASE WHEN rn = 63 THEN date END) as date_3m_ago,
                MAX(CASE WHEN rn = 126 THEN date END) as date_6m_ago
            FROM date_rows
        """)
        
        try:
            with self.db.bind.connect() as connection:
                dates_result = pd.read_sql(query_dates, connection, params={"target_date": target_date})
        except Exception as e:
            logger.error(f"❌ Error fetching dates: {e}")
            return {}
        
        if dates_result.empty or dates_result['current_date'].iloc[0] is None:
            logger.warning(f"❌ No trading dates found for {target_date}")
            return {}
        
        # Step 2: Get Market Cap Weighted Prices for each date
        all_group_data = {}
        
        for period_name, period_date in [
            ('current', dates_result['current_date'].iloc[0]),
            ('3m_ago', dates_result['date_3m_ago'].iloc[0]),
            ('6m_ago', dates_result['date_6m_ago'].iloc[0])
        ]:
            if pd.isna(period_date):
                logger.warning(f"❌ Missing date for {period_name}")
                continue
            
            try:
                # Use Equal-Weighted Index (Simple Average of Closes)
                # This is necessary because historical market_cap data is missing for 99% of records
                query_group_price = text("""
                    WITH stock_data AS (
                        SELECT DISTINCT ON (p.symbol)
                            p.symbol,
                            p.close,
                            p.industry_group
                        FROM prices p
                        WHERE p.date = :period_date
                        AND p.industry_group IS NOT NULL 
                        AND TRIM(p.industry_group) != ''
                        AND p.close > 0
                        ORDER BY p.symbol, p.date DESC
                    )
                    SELECT 
                        industry_group,
                        AVG(close) as group_index_price,
                        COUNT(symbol) as stock_count
                    FROM stock_data
                    GROUP BY industry_group
                    HAVING COUNT(symbol) >= 1
                """)
                
                with self.db.bind.connect() as connection:
                    period_data = pd.read_sql(query_group_price, connection, 
                                             params={"period_date": period_date})
                
                for _, row in period_data.iterrows():
                    group_name = row['industry_group']
                    if group_name not in all_group_data:
                        all_group_data[group_name] = {}
                    all_group_data[group_name][period_name] = row['group_index_price']
                
                logger.info(f"   📊 {period_name}: {len(period_data)} groups for {period_date}")
                
            except Exception as e:
                logger.error(f"   ❌ Error fetching {period_name} data: {e}")
                continue
        
        # Filter groups with all three periods
        complete_groups = {
            group: prices for group, prices in all_group_data.items()
            if all(k in prices for k in ['current', '3m_ago', '6m_ago'])
        }
        
        logger.info(f"✅ Found {len(complete_groups)} groups with complete data")
        return complete_groups
    
    def calculate_ibd_group_score(self, group_indices):
        """
        Calculate IBD industry group score using CORRECT weighted formula
        Based on IBD's methodology for Industry Groups (6 months timeframe)
        """
        if not group_indices:
            logger.warning("❌ No group indices to calculate scores")
            return pd.DataFrame()
        
        results = []
        
        for industry_group, prices in group_indices.items():
            try:
                C = prices['current']
                C3 = prices['3m_ago']
                C6 = prices['6m_ago']
                
                # Validate data
                if C <= 0 or C3 <= 0 or C6 <= 0:
                    continue
                
                # CORRECT IBD Formula for Industry Groups (6 months)
                # Weighted: Recent 3 months gets more weight than full 6 months
                perf_3m = (C - C3) / C3  # Last 3 months performance
                perf_6m = (C - C6) / C6  # Last 6 months performance
                
                # STRONGER weight for recent performance (2:1 ratio)
                # This captures sector rotation faster
                ibd_score = (2.0 * perf_3m) + (1.0 * perf_6m)
                
                # YTD Change is usually similar to 6-month for this calculation
                # In IBD, YTD for groups is often the 6-month performance
                ytd_change = perf_6m
                
                results.append({
                    'industry_group': industry_group,
                    'perf_3m': perf_3m * 100,
                    'perf_6m': perf_6m * 100,
                    'ibd_score': ibd_score * 100,
                    'ytd_change': ytd_change * 100
                })
                
            except Exception as e:
                logger.warning(f"⚠️ Error calculating score for {industry_group}: {e}")
                continue
        
        if not results:
            logger.warning("❌ No valid scores calculated")
            return pd.DataFrame()
        
        logger.info(f"✅ Calculated IBD scores for {len(results)} groups")
        return pd.DataFrame(results)
    
    def get_ytd_change(self, target_date: date):
        """
        Get YTD change from beginning of year (more accurate)
        """
        query_ytd = text("""
            WITH first_day_of_year AS (
                SELECT MIN(date) as start_date
                FROM prices 
                WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM :target_date)
                AND date <= :target_date
            ),
            start_prices AS (
                SELECT DISTINCT ON (p.symbol)
                    p.symbol,
                    p.industry_group,
                    p.close as start_price,
                    COALESCE(p.market_cap, 0) as start_mcap
                FROM prices p, first_day_of_year f
                WHERE p.date = f.start_date
                AND p.industry_group IS NOT NULL
                AND TRIM(p.industry_group) != ''
                ORDER BY p.symbol, p.date DESC
            ),
            current_prices AS (
                SELECT DISTINCT ON (p.symbol)
                    p.symbol,
                    p.industry_group,
                    p.close as current_price,
                    COALESCE(p.market_cap, 0) as current_mcap
                FROM prices p
                WHERE p.date = :target_date
                AND p.industry_group IS NOT NULL
                AND TRIM(p.industry_group) != ''
                ORDER BY p.symbol, p.date DESC
            ),
            valid_groups AS (
                SELECT 
                    sp.industry_group,
                    COUNT(DISTINCT sp.symbol) as stock_count
                FROM start_prices sp
                JOIN current_prices cp ON sp.symbol = cp.symbol 
                    AND sp.industry_group = cp.industry_group
                WHERE sp.start_price > 0 AND cp.current_price > 0
                GROUP BY sp.industry_group
                HAVING COUNT(DISTINCT sp.symbol) >= 1
            )
            SELECT 
                sp.industry_group,
                -- Equal-Weighted YTD Change (Simple Average of individual stock returns)
                AVG((cp.current_price - sp.start_price) / sp.start_price) as ytd_change
            FROM start_prices sp
            JOIN current_prices cp ON sp.symbol = cp.symbol 
                AND sp.industry_group = cp.industry_group
            WHERE sp.industry_group IN (SELECT industry_group FROM valid_groups)
            GROUP BY sp.industry_group
        """)
        
        try:
            with self.db.bind.connect() as connection:
                ytd_df = pd.read_sql(query_ytd, connection, 
                                    params={"target_date": target_date})
            return ytd_df
        except Exception as e:
            logger.error(f"❌ Error fetching YTD data: {e}")
            return pd.DataFrame()
    
    def prepare_summary_data(self, group_df, target_date):
        """
        Prepare final summary data with all required columns
        """
        if group_df.empty:
            logger.warning("❌ No group data to prepare summary")
            return pd.DataFrame()
        
        # Get group details with market cap
        query_details = text("""
            WITH latest_prices AS (
                SELECT DISTINCT ON (symbol)
                    symbol, 
                    industry_group,
                    sector,
                    COALESCE(market_cap, 0) as market_cap
                FROM prices 
                WHERE date = :target_date
                AND industry_group IS NOT NULL
                AND TRIM(industry_group) != ''
                ORDER BY symbol, date DESC
            ),
            valid_groups AS (
                SELECT industry_group
                FROM latest_prices
                GROUP BY industry_group
                HAVING COUNT(DISTINCT symbol) >= 1
            )
            SELECT 
                lp.industry_group,
                COALESCE(MAX(lp.sector), 'Unknown') as sector,
                COUNT(DISTINCT lp.symbol) as number_of_stocks,
                SUM(lp.market_cap) / 1000000000 as market_cap_bil
            FROM latest_prices lp
            INNER JOIN valid_groups vg ON lp.industry_group = vg.industry_group
            GROUP BY lp.industry_group
        """)
        
        try:
            with self.db.bind.connect() as connection:
                details_df = pd.read_sql(query_details, connection, 
                                        params={"target_date": target_date})
        except Exception as e:
            logger.error(f"❌ Error fetching group details: {e}")
            return pd.DataFrame()
        
        if details_df.empty:
            logger.warning("❌ No group details found")
            return pd.DataFrame()
        
        # Get more accurate YTD change
        ytd_df = self.get_ytd_change(target_date)
        
        # Merge all data
        summary_df = pd.merge(
            group_df, 
            details_df, 
            on='industry_group', 
            how='inner'
        )
        
        if not ytd_df.empty:
            summary_df = pd.merge(
                summary_df,
                ytd_df[['industry_group', 'ytd_change']],
                on='industry_group',
                how='left',
                suffixes=('', '_accurate')
            )
            # Use accurate YTD if available
            summary_df['ytd_change'] = summary_df.apply(
                lambda row: row['ytd_change_accurate'] * 100 
                if pd.notna(row['ytd_change_accurate']) 
                else row['ytd_change'],
                axis=1
            )
            summary_df = summary_df.drop(columns=['ytd_change_accurate'])
        
        if summary_df.empty:
            logger.warning("❌ No matching groups between scores and details")
            return pd.DataFrame()
        
        # Calculate rank based on IBD score
        summary_df['rank'] = summary_df['ibd_score'].rank(
            ascending=False, 
            method='min'
        ).astype(int)
        
        summary_df['total_groups'] = len(summary_df)
        
        # Letter grade (A+ to E)
        summary_df['letter_grade'] = summary_df.apply(
            lambda row: self._calculate_letter_grade(row['rank'], row['total_groups']), 
            axis=1
        )
        
        # Get historical ranks
        summary_df = self.get_historical_ranks(summary_df, target_date)
        
        # Calculate changes in rank
        summary_df['change_vs_last_week'] = summary_df.apply(
            lambda row: self._calculate_rank_change(row['rank'], row['rank_1_week_ago']), 
            axis=1
        )
        
        summary_df['change_vs_3m_ago'] = summary_df.apply(
            lambda row: self._calculate_rank_change(row['rank'], row['rank_3_months_ago']), 
            axis=1
        )
        
        summary_df['change_vs_6m_ago'] = summary_df.apply(
            lambda row: self._calculate_rank_change(row['rank'], row['rank_6_months_ago']), 
            axis=1
        )
        
        # Clean up columns for display
        summary_df = summary_df[[
            'industry_group', 'sector', 'number_of_stocks', 'rank',
            'letter_grade', 'ibd_score', 'ytd_change', 'market_cap_bil',
            'rank_1_week_ago', 'rank_3_months_ago', 'rank_6_months_ago',
            'change_vs_last_week', 'change_vs_3m_ago', 'change_vs_6m_ago'
        ]]
        
        logger.info(f"📋 Prepared summary for {len(summary_df)} groups")
        return summary_df
    
    def _calculate_letter_grade(self, rank, total_groups):
        """Calculate IBD-style letter grade (A+ to E)"""
        if total_groups == 0:
            return 'E'
        
        percentile = (rank / total_groups) * 100
        
        if percentile <= 5:  # Top 5%
            return 'A+'
        elif percentile <= 20:  # Top 20%
            return 'A'
        elif percentile <= 40:  # Top 40%
            return 'B'
        elif percentile <= 60:  # Top 60%
            return 'C'
        elif percentile <= 80:  # Top 80%
            return 'D'
        else:
            return 'E'
    
    def _calculate_rank_change(self, current_rank, historical_rank):
        """Calculate change in rank (positive = improved)"""
        if pd.isna(historical_rank) or historical_rank <= 0:
            return None
        return int(historical_rank) - int(current_rank)
    
    def get_historical_ranks(self, current_summary, target_date):
        """
        Look up historical ranks from database
        """
        # Define historical dates
        d_1w = target_date - timedelta(days=7)
        d_3m = target_date - timedelta(days=90)
        d_6m = target_date - timedelta(days=180)
        
        # Initialize columns
        current_summary['rank_1_week_ago'] = None
        current_summary['rank_3_months_ago'] = None
        current_summary['rank_6_months_ago'] = None
        
        # Check if history table exists
        try:
            table_exists = self.db.execute(text(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'industry_group_history')"
            )).scalar()
            
            if not table_exists:
                logger.info("ℹ️ No historical table available")
                return current_summary
        except Exception as e:
            logger.warning(f"⚠️ Could not check table existence: {e}")
            return current_summary
        
        # Fetch historical ranks
        for period_name, query_date in [
            ('rank_1_week_ago', d_1w),
            ('rank_3_months_ago', d_3m),
            ('rank_6_months_ago', d_6m)
        ]:
            try:
                query = text("""
                    WITH closest_date AS (
                        SELECT date
                        FROM industry_group_history
                        WHERE date <= :query_date
                        ORDER BY date DESC
                        LIMIT 1
                    )
                    SELECT industry_group, rank
                    FROM industry_group_history h, closest_date c
                    WHERE h.date = c.date
                """)
                
                with self.db.bind.connect() as connection:
                    hist_df = pd.read_sql(query, connection, 
                                         params={"query_date": query_date})
                
                if not hist_df.empty:
                    rank_dict = dict(zip(hist_df['industry_group'], hist_df['rank']))
                    current_summary[period_name] = current_summary['industry_group'].map(rank_dict)
                    logger.info(f"   📜 Found {len(rank_dict)} historical ranks for {period_name}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error fetching {period_name}: {e}")
                continue
        
        return current_summary
    
    def save(self, summary_df, target_date):
        """Save results to database"""
        if summary_df.empty:
            logger.warning("❌ No data to save")
            return
        
        logger.info(f"💾 Saving {len(summary_df)} groups for {target_date}...")
        
        saved_count = 0
        error_count = 0
        
        for _, row in summary_df.iterrows():
            try:
                # Check if record exists
                existing = self.db.query(IndustryGroupHistory).filter(
                    IndustryGroupHistory.date == target_date,
                    IndustryGroupHistory.industry_group == row['industry_group']
                ).first()
                
                if not existing:
                    existing = IndustryGroupHistory(
                        date=target_date,
                        industry_group=row['industry_group']
                    )
                    self.db.add(existing)
                
                # Update all fields
                existing.sector = str(row.get('sector', ''))[:100]
                existing.number_of_stocks = int(row.get('number_of_stocks', 0))
                existing.market_value = float(row.get('market_cap_bil', 0))
                existing.ytd_change_percent = float(row.get('ytd_change', 0))
                existing.rs_score = float(row.get('ibd_score', 0))
                existing.rank = int(row.get('rank', 0))
                existing.letter_grade = str(row.get('letter_grade', 'E'))
                
                # Historical ranks
                existing.rank_1_week_ago = self._safe_int(row.get('rank_1_week_ago'))
                existing.rank_3_months_ago = self._safe_int(row.get('rank_3_months_ago'))
                existing.rank_6_months_ago = self._safe_int(row.get('rank_6_months_ago'))
                
                # Changes
                existing.change_vs_last_week = self._safe_int(row.get('change_vs_last_week'))
                existing.change_vs_3m_ago = self._safe_int(row.get('change_vs_3m_ago'))
                existing.change_vs_6m_ago = self._safe_int(row.get('change_vs_6m_ago'))
                
                saved_count += 1
                
            except Exception as e:
                logger.error(f"❌ Error saving group {row.get('industry_group', 'unknown')}: {e}")
                error_count += 1
                continue
        
        try:
            self.db.commit()
            logger.info(f"✅ Successfully saved {saved_count} groups")
            if error_count > 0:
                logger.warning(f"⚠️ Failed to save {error_count} groups")
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Database commit failed: {e}")
    
    def _safe_int(self, value):
        """Safely convert to int"""
        try:
            if pd.isna(value) or value is None:
                return None
            return int(float(value))
        except (ValueError, TypeError):
            return None

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Calculate IBD Industry Group Rankings (Corrected Version)'
    )
    
    parser.add_argument("--date", type=str, help="Target date in YYYY-MM-DD format")
    parser.add_argument("--backfill", action="store_true", help="Backfill data for past dates")
    parser.add_argument("--clean", action="store_true", help="Delete all existing data before running")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔍 Verbose logging enabled")
    
    db = SessionLocal()
    
    # Clean if requested
    if args.clean:
        logger.warning("⚠️ Cleaning industry_group_history table...")
        try:
            deleted_count = db.query(IndustryGroupHistory).delete()
            db.commit()
            logger.info(f"✅ Table cleaned. Deleted {deleted_count} records.")
        except Exception as e:
            logger.error(f"❌ Failed to clean table: {e}")
            db.rollback()
    
    calc = IndustryGroupCalculator(db)
    
    dates_to_process = []
    
    if args.date:
        # Specific date
        try:
            dates_to_process.append(datetime.strptime(args.date, "%Y-%m-%d").date())
        except ValueError:
            logger.error(f"❌ Invalid date format: {args.date}")
            db.close()
            return
    elif args.backfill:
        # Backfill with realistic dates
        logger.info("🔄 Generating backfill dates...")
        
        # Get actual trading dates
        try:
            query_trading_dates = text("""
                SELECT DISTINCT date 
                FROM prices 
                WHERE date <= CURRENT_DATE
                ORDER BY date DESC 
                LIMIT 252  -- Approximately 1 year of trading days
            """)
            
            with db.bind.connect() as connection:
                trading_dates = pd.read_sql(query_trading_dates, connection)
            
            if not trading_dates.empty:
                # Take weekly dates (every 5 trading days)
                all_dates = sorted(trading_dates['date'].tolist())
                dates_to_process = all_dates[::5]
                logger.info(f"📅 Found {len(dates_to_process)} dates for backfill")
            else:
                logger.warning("⚠️ No trading dates found, using weekly dates")
                today = date.today()
                dates_to_process = []
                for i in range(0, 180, 7):
                    dates_to_process.append(today - timedelta(days=i))
                dates_to_process.sort()
                
        except Exception as e:
            logger.error(f"❌ Error fetching trading dates: {e}")
            # Simple fallback
            today = date.today()
            dates_to_process = []
            for i in range(0, 180, 7):
                dates_to_process.append(today - timedelta(days=i))
            dates_to_process.sort()
    else:
        # Today only
        dates_to_process.append(date.today())
    
    # Sort and process
    dates_to_process.sort()
    
    logger.info(f"🚀 Starting processing of {len(dates_to_process)} dates...")
    
    success_count = 0
    fail_count = 0
    
    for idx, target_date in enumerate(dates_to_process, 1):
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 Processing {idx}/{len(dates_to_process)}: {target_date}")
            logger.info(f"{'='*60}")
            
            # 1. Calculate market cap weighted indices
            group_indices = calc.calculate_group_index_prices(target_date)
            
            if not group_indices:
                logger.warning(f"❌ No group data for {target_date}")
                fail_count += 1
                continue
            
            # 2. Calculate IBD scores with CORRECT formula
            group_df = calc.calculate_ibd_group_score(group_indices)
            
            if group_df.empty:
                logger.warning(f"❌ No scores for {target_date}")
                fail_count += 1
                continue
            
            # 3. Prepare summary with accurate YTD
            summary_df = calc.prepare_summary_data(group_df, target_date)
            
            if summary_df.empty:
                logger.warning(f"❌ No summary for {target_date}")
                fail_count += 1
                continue
            
            # 4. Save to database
            calc.save(summary_df, target_date)
            
            logger.info(f"✅ Processed {target_date} ({len(summary_df)} groups)")
            success_count += 1
            
            # Show sample output
            if idx == 1:  # First run only
                logger.info(f"\n📊 Sample output (first 5 groups):")
                print(summary_df.head().to_string())
            
        except Exception as e:
            logger.error(f"❌ Failed to process {target_date}: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("🎯 PROCESSING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"✅ Success: {success_count} dates")
    logger.info(f"❌ Failed: {fail_count} dates")
    logger.info(f"📊 Total: {len(dates_to_process)} dates")
    
    db.close()

if __name__ == "__main__":
    main()