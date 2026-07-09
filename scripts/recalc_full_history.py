import sys
import os
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, date
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal
from app.models.stock_indicators import StockIndicator
from scripts.indicators_data_service import IndicatorsDataService

def recalculate_full_history_for_symbol(db: Session, symbol: str):
    print(f"📊 Fetching data for {symbol}...")
    
    query = text("""
        SELECT date, open, high, low, close
        FROM prices
        WHERE symbol = :symbol
        ORDER BY date ASC
    """)
    result = db.execute(query, {"symbol": symbol})
    rows = result.fetchall()
    
    rows_dicts = [dict(r._mapping) for r in rows]
    
    if not rows_dicts or len(rows_dicts) < 100:
        print(f"⚠️  {symbol}: Not enough data")
        return
        
    df = IndicatorsDataService.prepare_price_dataframe(rows_dicts)
    if df is None: return
    
    df_weekly = IndicatorsDataService.prepare_weekly_dataframe(df)
    if df_weekly is None: return
    
    print(f"🔄 Calculating all indicators for {len(df)} dates...")
    
    indicators_to_insert = []
    
    # We start from index 100 to ensure moving averages have enough data
    for idx in range(100, len(df)):
        target_date = df.index[idx].date()
        
        # Find closest weekly index
        w_idx = None
        for i in range(len(df_weekly) - 1, -1, -1):
            if df_weekly.index[i].date() <= target_date:
                w_idx = i
                break
                
        data = IndicatorsDataService.calculate_all_indicators(
            df=df,
            df_weekly=df_weekly,
            symbol=symbol,
            target_date=target_date,
            idx=idx,
            w_idx=w_idx
        )
        
        indicator_data = {
            'symbol': symbol,
            'date': target_date,
            **data
        }
        
        # Clean data for DB
        for k, v in list(indicator_data.items()):
            if k in ['price_history', 'weekly_history']:
                del indicator_data[k]
                continue
            if isinstance(v, (np.float64, np.float32, np.integer)):
                indicator_data[k] = round(float(v), 2) if not pd.isna(v) else None
            elif isinstance(v, np.bool_):
                indicator_data[k] = bool(v)
            elif isinstance(v, float):
                indicator_data[k] = round(v, 2) if not pd.isna(v) else None
            elif isinstance(v, (list, dict, np.ndarray, pd.Series)):
                indicator_data[k] = None
                
        indicators_to_insert.append(indicator_data)
        
        if len(indicators_to_insert) >= 200:
            stmt = insert(StockIndicator).values(indicators_to_insert)
            update_cols = {k: getattr(stmt.excluded, k) for k in indicators_to_insert[0].keys() if k not in ['symbol', 'date']}
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'date'],
                set_=update_cols
            )
            db.execute(stmt)
            indicators_to_insert = []
            print(f"   💾 Saved up to {target_date}...")
            
    if indicators_to_insert:
        stmt = insert(StockIndicator).values(indicators_to_insert)
        update_cols = {k: getattr(stmt.excluded, k) for k in indicators_to_insert[0].keys() if k not in ['symbol', 'date']}
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'date'],
            set_=update_cols
        )
        db.execute(stmt)
        print(f"   💾 Saved final batch up to {indicators_to_insert[-1]['date']}...")
        
    db.commit()
    print(f"✅ {symbol} full history calculation and save complete!")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        recalculate_full_history_for_symbol(db, '1150')
        recalculate_full_history_for_symbol(db, '1120')
        
        # Clear redis cache
        import asyncio
        from app.core.redis import redis_cache
        asyncio.run(redis_cache.flush_all())
        print("✅ Redis cache cleared!")
    finally:
        db.close()
