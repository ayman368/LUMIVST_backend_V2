#!/usr/bin/env python3
"""
Add price_vs_ema_percent columns to stock_indicators table
"""
import sys
import os
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal

def add_ema_columns():
    """Add missing price_vs_ema_percent columns to database"""
    db = SessionLocal()
    
    try:
        print("Adding price_vs_ema_percent columns to stock_indicators table...")
        
        # SQL to add the columns if they don't exist
        sql = """
        DO $$
        BEGIN
            -- Add price_vs_ema_10_percent if not exists
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='stock_indicators' AND column_name='price_vs_ema_10_percent') THEN
                ALTER TABLE stock_indicators 
                ADD COLUMN price_vs_ema_10_percent NUMERIC(14, 4) DEFAULT NULL;
                RAISE NOTICE 'Added price_vs_ema_10_percent column';
            END IF;
            
            -- Add price_vs_ema_21_percent if not exists
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_name='stock_indicators' AND column_name='price_vs_ema_21_percent') THEN
                ALTER TABLE stock_indicators 
                ADD COLUMN price_vs_ema_21_percent NUMERIC(14, 4) DEFAULT NULL;
                RAISE NOTICE 'Added price_vs_ema_21_percent column';
            END IF;
        END $$;
        """
        
        db.execute(text(sql))
        db.commit()
        print("✅ Columns added successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    add_ema_columns()
