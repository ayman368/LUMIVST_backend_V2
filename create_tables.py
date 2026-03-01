#!/usr/bin/env python
"""
Script to create all tables directly from SQLAlchemy models
"""
import os
import sys

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database import Base, engine
from app.models import (
    CompanyFinancialMetric,
    FinancialMetricCategory,
    CompanyMetricDisplaySetting,
)

if __name__ == "__main__":
    print("🔨 Creating all tables from SQLAlchemy models...")
    
    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)
        print("✅ All tables created successfully!")
        
        # List tables
        print("\n📊 Tables created:")
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name IN ('financial_metric_categories', 'company_metric_display_settings', 'company_financial_metrics')
                ORDER BY table_name
            """))
            rows = result.fetchall()
            if rows:
                for row in rows:
                    print(f"   ✓ {row[0]}")
                print("\n✅ All required tables exist!")
            else:
                print("   ⚠️ No tables found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
