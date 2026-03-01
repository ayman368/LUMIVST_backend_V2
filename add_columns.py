#!/usr/bin/env python
"""Add missing columns to company_financial_metrics table"""

from sqlalchemy import text
from app.core.database import engine

print("🔧 Adding missing columns to company_financial_metrics...")

queries = [
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS label_ar VARCHAR(500)",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS data_quality_score FLOAT DEFAULT 1.0",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS is_verified BOOLEAN DEFAULT false",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS verification_date TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS source_date TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
    "ALTER TABLE company_financial_metrics ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
]

try:
    with engine.connect() as conn:
        for query in queries:
            try:
                conn.execute(text(query))
                print(f"  ✓ {query.split('ADD COLUMN IF NOT EXISTS')[1].split(' ')[0].strip()}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"  ℹ Column already exists")
                else:
                    print(f"  ✓ Added")
        conn.commit()
    
    print("\n✅ All columns added successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
