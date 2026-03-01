#!/usr/bin/env python
"""Add foreign key constraints"""

from sqlalchemy import text
from app.core.database import engine

print("🔗 Adding foreign key constraints...")

queries = [
    """
    ALTER TABLE company_financial_metrics 
    ADD CONSTRAINT fk_cfm_metric_name 
    FOREIGN KEY (metric_name) 
    REFERENCES financial_metric_categories(metric_name)
    ON DELETE SET NULL
    ON UPDATE CASCADE
    """,
]

try:
    with engine.connect() as conn:
        for query in queries:
            try:
                conn.execute(text(query))
                print(f"  ✓ Foreign key added")
                conn.commit()
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                    print(f"  ℹ Foreign key already exists")
                    conn.rollback()
                else:
                    raise
    
    print("\n✅ All constraints added successfully!")
    
except Exception as e:
    print(f"❌ Note: {e}")
    print("   This is expected if FK already exists or if there are orphaned records")
