#!/usr/bin/env python
"""Check financial metrics data"""

from app.core.database import engine
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy.orm import sessionmaker

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()

# Check data exists
metrics = db.query(CompanyFinancialMetric).filter_by(company_symbol='1010').all()
print(f"✅ Total metrics for 1010: {len(metrics)}")

if metrics:
    # Show first few metrics
    print("\n📊 First 5 metrics:")
    for m in metrics[:5]:
        # Try to get section from category
        category = db.query(FinancialMetricCategory).filter_by(metric_name=m.metric_name).first()
        section = category.section if category else 'N/A'
        print(f"  - {m.metric_name:30} | {section:20} | value: {m.metric_value}")
    
    # Check by period
    periods = db.query(CompanyFinancialMetric.period).filter_by(company_symbol='1010').distinct().all()
    print(f"\n📅 Periods available: {len(periods)}")
    for p in periods:
        print(f"  - {p[0]}")

# Check categories
categories = db.query(FinancialMetricCategory).all()
print(f"\n🏷️  Total categories: {len(categories)}")

sections = db.query(FinancialMetricCategory.section).distinct().all()
print(f"\n📂 Sections: {[s[0] for s in sections]}")

db.close()
