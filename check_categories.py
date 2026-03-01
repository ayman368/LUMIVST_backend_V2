#!/usr/bin/env python
"""Check metric categories linking"""

from app.core.database import engine
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy.orm import sessionmaker

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()

# Count metrics with and without categories
total_metrics = db.query(CompanyFinancialMetric).filter_by(company_symbol='1010').count()

# Check metrics that have corresponding category
from sqlalchemy import and_

metrics_with_category = db.query(CompanyFinancialMetric).join(
    FinancialMetricCategory,
    CompanyFinancialMetric.metric_name == FinancialMetricCategory.metric_name,
    isouter=True
).filter(
    and_(
        CompanyFinancialMetric.company_symbol == '1010',
        FinancialMetricCategory.metric_name.isnot(None)
    )
).count()

print(f"📊 Metrics Analysis:")
print(f"  Total metrics: {total_metrics}")
print(f"  Metrics with category: {metrics_with_category}")
print(f"  Without category: {total_metrics - metrics_with_category}")

# Get unique metric names
unique_names = db.query(CompanyFinancialMetric.metric_name).filter_by(company_symbol='1010').distinct().count()
print(f"\n🏷️  Unique metric names: {unique_names}")

# Check which ones don't have categories
missing_categories = db.query(CompanyFinancialMetric.metric_name).filter_by(
    company_symbol='1010'
).distinct().all()

categories = db.query(FinancialMetricCategory.metric_name).all()
category_names = set([c[0] for c in categories])
metric_names = set([m[0] for m in missing_categories])

missing = metric_names - category_names
print(f"\n⚠️  Metric names without category: {len(missing)}")
if missing:
    for name in list(missing)[:5]:
        print(f"  - {name}")
    if len(missing) > 5:
        print(f"  ... and {len(missing) - 5} more")

db.close()
