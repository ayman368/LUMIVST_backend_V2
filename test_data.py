#!/usr/bin/env python
"""Test script to verify data and API"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import get_db
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory

print("\n🔍 CHECKING DATA IN DATABASE...\n")

db = next(get_db())

# Check metrics count
count = db.query(CompanyFinancialMetric).filter(
    CompanyFinancialMetric.company_symbol == '1010'
).count()
print(f"📊 Total metrics for symbol 1010: {count}")

# Check categories
cat_count = db.query(FinancialMetricCategory).count()
print(f"📂 Total metric categories: {cat_count}")

# Sample data
print(f"\n📄 Sample metrics (first 5):")
samples = db.query(CompanyFinancialMetric).filter(
    CompanyFinancialMetric.company_symbol == '1010'
).limit(5).all()

for s in samples:
    print(f"  ✓ {s.metric_name}: {s.metric_value} ({s.period}/{s.year})")

# Check if categories are linked
print(f"\n🔗 Checking metric categories...")
categories = db.query(FinancialMetricCategory).all()
sections = set()
for cat in categories:
    sections.add(cat.section)

print(f"📋 Sections found: {', '.join(sorted(sections))}")

db.close()
print("\n✅ Database check complete!")
