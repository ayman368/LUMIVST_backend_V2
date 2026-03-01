from app.core.database import get_db
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy import func

db = next(get_db())

# Check all sections
print("=== All Sections in Database ===")
sections = db.query(
    FinancialMetricCategory.section,
    func.count(FinancialMetricCategory.id).label('count')
).group_by(FinancialMetricCategory.section).all()

for section, count in sections:
    print(f"{section}: {count}")

print("\n=== Metrics in 'cash_flow' section ===")
cash_flow_metrics = db.query(FinancialMetricCategory).filter(
    FinancialMetricCategory.section == 'cash_flow'
).all()
print(f"Total: {len(cash_flow_metrics)}")
if len(cash_flow_metrics) > 0:
    for metric in cash_flow_metrics[:5]:
        print(f"  - {metric.metric_name} ({metric.section})")

print("\n=== Sample metrics from 'other' section ===")
other_metrics = db.query(FinancialMetricCategory).filter(
    FinancialMetricCategory.section == 'other'
).limit(10).all()

for metric in other_metrics:
    label = metric.description_en or metric.metric_name
    print(f"  - {label[:60]}")
    print(f"    Key: {metric.metric_name}")
