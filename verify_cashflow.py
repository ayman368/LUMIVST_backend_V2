from app.core.database import get_db
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy import func

db = next(get_db())

# Check CompanyFinancialMetric records by section for symbol 1010
print("=== CompanyFinancialMetric Records for Symbol 1010 ===")

# Get metrics and join with categories
result = db.query(
    FinancialMetricCategory.section,
    func.count(CompanyFinancialMetric.id).label('count')
).join(
    FinancialMetricCategory,
    FinancialMetricCategory.metric_name == CompanyFinancialMetric.metric_name
).filter(
    CompanyFinancialMetric.company_symbol == '1010'
).group_by(
    FinancialMetricCategory.section
).all()

total = 0
for section, count in result:
    print(f"  {section}: {count}")
    total += count

print(f"Total: {total}")

# Also check how many cash_flow metrics exist in CompanyFinancialMetric without joining
print("\n=== Direct Check: Metrics in CompanyFinancialMetric that have cash_flow category ===")
cash_flow_metric_names = [m.metric_name for m in db.query(FinancialMetricCategory).filter(
    FinancialMetricCategory.section == 'cash_flow'
).all()]

found_count = db.query(CompanyFinancialMetric).filter(
    CompanyFinancialMetric.company_symbol == '1010',
    CompanyFinancialMetric.metric_name.in_(cash_flow_metric_names)
).count()

print(f"Cash flow metrics with data: {found_count} / {len(cash_flow_metric_names)}")
