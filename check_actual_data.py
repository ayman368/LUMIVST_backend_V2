from app.core.database import get_db
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy import func

db = next(get_db())

# Get cash flow metrics
cash_flow_categories = db.query(FinancialMetricCategory).filter(
    FinancialMetricCategory.section == 'cash_flow'
).all()

print(f"=== Cash Flow Metrics in Categories ({len(cash_flow_categories)}) ===")
cash_flow_metric_names = [c.metric_name for c in cash_flow_categories]
for name in cash_flow_metric_names[:5]:
    print(f"  - {name}")

# Check if these metrics exist in CompanyFinancialMetric for symbol 1010
print(f"\n=== Checking CompanyFinancialMetric for symbol 1010 ===")

for metric_name in cash_flow_metric_names[:3]:
    count = db.query(CompanyFinancialMetric).filter(
        CompanyFinancialMetric.company_symbol == '1010',
        CompanyFinancialMetric.metric_name == metric_name
    ).count()
    print(f"{metric_name}: {count} records")

# Total metrics for symbol 1010 grouped by section
print(f"\n=== Total Metrics for Symbol 1010 by Section ===")
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

for section, count in result:
    print(f"  {section}: {count}")

print(f"\n=== All Metrics in 1010 without categorization ===")
all_1010 = db.query(CompanyFinancialMetric).filter(
    CompanyFinancialMetric.company_symbol == '1010'
).count()
print(f"Total: {all_1010}")
