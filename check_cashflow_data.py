from app.core.database import get_db
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy import func

db = next(get_db())

# Get cash flow metrics that have actual data for symbol 1010
print("=== Cash Flow Metrics with Data for Symbol 1010 ===")

cash_flow_metrics = db.query(FinancialMetricCategory).filter(
    FinancialMetricCategory.section == 'cash_flow'
).all()

cash_flow_names = [m.metric_name for m in cash_flow_metrics]

# Check which ones have data
for metric_name in cash_flow_names:
    count = db.query(CompanyFinancialMetric).filter(
        CompanyFinancialMetric.company_symbol == '1010',
        CompanyFinancialMetric.metric_name == metric_name
    ).count()
    if count > 0:
        # Get sample
        sample = db.query(CompanyFinancialMetric).filter(
            CompanyFinancialMetric.company_symbol == '1010',
            CompanyFinancialMetric.metric_name == metric_name
        ).first()
        print(f"✓ {metric_name}: {count} records")
        print(f"  Sample: {sample.year} {sample.period} = {sample.metric_value}")
