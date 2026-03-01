from app.core.database import get_db
from app.models.financial_metric_categories import FinancialMetricCategory
from sqlalchemy import func

db = next(get_db())

# Get all unique sections with count
sections = db.query(
    FinancialMetricCategory.section,
    func.count(FinancialMetricCategory.id).label('count')
).group_by(FinancialMetricCategory.section).all()

print('=== Main Sections ===')
for section, count in sections:
    print(f'{section}: {count} metrics')

print('\n=== All Subsections ===')
subsections = db.query(
    FinancialMetricCategory.section,
    FinancialMetricCategory.subsection,
    func.count(FinancialMetricCategory.id).label('count')
).group_by(FinancialMetricCategory.section, FinancialMetricCategory.subsection).order_by(FinancialMetricCategory.section).all()

for section, subsection, count in subsections:
    print(f'{section} > {subsection}: {count}')

# Get sample metrics from each section
print('\n=== Sample Metrics per Section ===')
for section, _ in sections:
    samples = db.query(FinancialMetricCategory.metric_name).filter(
        FinancialMetricCategory.section == section
    ).limit(3).all()
    print(f'\n{section}:')
    for (metric,) in samples:
        print(f'  - {metric}')
