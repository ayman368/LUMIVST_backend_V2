import requests

response = requests.get('http://localhost:8000/api/financial-metrics/1010/data-by-section')
data = response.json()

print("=== Data Structure ===")
for period, sections in data.items():
    print(f"\n{period}:")
    total_metrics = 0
    for section, metrics in sections.items():
        count = len(metrics) if isinstance(metrics, list) else 0
        print(f"  {section}: {count} metrics")
        total_metrics += count
    print(f"  Total: {total_metrics}")

# Specifically check for cash_flow
print("\n=== Cash Flow Section ===")
for period, sections in data.items():
    if 'cash_flow' in sections:
        print(f"{period}: {len(sections['cash_flow'])} cash_flow metrics")
        for metric in sections['cash_flow'][:3]:
            print(f"  - {metric['label']}")
    else:
        print(f"{period}: NO cash_flow section")

# Check 2024 Annual specifically
print("\n=== 2024 Annual Details ===")
if '2024 Annual' in data:
    annual = data['2024 Annual']
    print(f"Sections available: {list(annual.keys())}")
else:
    print("2024 Annual not found, available periods:")
    for period in data.keys():
        print(f"  - {period}")
