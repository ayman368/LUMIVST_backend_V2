import requests

response = requests.get('http://localhost:8000/api/financial-metrics/1010/data-by-section')
data = response.json()
other_metrics = data['2024 Annual']['other']

print("=== Looking for metrics that should be in 'cash_flow' ===\n")

keywords_to_check = ['cash', 'flow', 'operating', 'investing', 'financing', 'depreciation', 'amortization']

found = {}
for keyword in keywords_to_check:
    found[keyword] = []
    for metric in other_metrics:
        label = metric['label'].lower()
        key = metric['key'].lower()
        if keyword in label or keyword in key:
            found[keyword].append({
                'label': metric['label'],
                'key': metric['key']
            })

for keyword, metrics in sorted(found.items()):
    if metrics:
        print(f"\n[{keyword.upper()}] - Found {len(metrics)} metrics:")
        for m in metrics[:5]:  # Show first 5
            print(f"  - {m['label']}")
            print(f"    ({m['key']})")
        if len(metrics) > 5:
            print(f"  ... and {len(metrics)-5} more")
