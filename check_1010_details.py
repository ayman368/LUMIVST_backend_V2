import requests
import json

# Get data for symbol 1010
try:
    response = requests.get('http://localhost:8000/api/financial-metrics/1010/data-by-section')
    data = response.json()

    annual_data = data['2024 Annual']

    print("=== Other Section Metrics (First 20) ===")
    other_metrics = annual_data['other']
    for i, metric in enumerate(other_metrics[:20], 1):
        print(f"{i}. {metric['label']} ({metric['key']})")

    print(f"\n... and {len(other_metrics) - 20} more metrics")

    print("\n=== Checking for Cash Flow related metrics ===")
    cash_flow_count = 0
    for metric in other_metrics:
        if 'cash' in metric['label'].lower() or 'cash' in metric['key'].lower():
            print(f"  ✓ {metric['label']} ({metric['key']})")
            cash_flow_count += 1

    print(f"\nTotal Cash Flow metrics found: {cash_flow_count}")

    # Check visibility settings
    print("\n=== Checking visibility settings ===")
    try:
        settings_response = requests.get('http://localhost:8000/api/financial-metrics/metric-settings/1010')
        if settings_response.ok:
            settings = settings_response.json()
            visible_count = sum(1 for s in settings if s['is_visible'])
            hidden_count = sum(1 for s in settings if not s['is_visible'])
            print(f"Visible: {visible_count}")
            print(f"Hidden: {hidden_count}")
        else:
            print(f"Settings not found (status: {settings_response.status_code})")
    except Exception as e:
        print(f"No visibility settings yet (will be created on first toggle): {e}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
