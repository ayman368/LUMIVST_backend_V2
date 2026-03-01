import requests
import json

# Get data for symbol 4322
try:
    response = requests.get('http://localhost:8000/api/financial-metrics/4322/data-by-section')
    data = response.json()
    
    print("=== Available Periods ===")
    for period in sorted(data.keys()):
        print(f"  - {period}")
    
    # Check 2024 Annual
    if '2024 Annual' in data:
        annual_data = data['2024 Annual']
        print("\n=== Sections in 2024 Annual ===")
        total_metrics = 0
        for section in sorted(annual_data.keys()):
            metrics_count = len(annual_data[section])
            print(f"  - {section}: {metrics_count} metrics")
            total_metrics += metrics_count
        print(f"\nTotal: {total_metrics} metrics")
    else:
        print("\n2024 Annual NOT FOUND")
        print(f"Available periods: {list(data.keys())}")
        
except Exception as e:
    print(f"Error: {e}")
