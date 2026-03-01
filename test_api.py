#!/usr/bin/env python
"""Test API endpoints - Run this after starting the server"""

import requests
import json

API_URL = "http://localhost:8000"
SYMBOL = "1010"

print("\n🧪 TESTING API ENDPOINTS...\n")

# Test 1: data-by-section
print(f"1️⃣  Testing: {API_URL}/api/financial-metrics/{SYMBOL}/data-by-section")
try:
    res = requests.get(f"{API_URL}/api/financial-metrics/{SYMBOL}/data-by-section", timeout=10)
    print(f"   Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        periods = list(data.keys())[:3]
        print(f"   ✅ Data received! Periods: {periods}")
        print(f"   Total periods returned: {len(data)}")
    else:
        print(f"   ❌ Error: {res.text[:200]}")
except Exception as e:
    print(f"   ❌ Connection error: {e}")

# Test 2: metric-categories
print(f"\n2️⃣  Testing: {API_URL}/api/financial-metrics/metric-categories")
try:
    res = requests.get(f"{API_URL}/api/financial-metrics/metric-categories", timeout=10)
    print(f"   Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        print(f"   ✅ Categories retrieved! Count: {len(data)}")
        if data:
            print(f"   Sample category section: {data[0].get('section')}")
    else:
        print(f"   ❌ Error: {res.text[:200]}")
except Exception as e:
    print(f"   ❌ Connection error: {e}")

# Test 3: metrics-summary
print(f"\n3️⃣  Testing: {API_URL}/api/financial-metrics/{SYMBOL}/metrics-summary")
try:
    res = requests.get(f"{API_URL}/api/financial-metrics/{SYMBOL}/metrics-summary", timeout=10)
    print(f"   Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        print(f"   ✅ Summary received!")
        print(f"   Total metrics: {data.get('total_metrics')}")
        print(f"   By section: {data.get('metrics_by_section')}")
    else:
        print(f"   ❌ Error: {res.text[:200]}")
except Exception as e:
    print(f"   ❌ Connection error: {e}")

print("\n✅ API Tests Complete!")
print("💡 Make sure the server is running: uvicorn app.main:app --reload --port 8000")


db = SessionLocal()
print("[2] DB connection created", flush=True)

try:
    result = get_company_financial_data_by_section('1010', None, None, db)
    print(f"[3] API call succeeded: {type(result)}", flush=True)

    if result:
        periods = list(result.keys())
        print(f"[4] Found {len(periods)} periods", flush=True)
        for period in periods[:1]:
            sections = list(result[period].keys())
            metrics_count = sum(len(result[period][s]) for s in sections)
            print(f"[5] Period {period}: {sections} ({metrics_count} metrics)", flush=True)
    else:
        print("[X] No data returned!", flush=True)
except Exception as e:
    print(f"[ERROR] {e}", flush=True)
    import traceback
    traceback.print_exc()
finally:
    db.close()
    print("[DONE]", flush=True)
