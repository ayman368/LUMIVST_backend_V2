"""
Deprecated alias — use backfill_screener_daily_trend.py instead.

  python scripts/backfill_screener_daily_trend.py
"""
import runpy
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
runpy.run_path(os.path.join(os.path.dirname(__file__), "backfill_screener_daily_trend.py"), run_name="__main__")
