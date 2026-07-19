#!/usr/bin/env python3
"""
Scraper for Aporia Analytics - per-stock chart data
https://www.aporiaanalytics.com/analyticschartdata

Each stock's popup chart has 5 sub-charts, requested one at a time via
POST with a different chart_type:
    trend, breakout, longest_consolidation_window, volume, price_extreme

This script loops over a list of tickers x the 5 chart types and saves
the RAW JSON response for each. We don't yet know the exact JSON shape
returned by this endpoint (it's likely different from /analyticsdata),
so this version only fetches + saves raw files — once you send me one
sample response, I'll add a proper parser that turns it into a clean
table/text file like we did for the main table.

Requirements:
    pip install requests beautifulsoup4

Usage:
    # test with just a couple of tickers first (recommended!)
    python scrape_aporia_charts.py --tickers 1050,2222 --limit-types trend

    # run for tickers pulled from a previously scraped table file
    python scrape_aporia_charts.py --tickers-file aporia_out/table_all_analytics.txt

    # full run (272 tickers x 5 chart types = 1360 requests -- slow, be polite)
    python scrape_aporia_charts.py --tickers-file aporia_out/table_all_analytics.txt --delay 0.4
"""

import argparse
import json
import os
import sys
import time

import requests
from bs4 import BeautifulSoup

# Ensure backend root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal
from app.models.aporia import AporiaChart

BASE = "https://www.aporiaanalytics.com"
PAGE_URL = f"{BASE}/saudianalytics"
CHART_DATA_URL = f"{BASE}/analyticschartdata"

HEADERS_COMMON = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) "
        "Gecko/20100101 Firefox/152.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

CHART_TYPES = [
    "trend",
    "breakout",
    "longest_consolidation_window",
    "volume",
    "price_extreme",
]


def get_session_and_token():
    sess = requests.Session()
    sess.headers.update(HEADERS_COMMON)

    resp = sess.get(PAGE_URL, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
    token = token_input["value"] if token_input else None
    if not token:
        token = sess.cookies.get("csrftoken")
    if not token:
        sys.exit("Could not find a CSRF token on the page.")

    return sess, token


def fetch_chart_data(sess, token, ticker, chart_type, market="saudi"):
    headers = {
        **HEADERS_COMMON,
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PAGE_URL,
        "Origin": BASE,
    }
    body = {
        "market": market,
        "chart_type": chart_type,
        "ticker": ticker,
        "csrfmiddlewaretoken": token,
    }
    resp = sess.post(CHART_DATA_URL, headers=headers, data=body, timeout=30)
    resp.raise_for_status()
    return resp


def tickers_from_table_file(path):
    tickers = []
    with open(path, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        idx = header.index("ticker")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) > idx and parts[idx]:
                tickers.append(parts[idx])
    return tickers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", help="Comma-separated ticker list, e.g. 1050,2222")
    ap.add_argument("--tickers-file", help="Path to a table_*.txt from the main scraper "
                                            "(reads the 'ticker' column)")
    ap.add_argument("--limit-types", help="Comma-separated subset of chart types to fetch "
                                           "(default: all 5)")
    ap.add_argument("--out-dir", default="aporia_charts", help="Output directory")
    ap.add_argument("--delay", type=float, default=0.3,
                     help="Seconds to sleep between requests (be polite to the server)")
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_file:
        tickers = tickers_from_table_file(args.tickers_file)
    else:
        sys.exit("Pass --tickers 1050,2222 or --tickers-file aporia_out/table_all_analytics.txt")

    chart_types = CHART_TYPES
    if args.limit_types:
        chart_types = [c.strip() for c in args.limit_types.split(",") if c.strip()]

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"{len(tickers)} tickers x {len(chart_types)} chart types "
          f"= {len(tickers) * len(chart_types)} requests")

    print("Loading page + CSRF token...")
    sess, token = get_session_and_token()
    print(f"Got token: {token[:12]}...")

    done, failed = 0, []
    total = len(tickers) * len(chart_types)
    
    db = SessionLocal()

    for ticker in tickers:
        for chart_type in chart_types:
            try:
                resp = fetch_chart_data(sess, token, ticker, chart_type)
                try:
                    payload = resp.json()
                    
                    try:
                        # Delete existing record for this ticker and chart_type
                        db.query(AporiaChart).filter(AporiaChart.ticker == ticker, AporiaChart.chart_type == chart_type).delete()
                        
                        db_record = AporiaChart(
                            ticker=ticker,
                            chart_type=chart_type,
                            chart_data=payload
                        )
                        db.add(db_record)
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        print(f"DB Error for {ticker} {chart_type}: {e}")
                        
                except ValueError:
                    out_path = os.path.join(args.out_dir, f"{ticker}_{chart_type}.txt")
                    with open(out_path, "w", encoding="utf-8") as f:
                        f.write(resp.text)
                done += 1
            except Exception as e:
                failed.append((ticker, chart_type, str(e)))
            time.sleep(args.delay)

        print(f"  {ticker} done ({done}/{total})")

    db.close()

    print(f"\nFinished: {done}/{total} succeeded, {len(failed)} failed")
    if failed:
        print("Failures:")
        for t, c, e in failed[:20]:
            print(f"  {t} / {c}: {e}")


if __name__ == "__main__":
    main()