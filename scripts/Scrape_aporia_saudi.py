#!/usr/bin/env python3
"""
Scraper for Aporia Analytics - Saudi Stock Analytics table
https://www.aporiaanalytics.com/saudianalytics

The /analyticsdata endpoint returns JSON like:
    {"htmlData": ["<tr id=...>...</tr><tr id=...>...</tr>..."], "lastUpdated": [...], "numStocks": "272"}
i.e. the ROWS are a single blob of raw HTML, not structured JSON records.
This script parses that HTML with BeautifulSoup and writes a clean,
tab-separated table (all columns, all rows) to a text file.

Requirements:
    pip install requests beautifulsoup4

Usage:
    python scrape_aporia_saudi.py
    -> produces: aporia_saudi_raw.json  and  aporia_saudi_table.txt
"""

import json
import sys
import os
import requests
from bs4 import BeautifulSoup

# Ensure backend root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import SessionLocal
from app.models.aporia import AporiaAnalytics

BASE = "https://www.aporiaanalytics.com"
PAGE_URL = f"{BASE}/saudianalytics"
DATA_URL = f"{BASE}/analyticsdata"

HEADERS_COMMON = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) "
        "Gecko/20100101 Firefox/152.0"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Column order as it appears in each <tr> (matches the site's own header row)
COLUMNS = [
    "ticker", "name", "sector", "market_cap", "val_avg_3mo", "trailingPE",
    "last", "mtd_rtn", "mo3_rtn", "year_rtn", "daily_trend", "weekly_trend",
    "monthly_trend", "trend_rank", "pfh_250", "days_since_high_250",
    "breakout", "longest_consolidation_window", "position", "price_extreme",
    "vol_5_day_chng", "vol_20_day_chng",
]

TREND_COLS = {"daily_trend", "weekly_trend", "monthly_trend"}


def get_session_and_token():
    """Load the page fresh, grab cookies + the current CSRF token."""
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


def fetch_data(sess, token, market="saudi", sort_by="none",
               sort_type=False, analytics_filter="all_metrics"):
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
        "sort_by": sort_by,
        "sort_type": "true" if sort_type else "false",
        "analytics_filter": analytics_filter,
        "csrfmiddlewaretoken": token,
    }
    resp = sess.post(DATA_URL, headers=headers, data=body, timeout=30)
    resp.raise_for_status()
    return resp


def parse_trend_cell(td):
    """
    Trend cells (daily/weekly/monthly) hold a colored div with an
    up/down icon and two small numbers (or a star icon meaning
    'still active / ongoing').
    Returns a compact string like 'up:41,*' / 'down:15,5' / 'flat'.
    """
    inner = td.find("div")
    if inner is None:
        return "-"
    style = inner.get("style", "")
    if "#e0e0e0" in style.lower() or "grey" in style.lower():
        return "flat"

    direction = "-"
    if td.find("i", class_="icofont-circled-up"):
        direction = "up"
    elif td.find("i", class_="icofont-circled-down"):
        direction = "down"

    # the two small number divs (font-size: 10px)
    number_divs = inner.find_all(
        "div", style=lambda s: s and "font-size: 10px" in s
    )
    values = []
    for d in number_divs:
        if d.find("i", class_="icofont-star"):
            values.append("*")
        else:
            text = d.get_text(strip=True)
            values.append(text if text else "")

    return f"{direction}:" + ",".join(v for v in values if v != "") if values else direction


def parse_position_cell(td):
    span = td.find("span", class_="range-percentage")
    if span:
        return span.get_text(strip=True)
    return td.get_text(strip=True) or "-"


def parse_consolidation_cell(td):
    inner = td.find("div")
    if inner:
        return inner.get_text(strip=True)
    return td.get_text(strip=True) or "-"


def parse_breakout_cell(td):
    inner = td.find("div")
    if inner:
        return inner.get_text(strip=True)
    return td.get_text(strip=True) or "-"


def parse_price_extreme_cell(td):
    icons = td.find_all("i", class_="icofont-warning")
    if not icons:
        return td.get_text(strip=True) or "-"
    colors = []
    for icon in icons:
        style = icon.get("style", "")
        if "green" in style:
            colors.append("green")
        elif "red" in style:
            colors.append("red")
        else:
            colors.append("?")
    return ",".join(colors)


def parse_row(tr):
    row = {}
    for col in COLUMNS:
        td = tr.find("td", class_=col)
        if td is None:
            row[col] = ""
            continue
        if col in TREND_COLS:
            row[col] = parse_trend_cell(td)
        elif col == "position":
            row[col] = parse_position_cell(td)
        elif col == "longest_consolidation_window":
            row[col] = parse_consolidation_cell(td)
        elif col == "breakout":
            row[col] = parse_breakout_cell(td)
        elif col == "price_extreme":
            row[col] = parse_price_extreme_cell(td)
        else:
            row[col] = td.get_text(strip=True)
    return row


def parse_html_blob(html_str):
    soup = BeautifulSoup(html_str, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        if not tr.has_attr("id"):
            continue  # skip spacer rows
        rows.append(parse_row(tr))
    return rows


def write_text_table(records, out_path):
    if not records:
        print("No rows parsed — check the raw JSON / HTML manually.")
        return
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for rec in records:
            f.write("\t".join(rec.get(c, "") for c in COLUMNS) + "\n")
    print(f"Wrote {len(records)} rows x {len(COLUMNS)} columns -> {out_path}")

def save_to_db(records, filter_value):
    if not records:
        return
    
    db = SessionLocal()
    try:
        # Delete old records for this filter
        db.query(AporiaAnalytics).filter(AporiaAnalytics.filter_category == filter_value).delete()
        
        # Insert new records
        db_records = []
        for rec in records:
            db_rec = AporiaAnalytics(
                filter_category=filter_value,
                ticker=rec.get("ticker", ""),
                name=rec.get("name", ""),
                sector=rec.get("sector", ""),
                market_cap=rec.get("market_cap", ""),
                val_avg_3mo=rec.get("val_avg_3mo", ""),
                trailingPE=rec.get("trailingPE", ""),
                last=rec.get("last", ""),
                mtd_rtn=rec.get("mtd_rtn", ""),
                mo3_rtn=rec.get("mo3_rtn", ""),
                year_rtn=rec.get("year_rtn", ""),
                daily_trend=rec.get("daily_trend", ""),
                weekly_trend=rec.get("weekly_trend", ""),
                monthly_trend=rec.get("monthly_trend", ""),
                trend_rank=rec.get("trend_rank", ""),
                pfh_250=rec.get("pfh_250", ""),
                days_since_high_250=rec.get("days_since_high_250", ""),
                breakout=rec.get("breakout", ""),
                longest_consolidation_window=rec.get("longest_consolidation_window", ""),
                position=rec.get("position", ""),
                price_extreme=rec.get("price_extreme", ""),
                vol_5_day_chng=rec.get("vol_5_day_chng", ""),
                vol_20_day_chng=rec.get("vol_20_day_chng", "")
            )
            db_records.append(db_rec)
        db.bulk_save_objects(db_records)
        db.commit()
        print(f"Saved {len(db_records)} to database for {filter_value}")
    except Exception as e:
        db.rollback()
        print(f"Failed to save to db: {e}")
    finally:
        db.close()


# Dropdown -> analytics_filter value.
# ALL 6 CONFIRMED from real captured requests.
FILTERS = {
    "all_analytics": "all_metrics",
    "largest_market_cap": "largest",
    "strongest_uptrends": "strongest_uptrends",
    "strongest_downtrends": "strongest_downtrends",
    "breakouts": "breakouts",
    "consolidations": "consolidations",
}


def run_one_filter(sess, token, label, filter_value, out_dir="aporia_out"):
    import os
    os.makedirs(out_dir, exist_ok=True)

    print(f"\n=== {label} (analytics_filter={filter_value}) ===")
    resp = fetch_data(sess, token, analytics_filter=filter_value)

    raw_path = os.path.join(out_dir, f"raw_{label}.json")
    try:
        payload = resp.json()
    except ValueError:
        raw_path = os.path.join(out_dir, f"raw_{label}.txt")
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        print(f"Response wasn't JSON — saved raw text -> {raw_path}")
        return

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    html_list = payload.get("htmlData", [])
    html_blob = "".join(html_list) if isinstance(html_list, list) else str(html_list)

    records = parse_html_blob(html_blob)
    table_path = os.path.join(out_dir, f"table_{label}.txt")
    write_text_table(records, table_path)
    save_to_db(records, filter_value)


def main():
    print("Loading page + CSRF token...")
    sess, token = get_session_and_token()
    print(f"Got token: {token[:12]}...")

    for label, filter_value in FILTERS.items():
        try:
            run_one_filter(sess, token, label, filter_value)
        except Exception as e:
            print(f"FAILED for {label} ({filter_value}): {e}")

    print("\nDone. Check the aporia_out/ folder for 6 sets of raw_*.json + table_*.txt")


if __name__ == "__main__":
    main()