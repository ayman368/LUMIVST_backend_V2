"""
NAAIM Exposure Index Scraper — v4

Strategy:
  1. First run (mode='full'):  Download full Excel → parse → upsert all.
  2. Weekly runs (mode='incremental'):  Scrape HTML → get latest weeks → upsert only new.
  3. Fallback: If HTML fails → download Excel → filter to new records only.

Excel column detection: Name-based first, positional fallback.
Retry logic on HTTP requests.
YoY only computed for new records in incremental mode.

Changes from v3:
  - Single HTTP session created once per scrape_naaim() call and passed down (no
    redundant connections).
  - _scrape_html_latest() now returns (records, soup) so _cache_page_metadata() can
    reuse the already-fetched page instead of making a second HTTP request.
  - All imports (redis, json, os, re, sqlalchemy.func) moved to module top level.
  - Silent bare `except: continue` in Excel row parser replaced with logged debug.
  - Unified Redis socket_timeout constant (REDIS_SOCKET_TIMEOUT).
  - Cache key constants centralised in CACHE_KEYS dict — single source of truth shared
    with the router via this module's public symbol.
"""

import io
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import redis as sync_redis
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from sqlalchemy import func
from urllib3.util.retry import Retry

from app.core.database import SessionLocal
from app.models.naaim_exposure import NaaimExposure

logger = logging.getLogger(__name__)

NAAIM_PAGE_URL = "https://naaim.org/programs/naaim-exposure-index/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Unified Redis timeout used by both scraper and router
REDIS_SOCKET_TIMEOUT = 5

# ── Cache key registry ─────────────────────────────────────────
# Import this dict in the router too so keys are never duplicated.
CACHE_KEYS = {
    "latest":        "naaim:latest",
    "chart_prefix":  "naaim:chart:",
    "history_prefix": "naaim:history:",
    "page_metadata": "naaim:page_metadata",
    "all_pattern":   "naaim:*",
}


# ──────────────────────────────────────────────────────────────
# HTTP Session with retries
# ──────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    """
    Create a requests.Session with automatic retry (3 attempts, exponential backoff).
    Call once per scrape run and pass it down — do not call per sub-function.
    """
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s


# ──────────────────────────────────────────────────────────────
# Redis helper
# ──────────────────────────────────────────────────────────────
def _get_redis() -> sync_redis.Redis:
    """Return a synchronous Redis client with the unified timeout."""
    from app.core.config import settings
    return sync_redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
    )


# ──────────────────────────────────────────────────────────────
# Excel URL Discovery
# ──────────────────────────────────────────────────────────────
def _discover_excel_url(session: requests.Session, soup: Optional[BeautifulSoup] = None) -> Optional[str]:
    """
    Find the current Excel download link from the NAAIM page.
    Accepts an optional pre-fetched BeautifulSoup object to avoid re-fetching the page.
    """
    try:
        if soup is None:
            resp = session.get(NAAIM_PAGE_URL, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".xlsx") and "inception" in href.lower():
                logger.info(f"📎 Excel URL: {href}")
                return href

        for link in soup.find_all("a", href=True):
            if link["href"].endswith(".xlsx"):
                logger.info(f"📎 Fallback Excel URL: {link['href']}")
                return link["href"]

        logger.warning("⚠️ No .xlsx link found")
        return None
    except Exception as e:
        logger.error(f"❌ Excel URL discovery failed: {e}")
        return None


# ──────────────────────────────────────────────────────────────
# Excel Parser — name-based column detection + positional fallback
# ──────────────────────────────────────────────────────────────
_NAME_MAP = {
    "date":          ["date"],
    "naaim_index":   ["mean/average", "mean / average", "naaim number"],
    "bearish":       ["most bearish response", "bearish"],
    "quartile_1":    ["quart 1"],
    "quartile_2":    ["quart 2", "median"],
    "quartile_3":    ["quart 3"],
    "bullish":       ["most bullish response", "bullish"],
    "std_deviation": ["standard deviation", "deviation", "std dev"],
    "sp500":         ["s&p 500", "s&p500", "sp500"],
}

# Positional fallback (stable since inception)
_POS_MAP = {
    0: "date", 1: "naaim_index", 2: "bearish", 3: "quartile_1",
    4: "quartile_2", 5: "quartile_3", 6: "bullish", 7: "std_deviation", 9: "sp500",
}


def _detect_columns(columns: list) -> Dict[str, str]:
    """
    Map our field names to actual Excel column names.
    Try name-based matching first; fill gaps with positional fallback.
    """
    col_map: Dict[str, str] = {}

    # 1) Name-based
    for field, keywords in _NAME_MAP.items():
        for col in columns:
            col_lower = str(col).strip().lower()
            for kw in keywords:
                if col_lower.startswith(kw) or kw in col_lower:
                    if field not in col_map:
                        col_map[field] = col
                    break

    # 2) Positional fallback for missing fields
    for pos, field in _POS_MAP.items():
        if field not in col_map and pos < len(columns):
            col_map[field] = columns[pos]
            logger.debug(f"  ↪ Positional fallback: {field} → col[{pos}] = '{columns[pos]}'")

    return col_map


def _parse_excel(content: bytes) -> List[Dict]:
    """Parse NAAIM Excel with hybrid column detection."""
    records = []
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=0)
        cols = list(df.columns)
        logger.info(f"📊 Excel: {len(df)} rows, columns: {cols}")

        col_map = _detect_columns(cols)
        logger.info(f"📊 Column mapping: {col_map}")

        if "date" not in col_map or "naaim_index" not in col_map:
            logger.error("❌ Missing required columns (date/naaim_index)")
            return []

        seen_dates: set = set()

        for idx, row in df.iterrows():
            try:
                # Date — ensure it's always a Python date, not pd.Timestamp
                raw = row[col_map["date"]]
                if pd.isna(raw):
                    continue
                if isinstance(raw, (pd.Timestamp, datetime)):
                    dt = raw.date()
                elif isinstance(raw, date):
                    dt = raw
                else:
                    dt = pd.to_datetime(str(raw)).date()

                # Dedup
                if dt in seen_dates:
                    continue
                seen_dates.add(dt)

                # NAAIM Index
                nv = row[col_map["naaim_index"]]
                if pd.isna(nv):
                    continue

                record: Dict = {"date": dt, "naaim_index": float(nv)}

                # Optional fields
                for field in ["sp500", "bearish", "quartile_1", "quartile_2",
                               "quartile_3", "bullish", "std_deviation"]:
                    if field in col_map:
                        v = row[col_map[field]]
                        if pd.notna(v):
                            val = str(v).replace(",", "").strip() if isinstance(v, str) else v
                            record[field] = float(val)

                records.append(record)

            except Exception as e:
                # Log so parse failures are visible during debugging; don't swallow silently.
                logger.warning(f"  ↪ Skipping row {idx}: {e}")
                continue

    except Exception as e:
        logger.error(f"❌ Excel parse error: {e}")

    logger.info(f"📊 Parsed {len(records)} unique records from Excel")
    return records


# ──────────────────────────────────────────────────────────────
# HTML Scraper
# ──────────────────────────────────────────────────────────────
def _scrape_html_latest(
    session: requests.Session,
) -> Tuple[List[Dict], Optional[BeautifulSoup]]:
    """
    Scrape the NAAIM page HTML tables for latest data.

    Returns (records, soup) so callers can reuse the fetched page
    (e.g. for metadata caching) without an extra HTTP request.
    """
    records = []
    soup: Optional[BeautifulSoup] = None
    try:
        resp = session.get(NAAIM_PAGE_URL, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        tables = soup.find_all("table")
        logger.info(f"🔍 Found {len(tables)} tables on page")

        naaim_data: Dict[str, float] = {}
        sp500_data: Dict[str, float] = {}
        detail_data: Dict[str, Dict] = {}

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            header_cells = rows[0].find_all(["th", "td"])
            num_cols = len(header_cells)
            header_text = " ".join(c.get_text(strip=True) for c in header_cells).lower()

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                dt = _parse_date(cells[0].get_text(strip=True))
                if not dt:
                    continue
                key = dt.isoformat()

                try:
                    if num_cols >= 7 and "bearish" in header_text:
                        if len(cells) >= 8:
                            detail_data[key] = {
                                "naaim_index": _safe_float(cells[1]),
                                "bearish": _safe_float(cells[2]),
                                "quartile_1": _safe_float(cells[3]),
                                "quartile_2": _safe_float(cells[4]),
                                "quartile_3": _safe_float(cells[5]),
                                "bullish": _safe_float(cells[6]),
                                "std_deviation": _safe_float(cells[7]),
                            }
                    elif "naaim" in header_text:
                        naaim_data[key] = _safe_float(cells[1])
                    elif "s&p" in header_text or "sp" in header_text:
                        sp500_data[key] = _safe_float(cells[1])
                except (ValueError, IndexError):
                    continue

        # Merge
        all_dates = set(naaim_data.keys()) | set(sp500_data.keys()) | set(detail_data.keys())
        for key in sorted(all_dates):
            dt = date.fromisoformat(key)
            record: Dict = {"date": dt}
            if key in naaim_data:
                record["naaim_index"] = naaim_data[key]
            if key in sp500_data:
                record["sp500"] = sp500_data[key]
            if key in detail_data:
                record.update(detail_data[key])
            if "naaim_index" in record and record["naaim_index"] is not None:
                records.append(record)

    except Exception as e:
        logger.error(f"❌ HTML scraping error: {e}")

    logger.info(f"🔍 Scraped {len(records)} records from HTML")
    return records, soup


# ──────────────────────────────────────────────────────────────
# Date / float helpers
# ──────────────────────────────────────────────────────────────
def _parse_date(s: str) -> Optional[date]:
    for fmt in ("%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(cell) -> Optional[float]:
    try:
        text = cell.get_text(strip=True).replace(",", "")
        return float(text) if text else None
    except (ValueError, AttributeError):
        return None


# ──────────────────────────────────────────────────────────────
# YoY calculation
# ──────────────────────────────────────────────────────────────
def _calculate_yoy(
    records: List[Dict],
    lookup: Optional[Dict[date, float]] = None,
) -> List[Dict]:
    """
    Compute YoY % for records using lookup for historical reference.
    If lookup is None, build it from records themselves (full-history mode).
    Uses fuzzy ±7 day matching to handle misaligned survey weeks.
    """
    if lookup is None:
        lookup = {r["date"]: r["naaim_index"] for r in records}

    for record in records:
        dt = record["date"]
        try:
            target = date(dt.year - 1, dt.month, dt.day)
        except ValueError:
            target = date(dt.year - 1, dt.month, dt.day - 1)

        # Exact match first
        if target in lookup and lookup[target] != 0:
            record["yoy_pct"] = round(
                ((record["naaim_index"] - lookup[target]) / abs(lookup[target])) * 100, 2
            )
            continue

        # Fuzzy ±7 days
        best, best_diff = None, timedelta(days=8)
        for delta in range(-7, 8):
            c = target + timedelta(days=delta)
            if c in lookup and abs(c - target) < best_diff:
                best_diff = abs(c - target)
                best = c

        if best and lookup[best] != 0:
            record["yoy_pct"] = round(
                ((record["naaim_index"] - lookup[best]) / abs(lookup[best])) * 100, 2
            )

    return records


# ──────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────
def _get_last_db_date() -> Optional[date]:
    db = SessionLocal()
    try:
        return db.query(func.max(NaaimExposure.date)).scalar()
    finally:
        db.close()


def _get_historical_lookup(since: date) -> Dict[date, float]:
    """Load historical naaim_index values from DB for YoY reference."""
    db = SessionLocal()
    try:
        rows = (
            db.query(NaaimExposure.date, NaaimExposure.naaim_index)
            .filter(NaaimExposure.date >= since)
            .all()
        )
        return {r.date: r.naaim_index for r in rows}
    finally:
        db.close()


def _upsert_records(records: List[Dict]) -> Tuple[int, int]:
    db = SessionLocal()
    inserted = updated = 0
    try:
        existing_map = {row.date: row for row in db.query(NaaimExposure).all()}
        for record in records:
            dt = record["date"]
            if dt in existing_map:
                obj = existing_map[dt]
                changed = False
                for k, v in record.items():
                    if k == "date":
                        continue
                    if v is not None and getattr(obj, k, None) != v:
                        setattr(obj, k, v)
                        changed = True
                if changed:
                    updated += 1
            else:
                db.add(NaaimExposure(**record))
                inserted += 1
        db.commit()
        logger.info(f"✅ Upsert: {inserted} inserted, {updated} updated")
    except Exception as e:
        db.rollback()
        logger.error(f"❌ DB error: {e}")
        raise
    finally:
        db.close()
    return inserted, updated


# ──────────────────────────────────────────────────────────────
# Page metadata cache
# ──────────────────────────────────────────────────────────────
def _cache_page_metadata(soup: Optional[BeautifulSoup] = None, session: Optional[requests.Session] = None) -> None:
    """
    Extract Last Quarter Avg + Posted On from the NAAIM page and store in Redis.

    Accepts a pre-fetched soup to avoid a redundant HTTP request.
    Falls back to fetching the page itself only when soup is not provided.
    """
    try:
        if soup is None:
            if session is None:
                session = _build_session()
            resp = session.get(NAAIM_PAGE_URL, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

        text = soup.get_text()
        metadata: Dict = {}

        match = re.search(r'Last\s*Quarter\s*Average\s*\(Q\d\)\s*(\d+\.?\d*)', text)
        if match:
            metadata["last_quarter_avg"] = float(match.group(1))
            logger.info(f"📊 Cached Last Quarter Avg: {metadata['last_quarter_avg']}")

        match = re.search(r'\*?\s*Posted\s+on\s+(\w+,\s+\w+\s+\d+,\s+\d{4})', text)
        if match:
            metadata["posted_on"] = match.group(1)
            logger.info(f"📊 Cached Posted On: {metadata['posted_on']}")

        # Also extract current quarter label (e.g. "Q2") for the frontend
        q_match = re.search(r'Last\s*Quarter\s*Average\s*\((Q\d)\)', text)
        if q_match:
            metadata["last_quarter_label"] = q_match.group(1)

        if metadata:
            r = _get_redis()
            try:
                r.set(CACHE_KEYS["page_metadata"], json.dumps(metadata), ex=604800)  # 7 days TTL
                # Invalidate the API-level cache so the next /latest call
                # rebuilds its response using this fresh metadata instead
                # of serving a stale fallback-computed value.
                r.delete(CACHE_KEYS["latest"])
                logger.info("✅ Page metadata cached in Redis (API cache invalidated)")
            finally:
                r.close()

    except Exception as e:
        logger.warning(f"⚠️ Failed to cache page metadata: {e}")


# ──────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────
def scrape_naaim(mode: str = "auto") -> Dict:
    """
    Scrape NAAIM data and upsert into the database.

    Modes:
      'auto'        — DB empty → full; otherwise → incremental.
      'full'        — Download full Excel history.
      'incremental' — HTML scrape for recent data only.
    """
    logger.info("=" * 50)
    logger.info(f"📊 [NAAIM] Starting (mode={mode})...")

    last_date = _get_last_db_date()
    logger.info(f"📊 [NAAIM] Last DB date: {last_date or 'EMPTY'}")

    if mode == "auto":
        mode = "full" if last_date is None else "incremental"
        logger.info(f"📊 [NAAIM] Auto → {mode}")

    # Single session for the entire scrape run
    session = _build_session()
    records: List[Dict] = []
    page_soup: Optional[BeautifulSoup] = None  # reused for metadata caching

    if mode == "full":
        # ── Full Excel ──
        excel_url = _discover_excel_url(session)
        if excel_url:
            try:
                logger.info("⬇️ Downloading Excel...")
                resp = session.get(excel_url, timeout=30)
                resp.raise_for_status()
                records = _parse_excel(resp.content)
            except Exception as e:
                logger.warning(f"⚠️ Excel failed: {e}")

        if not records:
            logger.info("🔄 Trying HTML fallback...")
            records, page_soup = _scrape_html_latest(session)

        if not records:
            return {"status": "error", "message": "No data", "inserted": 0, "updated": 0}

        records.sort(key=lambda r: r["date"])
        records = _calculate_yoy(records)

    elif mode == "incremental":
        # ── HTML for recent weeks (reuse soup for metadata) ──
        records, page_soup = _scrape_html_latest(session)

        # Filter to only new records
        if last_date:
            before = len(records)
            records = [r for r in records if r["date"] > last_date]
            logger.info(f"📊 Filtered: {before} → {len(records)} new records")

        # Fallback to Excel if HTML got nothing new
        if not records:
            logger.info("🔄 No new HTML data, trying Excel...")
            excel_url = _discover_excel_url(session, soup=page_soup)
            if excel_url:
                try:
                    resp = session.get(excel_url, timeout=30)
                    resp.raise_for_status()
                    all_recs = _parse_excel(resp.content)
                    records = [r for r in all_recs if r["date"] > last_date] if last_date else all_recs
                except Exception as e:
                    logger.warning(f"⚠️ Excel fallback failed: {e}")

        if not records:
            logger.info("📊 [NAAIM] No new data.")
            _cache_page_metadata(soup=page_soup, session=session)
            return {"status": "no_new_data", "inserted": 0, "updated": 0}

        records.sort(key=lambda r: r["date"])

        # YoY: use DB historical lookup (~14 months back)
        earliest_new = records[0]["date"]
        lookup_since = date(earliest_new.year - 1, max(1, earliest_new.month - 1), 1)
        historical = _get_historical_lookup(lookup_since)
        for r in records:
            historical[r["date"]] = r["naaim_index"]
        records = _calculate_yoy(records, lookup=historical)

    # Upsert into DB
    inserted, updated = _upsert_records(records)

    # Cache page metadata — reuse already-fetched soup (zero extra HTTP requests)
    _cache_page_metadata(soup=page_soup, session=session)

    result = {
        "status": "success",
        "mode": mode,
        "total_parsed": len(records),
        "inserted": inserted,
        "updated": updated,
        "date_range": f"{records[0]['date']} to {records[-1]['date']}",
    }
    logger.info(f"📊 [NAAIM] Done: {result}")
    return result


# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = sys.argv[1] if len(sys.argv) > 1 else "auto"
    result = scrape_naaim(mode=m)
    print(result)