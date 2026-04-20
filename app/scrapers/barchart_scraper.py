"""
Barchart SOFR Futures scraper
==============================
Strategies (in order):
  1. Barchart core-api with explicit symbol list (all active contracts)
  2. Barchart CSV download endpoint
  3. undetected-chromedriver + API with browser cookies

Install:
    pip install requests
    pip install undetected-chromedriver   # optional, strategy 3 only
"""

import io
import csv
import time
import logging
from datetime import date, datetime
from urllib.parse import unquote

import requests

from app.core.database import SessionLocal
from app.models.economic_indicators import SofrFutures

logger = logging.getLogger(__name__)

BASE_URL = "https://www.barchart.com"
PAGE_URL  = f"{BASE_URL}/futures/quotes/SR*0/futures-prices?orderBy=priceChange&orderDir=desc"
CSV_URL   = f"{BASE_URL}/futures/quotes/SR*0/futures-prices/download"

# Barchart futures month codes (standard)
_MONTH_CODES = "FGHJKMNQUVXZ"   # Jan–Dec

API_FIELDS = (
    "symbol,contractName,lastPrice,priceChange,"
    "openPrice,highPrice,lowPrice,previousPrice,"
    "volume,openInterest,tradeTime"
)

_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": PAGE_URL,
}

# ─────────────────────────────────────────────
# Symbol list builder
# ─────────────────────────────────────────────

def _build_symbols(years_ahead: int = 2) -> str:
    """
    Build a comma-separated list of all SOFR futures symbols
    for the current year through (current + years_ahead).

    Example result: SRF26,SRG26,...,SRZ27,SRF28,...,SRZ28
    """
    current_year = datetime.now().year
    syms = []
    for yr in range(current_year, current_year + years_ahead + 1):
        suffix = str(yr)[-2:]          # "26", "27", ...
        for code in _MONTH_CODES:
            syms.append(f"SR{code}{suffix}")
    return ",".join(syms)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _f(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s.upper() in ("N/A", "-", "", "NONE"):
        return None
    try:
        return float(s.rstrip("s").replace(",", "").lstrip("+"))
    except ValueError:
        return None


def _i(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s.upper() in ("N/A", "-", "", "NONE"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_BASE_HEADERS)
    return s


def _get_xsrf(session: requests.Session) -> str:
    """Load the page once to get session cookies; return decoded XSRF token."""
    logger.info("🌐 Fetching page to acquire session cookies…")
    r = session.get(PAGE_URL, timeout=25)
    r.raise_for_status()
    token = unquote(session.cookies.get("XSRF-TOKEN", ""))
    logger.info(f"🔑 XSRF token: {'ok' if token else 'MISSING'}")
    return token


# ─────────────────────────────────────────────
# Strategy 1 — internal JSON API (all symbols)
# ─────────────────────────────────────────────

def _via_api(today: date) -> list[dict] | None:
    session = _make_session()
    try:
        xsrf = _get_xsrf(session)

        symbols = _build_symbols(years_ahead=2)
        url = (
            f"{BASE_URL}/proxies/core-api/v1/quotes/get"
            f"?symbols={symbols}"
            f"&fields={API_FIELDS}"
            "&raw=1"
        )
        logger.info(f"📡 Calling core-api with {len(symbols.split(','))} symbols…")

        r = session.get(url, headers={
            "Accept": "application/json",
            "x-xsrf-token": xsrf,
        }, timeout=25)

        logger.info(f"API status: {r.status_code}")
        if r.status_code != 200:
            logger.warning(f"API {r.status_code}: {r.text[:300]}")
            return None

        data = r.json().get("data", [])
        if not data:
            logger.warning("API returned empty data array.")
            return None

        records = []
        for item in data:
            raw = item.get("raw", item)
            sym = str(raw.get("symbol", "")).strip()
            # Skip entries with no price data at all
            if not sym or _f(raw.get("lastPrice")) is None:
                continue
            rec = {
                "scrape_date":   today,
                "contract":      sym,
                "last_price":    _f(raw.get("lastPrice")),
                "change":        _f(raw.get("priceChange")),
                "open_price":    _f(raw.get("openPrice")),
                "high":          _f(raw.get("highPrice")),
                "low":           _f(raw.get("lowPrice")),
                "previous":      _f(raw.get("previousPrice")),
                "volume":        _i(raw.get("volume")),
                "open_interest": _i(raw.get("openInterest")),
                "updated_time":  str(raw.get("tradeTime", "")) or None,
            }
            records.append(rec)
            logger.info(f"  📌 {sym}: Last={rec['last_price']}, Chg={rec['change']}")

        logger.info(f"✅ API: {len(records)} contracts with price data.")
        return records if len(records) >= 3 else None  # sanity check

    except Exception as exc:
        logger.warning(f"API strategy error: {exc}")
        return None


# ─────────────────────────────────────────────
# Strategy 2 — CSV download
# ─────────────────────────────────────────────

def _via_csv(today: date) -> list[dict] | None:
    session = _make_session()
    try:
        xsrf = _get_xsrf(session)

        logger.info("📥 Trying CSV download…")
        r = session.get(CSV_URL, params={
            "orderBy": "priceChange",
            "orderDir": "desc",
            "startRow": 1,
            "type": "futures-prices",
        }, headers={
            "Accept": "text/csv,*/*",
            "x-xsrf-token": xsrf,
        }, timeout=25)

        logger.info(f"CSV status: {r.status_code}")
        if r.status_code != 200:
            logger.warning(f"CSV {r.status_code}: {r.text[:200]}")
            return None

        content = r.text.strip()
        if not content or len(content) < 50:
            logger.warning("CSV response too short / empty.")
            return None

        # Log the first line so we can see actual column names
        first_line = content.split("\n")[0]
        logger.info(f"CSV headers: {first_line}")

        reader = csv.DictReader(io.StringIO(content))
        records = []
        for row in reader:
            row = {k.strip(): v.strip() for k, v in row.items()}
            sym = row.get("Symbol", "").strip()
            if not sym or not sym.startswith("SR"):
                continue
            rec = {
                "scrape_date":   today,
                "contract":      sym,
                "last_price":    _f(row.get("Last") or row.get("Last Price")),
                "change":        _f(row.get("Change")),
                "open_price":    _f(row.get("Open")),
                "high":          _f(row.get("High")),
                "low":           _f(row.get("Low")),
                "previous":      _f(row.get("Previous")),
                "volume":        _i(row.get("Volume")),
                "open_interest": _i(row.get("Open Int") or row.get("Open Interest")),
                "updated_time":  row.get("Time") or None,
            }
            records.append(rec)
            logger.info(f"  📌 {sym}: Last={rec['last_price']}, Chg={rec['change']}")

        logger.info(f"✅ CSV: {len(records)} contracts.")
        return records if len(records) >= 3 else None

    except Exception as exc:
        logger.warning(f"CSV strategy error: {exc}")
        return None


# ─────────────────────────────────────────────
# Strategy 3 — undetected-chromedriver + API
# ─────────────────────────────────────────────

def _via_selenium(today: date) -> list[dict] | None:
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.warning("undetected_chromedriver not installed — skipping.")
        return None

    logger.info("🤖 Trying Selenium + API…")
    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=opts, headless=True, use_subprocess=True)
    driver.set_page_load_timeout(60)

    try:
        driver.get(PAGE_URL)
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(text(),'SRJ') or contains(text(),'SRK')]")
            )
        )
        time.sleep(3)

        sel_cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        xsrf = unquote(sel_cookies.get("XSRF-TOKEN", ""))

        session = _make_session()
        session.cookies.update(sel_cookies)

        symbols = _build_symbols(years_ahead=2)
        url = (
            f"{BASE_URL}/proxies/core-api/v1/quotes/get"
            f"?symbols={symbols}&fields={API_FIELDS}&raw=1"
        )
        r = session.get(url, headers={
            "Accept": "application/json",
            "x-xsrf-token": xsrf,
        }, timeout=25)

        if r.status_code != 200:
            logger.warning(f"Selenium+API: {r.status_code}")
            return None

        data = r.json().get("data", [])
        records = []
        for item in data:
            raw = item.get("raw", item)
            sym = str(raw.get("symbol", "")).strip()
            if not sym or _f(raw.get("lastPrice")) is None:
                continue
            rec = {
                "scrape_date":   today,
                "contract":      sym,
                "last_price":    _f(raw.get("lastPrice")),
                "change":        _f(raw.get("priceChange")),
                "open_price":    _f(raw.get("openPrice")),
                "high":          _f(raw.get("highPrice")),
                "low":           _f(raw.get("lowPrice")),
                "previous":      _f(raw.get("previousPrice")),
                "volume":        _i(raw.get("volume")),
                "open_interest": _i(raw.get("openInterest")),
                "updated_time":  str(raw.get("tradeTime", "")) or None,
            }
            records.append(rec)
            logger.info(f"  📌 {sym}: Last={rec['last_price']}, Chg={rec['change']}")

        logger.info(f"✅ Selenium+API: {len(records)} contracts.")
        return records if len(records) >= 3 else None

    except Exception as exc:
        logger.warning(f"Selenium strategy error: {exc}")
        return None
    finally:
        driver.quit()


# ─────────────────────────────────────────────
# DB upsert
# ─────────────────────────────────────────────

def _upsert(records: list[dict]) -> bool:
    db = SessionLocal()
    try:
        ins = upd = 0
        for rec in records:
            existing = (
                db.query(SofrFutures)
                .filter(
                    SofrFutures.scrape_date == rec["scrape_date"],
                    SofrFutures.contract    == rec["contract"],
                )
                .first()
            )
            if existing:
                for k, v in rec.items():
                    if k not in ("scrape_date", "contract"):
                        setattr(existing, k, v)
                upd += 1
            else:
                db.add(SofrFutures(**rec))
                ins += 1
        db.commit()
        logger.info(f"💾 DB: {ins} inserted, {upd} updated (date: {records[0]['scrape_date']})")
        return True
    except Exception as exc:
        db.rollback()
        logger.error(f"❌ DB error: {exc}")
        return False
    finally:
        db.close()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def scrape_sofr_futures() -> bool:
    logger.info("🚀 SOFR Futures scraper starting…")
    today = date.today()

    for name, fn in [
        ("API",      lambda: _via_api(today)),
        ("CSV",      lambda: _via_csv(today)),
        ("Selenium", lambda: _via_selenium(today)),
    ]:
        logger.info(f"▶ Trying strategy: {name}")
        records = fn()
        if records:
            return _upsert(records)
        logger.warning(f"✗ Strategy '{name}' returned no usable data.")

    logger.error("❌ All strategies failed.")
    return False