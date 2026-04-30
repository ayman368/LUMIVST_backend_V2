"""
Eurodollar Futures Scraper — Investing.com  [FIXED v2]
=======================================================
الإصلاحات:
  ✅ طريقة جديدة: API مباشرة من Investing.com (بدون Selenium)
  ✅ إصلاح مشكلة ChromeDriver version mismatch
  ✅ fallback تلقائي بين 3 طرق

الأعمدة: symbol | contract | last_price | change | open | high | low | volume
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import date
from typing import Optional
from pathlib import Path

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("eurodollar_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

PAGE_URL = "https://www.investing.com/rates-bonds/eurodollar-futures"

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │  API داخلية لـ Investing.com — بترجع JSON مباشرة بدون JS rendering        │
# │  category_id=27 هو Eurodollar Futures على الموقع                           │
# └─────────────────────────────────────────────────────────────────────────────┘
API_URL = (
    "https://api.investing.com/api/financialdata/assets/earningsByCategoryId"
    "?category=eurodollar-futures&country_id=5&tab_id=overview"
)

# الـ API البديلة (pairlist)
API_URL_ALT = (
    "https://api.investing.com/api/financialdata/assets/pairsByCategories"
    "?category=eurodollar-futures"
)

HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) "
        "Gecko/20100101 Firefox/135.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.investing.com/",
}

HEADERS_API = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) "
        "Gecko/20100101 Firefox/135.0"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.investing.com",
    "Referer": "https://www.investing.com/rates-bonds/eurodollar-futures",
    "domain-id": "www",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().rstrip("s").replace(",", "").lstrip("+")
    if s.upper() in ("N/A", "-", "", "NONE", "N/D"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _i(v) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s.upper() in ("N/A", "-", "", "NONE", "N/D"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _save_debug(content: str, path: str = "debug_page.html") -> None:
    try:
        Path(path).write_text(content, encoding="utf-8")
        logger.info(f"  💾 محفوظ لـ debug: {path}")
    except Exception:
        pass


# ─── Method 1: Direct API ─────────────────────────────────────────────────────

def _fetch_via_api(session) -> Optional[list[dict]]:
    """
    ✅ الطريقة الأسرع والأكثر موثوقية.
    Investing.com بتعرض بيانات الجدول عبر API داخلية.
    بنجيب session cookies أولاً من الصفحة الرئيسية.
    """
    import requests

    today = date.today().isoformat()
    logger.info("🔌 جاري الجلب عبر Investing.com API…")

    # زيارة الصفحة الرئيسية للحصول على cookies ضرورية
    try:
        session.get("https://www.investing.com/", timeout=20, headers=HEADERS_BROWSER)
        time.sleep(1.5)
        # زيارة صفحة Eurodollar للـ Referer cookie
        session.get(PAGE_URL, timeout=20, headers=HEADERS_BROWSER)
        time.sleep(1)
    except Exception as e:
        logger.warning(f"  ⚠️  تعذّر جلب الـ cookies: {e}")

    # قائمة API endpoints نجربهم بالترتيب
    api_endpoints = [
        # Endpoint 1: الجدول المباشر
        "https://api.investing.com/api/financialdata/assets/pairsByCategories"
        "?category=eurodollar-futures&limit=50&include_major=0",

        # Endpoint 2: بديل
        "https://api.investing.com/api/financialdata/assets/earningsByCategoryId"
        "?category=eurodollar-futures",

        # Endpoint 3: search
        "https://api.investing.com/api/search/v2/search?q=eurodollar+futures&tab=futures&limit=20",
    ]

    for endpoint in api_endpoints:
        try:
            resp = session.get(endpoint, headers=HEADERS_API, timeout=20)
            logger.info(f"  API [{resp.status_code}]: {endpoint[:80]}…")

            if resp.status_code != 200:
                continue

            data = resp.json()
            records = _parse_api_response(data, today)
            if records and len(records) >= 3:
                logger.info(f"  ✅ API نجحت: {len(records)} عقد")
                return records

        except Exception as e:
            logger.warning(f"  ⚠️  API endpoint فشل: {e}")

    return None


def _parse_api_response(data: dict, today: str) -> Optional[list[dict]]:
    """تحليل JSON من API."""
    records = []

    # هيكل 1: {"data": [{"symbol": ..., "last": ..., ...}]}
    items = None
    if isinstance(data, dict):
        for key in ("data", "pairs", "results", "quotes", "assets"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
    elif isinstance(data, list):
        items = data

    if not items:
        return None

    for item in items:
        if not isinstance(item, dict):
            continue

        # استخرج الحقول — بنجرب أسماء مختلفة
        symbol   = item.get("symbol") or item.get("pairSymbol") or item.get("code")
        contract = (
            item.get("name") or item.get("shortName") or
            item.get("full_name") or item.get("pair_name") or symbol
        )
        last     = _f(item.get("last") or item.get("lastPrice") or item.get("bid"))
        change   = _f(item.get("change") or item.get("chg") or item.get("netChange"))
        open_p   = _f(item.get("open") or item.get("openPrice"))
        high     = _f(item.get("high") or item.get("dayHigh"))
        low      = _f(item.get("low") or item.get("dayLow"))
        volume   = _i(item.get("volume") or item.get("vol"))
        upd_time = str(item.get("time") or item.get("lastTime") or "")

        if not contract or last is None:
            continue

        # فلتر: نأخذ فقط Eurodollar (GE*)
        if symbol and not str(symbol).upper().startswith("GE"):
            continue

        records.append({
            "scrape_date":   today,
            "symbol":        symbol,
            "contract":      contract,
            "last_price":    last,
            "change":        change,
            "open_price":    open_p,
            "high":          high,
            "low":           low,
            "previous":      None,
            "volume":        volume,
            "open_interest": None,
            "updated_time":  upd_time,
        })

    return records if records else None


# ─── Method 2: HTML Table Parsing ─────────────────────────────────────────────

def _fetch_via_html(session) -> Optional[list[dict]]:
    """
    ✅ جلب HTML الكامل + تحليل الجدول.
    تشتغل لو الصفحة render server-side أو لو Cloudflare bypass نجح.
    """
    import cloudscraper as cs_module

    today = date.today().isoformat()

    for label, getter in [
        ("cloudscraper", lambda: _get_cloudscraper_html()),
        ("requests",     lambda: _get_requests_html(session)),
    ]:
        logger.info(f"  محاولة HTML عبر {label}…")
        html = getter()
        if not html or len(html) < 100_000:
            logger.warning(f"  ⚠️  {label}: HTML قصير جداً ({len(html) if html else 0} حرف) — الصفحة تُعرض بـ JS")
            continue

        records = _parse_html_table(html, today)
        if records:
            return records

    return None


def _get_cloudscraper_html() -> Optional[str]:
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "firefox", "platform": "windows", "mobile": False}
        )
        scraper.get("https://www.investing.com/", timeout=20)
        time.sleep(2)
        resp = scraper.get(PAGE_URL, timeout=30)
        resp.raise_for_status()
        logger.info(f"    HTTP {resp.status_code} | {len(resp.text):,} حرف")
        return resp.text
    except ImportError:
        logger.warning("    cloudscraper غير مثبت")
        return None
    except Exception as e:
        logger.warning(f"    cloudscraper فشل: {e}")
        return None


def _get_requests_html(session) -> Optional[str]:
    try:
        session.headers.update(HEADERS_BROWSER)
        session.get("https://www.investing.com/", timeout=20)
        time.sleep(1.5)
        resp = session.get(PAGE_URL, timeout=30)
        resp.raise_for_status()
        logger.info(f"    HTTP {resp.status_code} | {len(resp.text):,} حرف")
        return resp.text
    except Exception as e:
        logger.warning(f"    requests فشل: {e}")
        return None


def _parse_html_table(html: str, today: str) -> Optional[list[dict]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    table = (
        soup.find("table", {"id": "cr1"}) or
        next(
            (t for t in soup.find_all("table")
             if "month" in t.get_text().lower() and "last" in t.get_text().lower()),
            None
        )
    )

    if not table:
        logger.error("    ❌ الجدول مش موجود في HTML")
        _save_debug(html)
        return None

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")
    records = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        off = 1 if len(cells) >= 10 else 0
        month_cell = cells[off]
        contract = month_cell.get_text(strip=True)

        symbol = None
        link = month_cell.find("a")
        if link:
            href = link.get("href", "")
            if "symbol=" in href:
                symbol = href.split("symbol=")[-1].strip()

        i = off + 1
        last  = _f(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        chg   = _f(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        open_ = _f(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        high  = _f(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        low   = _f(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        vol   = _i(cells[i].get_text(strip=True)) if len(cells) > i else None; i += 1
        upd   = cells[i].get_text(strip=True)     if len(cells) > i else None

        if not contract or last is None:
            continue
        if contract.lower() in ("month", "contract", "name", "symbol"):
            continue

        records.append({
            "scrape_date": today,  "symbol": symbol,
            "contract": contract,  "last_price": last,
            "change": chg,         "open_price": open_,
            "high": high,          "low": low,
            "previous": None,      "volume": vol,
            "open_interest": None, "updated_time": upd,
        })

    logger.info(f"    📊 {len(records)} عقد مستخرج")
    return records if len(records) >= 3 else None


# ─── Method 3: Selenium (Fixed) ───────────────────────────────────────────────

def _fetch_via_selenium(headless: bool = True) -> Optional[list[dict]]:
    """
    ✅ Selenium مصلوح — يتجنب مشكلة version mismatch.

    الإصلاح: بدل undetected_chromedriver نستخدم selenium-manager
    المدمج في selenium 4.6+ اللي بيحمّل ChromeDriver المناسب تلقائياً.

    لو مازلت تحب undetected_chromedriver، شغّل أولاً:
        Remove-Item -Recurse -Force "$env:APPDATA\\undetected_chromedriver"
    وحدد version_main يدوياً زي ما في الكود.
    """
    today = date.today().isoformat()
    logger.info(f"🤖 Selenium {'(headless)' if headless else '(visible)'}…")

    # ── محاولة 1: selenium عادي مع selenium-manager (بيختار ChromeDriver تلقائياً) ──
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(f"--user-agent={HEADERS_BROWSER['User-Agent']}")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])

        # selenium-manager (selenium >= 4.6) بيحدد ChromeDriver تلقائياً
        logger.info("  جاري استخدام selenium-manager (بدون undetected_chromedriver)…")
        driver = webdriver.Chrome(options=opts)
        return _run_selenium_scrape(driver, today)

    except Exception as e:
        logger.warning(f"  ⚠️  selenium عادي فشل: {e}")

    # ── محاولة 2: undetected_chromedriver مع version_main صريح ──
    try:
        import subprocess
        import re as _re

        # اكتشاف version Chrome الفعلي
        chrome_ver = 147  # default
        try:
            result = subprocess.run(
                ["reg", "query",
                 r"HKLM\SOFTWARE\Google\Chrome\BLBeacon", "/v", "version"],
                capture_output=True, text=True
            )
            match = _re.search(r"(\d+)\.\d+\.\d+\.\d+", result.stdout)
            if match:
                chrome_ver = int(match.group(1))
                logger.info(f"  Chrome version اكتُشف: {chrome_ver}")
        except Exception:
            logger.warning(f"  ⚠️  تعذّر اكتشاف Chrome version، نستخدم {chrome_ver}")

        import undetected_chromedriver as uc

        opts2 = uc.ChromeOptions()
        if headless:
            opts2.add_argument("--headless=new")
        opts2.add_argument("--no-sandbox")
        opts2.add_argument("--disable-dev-shm-usage")
        opts2.add_argument("--disable-gpu")
        opts2.add_argument("--disable-software-rasterizer")
        opts2.add_argument("--disable-extensions")
        opts2.add_argument("--window-size=1920,1080")
        opts2.add_argument(f"--user-agent={HEADERS_BROWSER['User-Agent']}")

        logger.info(f"  undetected_chromedriver مع version_main={chrome_ver}…")
        driver2 = uc.Chrome(options=opts2, version_main=chrome_ver)
        return _run_selenium_scrape(driver2, today)

    except Exception as e:
        logger.error(f"  ❌ undetected_chromedriver فشل: {e}")
        return None


def _run_selenium_scrape(driver, today: str) -> Optional[list[dict]]:
    """تنفيذ الـ scraping بعد تهيئة الـ driver."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    try:
        driver.set_page_load_timeout(120)

        # زيارة الرئيسية أولاً
        driver.get("https://www.investing.com/")
        time.sleep(2)

        # قبول cookies لو ظهر
        try:
            btn = driver.find_element(
                By.XPATH,
                "//*[contains(@id,'onetrust') or contains(@class,'accept') or "
                "(contains(text(),'Accept') and not(contains(text(),'terms')))]"
            )
            btn.click()
            time.sleep(1)
        except Exception:
            pass

        driver.get(PAGE_URL)

        # انتظر الجدول
        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "table#cr1, table.genTbl, [data-test='futures-table']")
            )
        )
        time.sleep(3)  # انتظر اكتمال التحميل

        html = driver.page_source
        logger.info(f"  ✅ HTML مجلوب: {len(html):,} حرف")
        return _parse_html_table(html, today)

    except Exception as e:
        logger.error(f"  ❌ Selenium scrape فشل: {e}")
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ─── Output ───────────────────────────────────────────────────────────────────

def save_csv(records: list[dict], path: str) -> bool:
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"💾 CSV محفوظ: {path} ({len(records)} صف)")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ CSV: {e}")
        return False


def save_json(records: list[dict], path: str) -> bool:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 JSON محفوظ: {path} ({len(records)} سجل)")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ JSON: {e}")
        return False


def print_table(records: list[dict]) -> None:
    if not records:
        return
    w = 110
    print("\n" + "═" * w)
    print(
        f"  {'Symbol':<8} {'Contract':<12} {'Last':>9} {'Chg':>8} "
        f"{'Open':>9} {'High':>9} {'Low':>9} {'Volume':>9} {'Time':>10}"
    )
    print("─" * w)
    for r in records:
        chg = r.get("change") or 0
        sign = "+" if chg > 0 else ""
        chg_str = f"{sign}{chg:.4f}" if chg != 0 else "0.0000"
        print(
            f"  {str(r.get('symbol') or ''):8} "
            f"{str(r['contract']):<12} "
            f"{str(r['last_price'] or ''):>9} "
            f"{chg_str:>8} "
            f"{str(r['open_price'] or ''):>9} "
            f"{str(r['high'] or ''):>9} "
            f"{str(r['low'] or ''):>9} "
            f"{str(r['volume'] or 0):>9} "
            f"{str(r['updated_time'] or ''):>10}"
        )
    print("═" * w)
    print(f"  ✅ {len(records)} عقد | تاريخ السحب: {records[0]['scrape_date']}\n")


def upsert_db(records: list[dict]) -> bool:
    try:
        from app.core.database import SessionLocal
        from app.models.economic_indicators import EurodollarFutures
    except ImportError:
        logger.warning("⚠️  DB models مش موجودة — تم تخطي DB")
        return False

    db = SessionLocal()
    try:
        ins = upd = 0
        for rec in records:
            existing = (
                db.query(EurodollarFutures)
                .filter(
                    EurodollarFutures.scrape_date == rec["scrape_date"],
                    EurodollarFutures.contract == rec["contract"],
                )
                .first()
            )
            if existing:
                for k, v in rec.items():
                    if k not in ("scrape_date", "contract"):
                        setattr(existing, k, v)
                upd += 1
            else:
                db.add(EurodollarFutures(**rec))
                ins += 1
        db.commit()
        logger.info(f"💾 DB: {ins} جديد | {upd} محدث")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"❌ خطأ DB: {e}")
        return False
    finally:
        db.close()


# ─── Main Entry Point ─────────────────────────────────────────────────────────

def scrape_eurodollar_futures(
    method: str = "auto",
    output_csv: Optional[str] = None,
    output_json: Optional[str] = None,
    save_db: bool = False,
    print_results: bool = True,
    force: bool = False,
) -> Optional[list[dict]]:
    """
    نقطة الدخول الرئيسية.

    ترتيب الطرق في auto:
      1. API مباشرة  (أسرع — بدون JS)
      2. HTML parsing (مع cloudscraper/requests)
      3. Selenium     (أبطأ لكن الأضمن)
    """
    import requests

    logger.info("=" * 60)
    logger.info("🚀 Eurodollar Futures Scraper v2 — Investing.com")
    logger.info(f"   الطريقة: {method} | {date.today()}")
    logger.info("=" * 60)

    # ── فحص إذا تم السحب اليوم مسبقاً ──
    if save_db and not force:
        try:
            from app.core.database import SessionLocal as _SL
            from app.models.economic_indicators import EurodollarFutures as _EF
            _db = _SL()
            try:
                existing_count = _db.query(_EF).filter(
                    _EF.scrape_date == date.today()
                ).count()
                if existing_count > 0:
                    logger.info(f"ℹ️ تم السحب مسبقاً اليوم ({date.today()}). يوجد {existing_count} سجل. استخدم --force لإعادة السحب.")
                    return None
            finally:
                _db.close()
        except ImportError:
            pass

    session = requests.Session()
    session.headers.update(HEADERS_BROWSER)
    records = None

    if method == "api":
        records = _fetch_via_api(session)
    elif method == "html":
        records = _fetch_via_html(session)
    elif method == "selenium":
        records = _fetch_via_selenium()
    else:  # auto
        steps = [
            ("api",      lambda: _fetch_via_api(session)),
            ("html",     lambda: _fetch_via_html(session)),
            ("selenium", lambda: _fetch_via_selenium()),
        ]
        for name, fn in steps:
            logger.info(f"\n{'─'*40}\n  جاري تجربة: {name}")
            try:
                records = fn()
            except Exception as e:
                logger.warning(f"  ⚠️  {name} رمى exception: {e}")
                records = None

            if records and len(records) >= 3:
                logger.info(f"  ✅ نجحت: {name} ({len(records)} عقد)")
                break
            logger.warning(f"  ⚠️  فشلت أو بيانات غير كافية: {name}")

    if not records:
        logger.error("❌ فشل السحب من جميع الطرق!")
        return None

    logger.info(f"\n✅ تم سحب {len(records)} عقد بنجاح!")

    if print_results:
        print_table(records)
    if output_csv:
        save_csv(records, output_csv)
    if output_json:
        save_json(records, output_json)
    if save_db:
        upsert_db(records)

    return records


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Eurodollar Futures Scraper v2 — Investing.com",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
أمثلة:
  python eurodollar_scraper.py                        # auto (api → html → selenium)
  python eurodollar_scraper.py --method api           # API مباشرة فقط
  python eurodollar_scraper.py --method selenium      # Selenium مصلوح
  python eurodollar_scraper.py -o data.csv -j data.json
  python eurodollar_scraper.py --db
        """,
    )
    parser.add_argument(
        "--method", "-m",
        choices=["auto", "api", "html", "selenium"],
        default="auto",
        help="طريقة الجلب (default: auto)",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE.csv",
        default=f"eurodollar_{date.today().isoformat()}.csv",
        help="مسار ملف CSV",
    )
    parser.add_argument(
        "--json", "-j",
        metavar="FILE.json",
        default=None,
        help="مسار ملف JSON (اختياري)",
    )
    parser.add_argument("--db",    action="store_true", help="حفظ في DB")
    parser.add_argument("--quiet", "-q", action="store_true", help="بدون طباعة")
    parser.add_argument("--force", action="store_true", help="إعادة السحب حتى لو تم السحب اليوم")

    args = parser.parse_args()
    result = scrape_eurodollar_futures(
        method=args.method,
        output_csv=args.output,
        output_json=args.json,
        save_db=args.db,
        print_results=not args.quiet,
        force=args.force,
    )
    sys.exit(0 if result else 1)