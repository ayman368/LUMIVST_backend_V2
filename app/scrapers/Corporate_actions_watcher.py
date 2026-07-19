# # backend/scrapers/corporate_actions_watcher.py
# """
# Corporate Actions Watcher — Saudi Exchange (اجراءات المصدر)
# ==============================================================
# Monitors:
#     https://www.saudiexchange.sa/wps/portal/saudiexchange/newsandreports/
#     issuer-financial-calendars/corporate-actions

# for NEW rows (Symbol + Issue Type + Eligibility Date) that require the
# historical-price / indicators / RS pipeline to be re-run — the exact bug
# that hit CHEMANOL (2001) on 15/07/2026.

# Design notes
# ------------
# - CORRECTED (verified by fetching the page two different ways): the table is
#   **NOT** server-rendered. A raw page load returns an EMPTY <table> (just
#   headers); the rows only appear after client-side JS makes an AJAX/XHR call
#   once the page has settled — same underlying pattern `Reports.py` had to
#   reverse-engineer for the Historical Reports page. So this scraper cannot
#   just "open the page and grab the HTML" — it has to actually wait for a
#   real data row (a numeric symbol in the first cell) to show up before
#   reading `page.content()`. See `fetch_corporate_actions_html()`.
# - We use Playwright (same engine as `historical_scraper.py`) rather than
#   plain `requests`, both because of the JS-rendering requirement above and
#   because this site's WAF/CAPTCHA layer blocks plain `requests.get()` (the
#   same reason `Reports.py` had to drive `fetch()` from inside the browser
#   context instead of using the `requests` library directly).
# - While loading, we also log any request whose URL looks like the portlet's
#   AJAX action endpoint. If you capture that URL from a real run, you can
#   later replace `fetch_corporate_actions_html()` with a direct call to it
#   (like `scrape_with_api()` in `Reports.py`) — much faster than rendering
#   the whole page every time.

# - Classification logic (see CLASSIFICATION dict) mirrors what we discussed:
#     AUTO_ADJUST   -> price change is a pure mechanical function of
#                      New Capital / Previous Capital (splits, bonus shares,
#                      capital reduction). Safe to auto re-scrape + recalc.
#     NEEDS_REVIEW  -> capital changed because of an inflow of new money/assets
#                      (rights issue, capital increase, acquisition, merger).
#                      The ratio alone is NOT a valid price-adjustment factor;
#                      flag for manual review / wait for provider's adjusted
#                      close.

# - Unique key for de-duplication: (symbol, issue_type, eligibility_date).
#   Some symbols appear many times in the table with different actions/dates
#   (e.g. MAADEN, BAHRI, TAPRCO) — Symbol alone is NOT unique enough.

# Run this on a schedule (cron / APScheduler) — weekly is enough since the
# table only changes when Tadawul publishes a new action.
# """

# import asyncio
# import logging
# import subprocess
# import sys
# import os
# from datetime import datetime, date
# from typing import List, Dict, Any, Optional

# from bs4 import BeautifulSoup
# from playwright.async_api import async_playwright

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # TODO: adjust these imports to match your real project layout
# from app.core.database import SessionLocal, engine  # same as Reports.py
# from app.models.corporate_actions import CorporateAction  # see model below

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# URL = (
#     "https://www.saudiexchange.sa/wps/portal/saudiexchange/newsandreports/"
#     "issuer-financial-calendars/corporate-actions?locale=en"
# )

# # ---------------------------------------------------------------------------
# # Classification
# # ---------------------------------------------------------------------------
# AUTO_ADJUST_TYPES = {
#     "Capital Reduction",
#     "Bonus Shares",
#     "Forward Shares Split",
#     "Reverse Shares Split",
# }

# NEEDS_REVIEW_TYPES = {
#     "Rights Issue",
#     "Capital Increase",
#     "Capital Increase – Debt Conversion",
#     "Capital Increase - Offering Shares with Suspension of Right Issue",
#     "Acquisition",
#     "Merge",
#     "Fund Units Cancellation",
#     "Unit Splits",
#     "Increase of The Total Value of The Fund Assets",
# }


# def classify(issue_type: str) -> str:
#     issue_type = (issue_type or "").strip()
#     if issue_type in AUTO_ADJUST_TYPES:
#         return "AUTO_ADJUST"
#     if issue_type in NEEDS_REVIEW_TYPES:
#         return "NEEDS_REVIEW"
#     logger.warning(f"Unknown issue_type '{issue_type}' — defaulting to NEEDS_REVIEW")
#     return "NEEDS_REVIEW"


# # ---------------------------------------------------------------------------
# # Parsing helpers
# # ---------------------------------------------------------------------------
# def parse_date(text: str) -> Optional[date]:
#     text = (text or "").strip()
#     for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
#         try:
#             return datetime.strptime(text, fmt).date()
#         except ValueError:
#             continue
#     return None


# def parse_capital(text: str) -> Optional[int]:
#     """'^150,000,000' -> 150000000"""
#     if not text:
#         return None
#     cleaned = text.replace("^", "").replace(",", "").strip()
#     try:
#         return int(cleaned)
#     except ValueError:
#         return None


# def parse_table(html: str) -> List[Dict[str, Any]]:
#     """Parse the Corporate Actions HTML table into row dicts."""
#     soup = BeautifulSoup(html, "html.parser")
#     rows_out: List[Dict[str, Any]] = []

#     table = soup.find("table")
#     if table is None:
#         logger.error("Corporate Actions table not found in page HTML.")
#         return rows_out

#     for tr in table.find_all("tr"):
#         cells = tr.find_all("td")
#         if len(cells) < 7:
#             continue  # header row / malformed row

#         symbol = cells[0].get_text(strip=True)
#         if not symbol.isdigit():
#             continue  # skip header/garbage rows

#         company_name = cells[1].get_text(strip=True)
#         announcement_date = parse_date(cells[2].get_text(strip=True))
#         issue_type = cells[3].get_text(strip=True)
#         eligibility_date = parse_date(cells[4].get_text(strip=True))
#         new_capital = parse_capital(cells[5].get_text(strip=True))
#         previous_capital = parse_capital(cells[6].get_text(strip=True))

#         if not eligibility_date:
#             continue

#         rows_out.append({
#             "symbol": symbol,
#             "company_name": company_name,
#             "recommendation_announcement_date": announcement_date,
#             "issue_type": issue_type,
#             "eligibility_date": eligibility_date,
#             "new_capital": new_capital,
#             "previous_capital": previous_capital,
#             "classification": classify(issue_type),
#         })

#     return rows_out


# # ---------------------------------------------------------------------------
# # Fetching (Playwright — same engine as historical_scraper.py)
# # ---------------------------------------------------------------------------
# async def fetch_corporate_actions_html() -> str:
#     """
#     IMPORTANT: unlike a normal company profile page, this table starts EMPTY
#     on initial page load and is populated afterwards via an AJAX/XHR call
#     (confirmed by comparing a raw fetch — empty <tbody> — against a fetch
#     taken after the JS had already run — full rows since 2021). This is the
#     same pattern `Reports.py` had to work around for the Historical Reports
#     page. So we can't just "open the page and wait a bit" — we have to wait
#     for an ACTUAL DATA ROW (a numeric symbol in the first <td>) to appear,
#     not just for the empty table skeleton.

#     We also log any XHR/fetch request whose URL looks like it might be the
#     corporate-actions data source — useful if you later want to bypass the
#     UI entirely and hit that endpoint directly (like scrape_with_api in
#     Reports.py), which would be far faster and more reliable than rendering
#     the whole page.
#     """
#     discovered_endpoints = []

#     async def on_request(request):
#         url = request.url
#         if "saudiexchange.sa" in url and request.method in ("GET", "POST"):
#             if "!ut/p/" in url and ("corporate-actions" in url or "CompaniesDividendsPortlet" in url):
#                 discovered_endpoints.append(url)

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         page = await browser.new_page()
#         page.on("request", on_request)
#         try:
#             await page.goto(URL, wait_until="networkidle", timeout=60000)

#             try:
#                 await page.wait_for_function(
#                     """() => {
#                         const cells = document.querySelectorAll('table td:first-child');
#                         for (const c of cells) {
#                             if (/^\\d{3,5}$/.test(c.textContent.trim())) return true;
#                         }
#                         return false;
#                     }""",
#                     timeout=45000,
#                 )
#             except Exception:
#                 logger.warning(
#                     "No data rows appeared within 45s. The table may still "
#                     "be loading via AJAX, may require interacting with the "
#                     "'Time Period' filter first, or the site may be showing "
#                     "a CAPTCHA. Dumping whatever HTML is currently present."
#                 )

#             if discovered_endpoints:
#                 logger.info(
#                     f"Possible AJAX endpoint(s) seen during load "
#                     f"(candidates for a faster direct-API approach, à la "
#                     f"Reports.py): {discovered_endpoints}"
#                 )

#             html = await page.content()
#             return html
#         finally:
#             await browser.close()


# # ---------------------------------------------------------------------------
# # DB helpers
# # ---------------------------------------------------------------------------
# def ensure_table_exists():
#     try:
#         CorporateAction.__table__.create(engine, checkfirst=True)
#         logger.info("✅ Table 'corporate_actions' is ready.")
#     except Exception as e:
#         logger.error(f"Error creating table: {e}")


# def get_known_keys(db) -> set:
#     results = db.query(
#         CorporateAction.symbol,
#         CorporateAction.issue_type,
#         CorporateAction.eligibility_date,
#     ).all()
#     return {(r[0], r[1], r[2]) for r in results}


# def save_new_actions(db, new_rows: List[Dict[str, Any]]):
#     for row in new_rows:
#         db.add(CorporateAction(
#             symbol=row["symbol"],
#             company_name=row["company_name"],
#             recommendation_announcement_date=row["recommendation_announcement_date"],
#             issue_type=row["issue_type"],
#             eligibility_date=row["eligibility_date"],
#             new_capital=row["new_capital"],
#             previous_capital=row["previous_capital"],
#             classification=row["classification"],
#             processed=False,
#             detected_at=datetime.utcnow(),
#         ))
#     db.commit()


# # ---------------------------------------------------------------------------
# # Downstream pipeline (re-uses your existing scripts)
# # ---------------------------------------------------------------------------
# def trigger_pipeline_for_symbol(symbol: str):
#     """
#     Mirrors the manual steps we ran for CHEMANOL:
#       1. Re-scrape historical prices for this symbol only.
#       2. Recalculate indicators/moving averages for this symbol.
#     RS recalculation for the WHOLE market is deferred and run ONCE at the
#     end of the batch (see run_watcher), not per-symbol, since it's a full
#     market rebuild.
#     """
#     logger.info(f"  → Re-scraping historical prices for {symbol}...")
#     # TODO: replace with the real invocation used in your project —
#     # e.g. calling HistoricalScraper(symbols=[symbol]).scrape_all()
#     # directly (async) instead of subprocess, if this runs in the same
#     # process/event loop.
#     subprocess.run(
#         [sys.executable, "scripts/historical_scraper.py", "--symbol", symbol],
#         check=False,
#     )

#     logger.info(f"  → Recalculating indicators for {symbol}...")
#     subprocess.run(
#         [sys.executable, "scripts/recalc_full_history.py", "--symbol", symbol],
#         check=False,
#     )


# def trigger_market_rs_recalc():
#     logger.info("  → Rebuilding rs_daily_v2 for the entire market...")
#     subprocess.run([sys.executable, "scripts/recalculate_all_rs.py"], check=False)


# # ---------------------------------------------------------------------------
# # Notifications — plug in Telegram / email / Slack here
# # ---------------------------------------------------------------------------
# def send_alert(new_rows: List[Dict[str, Any]]):
#     if not new_rows:
#         return

#     lines = ["🔔 Corporate Actions detected:\n"]
#     for row in new_rows:
#         lines.append(
#             f"- {row['symbol']} ({row['company_name']}): {row['issue_type']} "
#             f"| Eligibility: {row['eligibility_date']} "
#             f"| {row['previous_capital']} → {row['new_capital']} "
#             f"| {row['classification']}"
#         )
#     message = "\n".join(lines)
#     logger.info(message)

#     # TODO: send via your actual channel, e.g.:
#     # send_telegram_message(message)
#     # send_email(subject="Corporate Actions Alert", body=message)


# # ---------------------------------------------------------------------------
# # Main
# # ---------------------------------------------------------------------------
# def run_watcher():
#     logger.info(f"=== Corporate Actions Watcher started at {datetime.now()} ===")

#     ensure_table_exists()
#     db = SessionLocal()

#     try:
#         known_keys = get_known_keys(db)
#         logger.info(f"DB currently has {len(known_keys)} known corporate actions.")

#         html = asyncio.run(fetch_corporate_actions_html())
#         all_rows = parse_table(html)
#         logger.info(f"Parsed {len(all_rows)} rows from Saudi Exchange.")

#         new_rows = [
#             r for r in all_rows
#             if (r["symbol"], r["issue_type"], r["eligibility_date"]) not in known_keys
#         ]

#         if not new_rows:
#             logger.info("No new corporate actions found. Nothing to do.")
#             return

#         logger.info(f"Found {len(new_rows)} NEW corporate action(s).")
#         save_new_actions(db, new_rows)
#         send_alert(new_rows)

#         auto_symbols = sorted({
#             r["symbol"] for r in new_rows if r["classification"] == "AUTO_ADJUST"
#         })
#         review_symbols = sorted({
#             r["symbol"] for r in new_rows if r["classification"] == "NEEDS_REVIEW"
#         })

#         if review_symbols:
#             logger.warning(
#                 f"⚠️  These symbols need MANUAL review before touching price "
#                 f"history (Rights Issue / Acquisition / Merge / etc.): "
#                 f"{review_symbols}"
#             )

#         for symbol in auto_symbols:
#             trigger_pipeline_for_symbol(symbol)
#             # mark processed
#             db.query(CorporateAction).filter(
#                 CorporateAction.symbol == symbol,
#                 CorporateAction.eligibility_date.in_(
#                     [r["eligibility_date"] for r in new_rows if r["symbol"] == symbol]
#                 ),
#             ).update({"processed": True}, synchronize_session=False)
#         db.commit()

#         if auto_symbols:
#             trigger_market_rs_recalc()

#         logger.info(
#             f"=== Watcher finished: {len(auto_symbols)} auto-processed, "
#             f"{len(review_symbols)} flagged for review ==="
#         )

#     except Exception as e:
#         logger.error(f"Watcher failed: {e}")
#     finally:
#         db.close()


# if __name__ == "__main__":
#     run_watcher()