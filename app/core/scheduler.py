"""
Automated Scraper Scheduler — APScheduler + Render compatible.

Schedule (Egypt Time — Africa/Cairo):
  Daily  (Mon–Fri, 01:00):  SP500, Treasury.gov, Spreads, Eurodollar, FedWatch
  Weekly (Fri,      01:30):  IC4WSA (released Thursday)
  Monthly(10th,     02:00):  UNRATE, PAYEMS, SP500 PE, GuruFocus EY/PE

Why 01:00 AM Egypt?
  = 22:00 UTC = 18:00 ET → 2 hours after US market close.
  Most data sources update within 1–2 h after close.

Activation:
  Set env var  ENABLE_SCHEDULER=true  on Render (or locally).
"""

import logging
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
EGYPT_TZ = "Africa/Cairo"
_scheduler: BackgroundScheduler | None = None


# ─── Job: Daily ────────────────────────────────────────────────
def job_daily_scrapers():
    logger.info("=" * 50)
    logger.info("📅 [Scheduler] DAILY scrapers starting…")

    _safe("SP500", lambda: __import__(
        "app.scrapers.sp500_scraper", fromlist=["scrape_sp500"]
    ).scrape_sp500(mode="incremental"))

    _safe("Treasury.gov", lambda: __import__(
        "app.scrapers.treasury_gov_scraper", fromlist=["scrape_treasury_gov"]
    ).scrape_treasury_gov(mode="incremental"))

    for code in ["BAMLC0A3CA", "BAMLC0A4CBBB", "BAMLC0A3CAEY"]:
        _safe(f"FRED:{code}", lambda c=code: __import__(
            "app.scrapers.fred_scraper", fromlist=["scrape_fred_indicator"]
        ).scrape_fred_indicator(c))

    _safe("Eurodollar", lambda: __import__(
        "app.scrapers.eurodollar_scraper", fromlist=["scrape_eurodollar_futures"]
    ).scrape_eurodollar_futures(save_db=True))

    _safe("CME FedWatch", lambda: __import__(
        "app.scrapers.cmefedwatch_scraper", fromlist=["scrape_cme_fedwatch"]
    ).scrape_cme_fedwatch())

    _clear_cache()
    logger.info("📅 [Scheduler] DAILY scrapers finished.")


# ─── Job: Weekly ───────────────────────────────────────────────
def job_weekly_scrapers():
    logger.info("📆 [Scheduler] WEEKLY scrapers starting…")
    _safe("IC4WSA", lambda: __import__(
        "app.scrapers.fred_scraper", fromlist=["scrape_fred_indicator"]
    ).scrape_fred_indicator("IC4WSA"))
    _clear_cache()
    logger.info("📆 [Scheduler] WEEKLY scrapers finished.")


# ─── Job: Monthly ─────────────────────────────────────────────
def job_monthly_scrapers():
    logger.info("🗓️ [Scheduler] MONTHLY scrapers starting…")

    for code in ["UNRATE", "PAYEMS"]:
        _safe(f"FRED:{code}", lambda c=code: __import__(
            "app.scrapers.fred_scraper", fromlist=["scrape_fred_indicator"]
        ).scrape_fred_indicator(c))

    _safe("SP500 PE (Multpl)", lambda: __import__(
        "app.scrapers.sp500_pe_scraper", fromlist=["scrape_sp500_pe"]
    ).scrape_sp500_pe())

    _safe("GuruFocus EY", lambda: __import__(
        "app.scrapers.gurufocus_scraper", fromlist=["scrape_gurufocus_indicator"]
    ).scrape_gurufocus_indicator(
        url="https://www.gurufocus.com/economic_indicators/151/sp-500-earnings-yield",
        indicator_code="SP500_EY",
        mode="incremental",
    ))

    _safe("GuruFocus PE", lambda: __import__(
        "app.scrapers.gurufocus_scraper", fromlist=["scrape_gurufocus_indicator"]
    ).scrape_gurufocus_indicator(
        url="https://www.gurufocus.com/economic_indicators/57/sp-500-pe-ratio",
        indicator_code="SP500_PE",
        mode="incremental",
    ))

    _clear_cache()
    logger.info("🗓️ [Scheduler] MONTHLY scrapers finished.")


# ─── Helpers ───────────────────────────────────────────────────
def _safe(name: str, fn):
    """Run a scraper function with error handling."""
    try:
        logger.info(f"  🔄 {name}…")
        fn()
        logger.info(f"  ✅ {name} done")
    except Exception as e:
        logger.error(f"  ❌ {name} failed: {e}")


def _clear_cache():
    """
    Clear economic indicator cache keys using a SYNC Redis connection.
    APScheduler runs jobs in a background thread, so we can't reliably
    access the async event loop. A direct sync redis.Redis call avoids
    the 'Event loop is closed' / 'Future attached to a different loop' errors.
    """
    try:
        import redis as sync_redis

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = sync_redis.from_url(redis_url, decode_responses=True, socket_timeout=5)
        keys = r.keys("economic:*")
        if keys:
            r.delete(*keys)
        logger.info(f"  🗑️ Cleared {len(keys)} cache keys")
        r.close()
    except Exception as e:
        logger.warning(f"  ⚠️ Cache clear skipped: {e}")


# ─── Start / Stop ─────────────────────────────────────────────
def start_scheduler():
    global _scheduler
    if os.getenv("ENABLE_SCHEDULER", "false").lower() not in ("true", "1", "yes"):
        logger.info("⏸️ [Scheduler] Disabled. Set ENABLE_SCHEDULER=true to enable.")
        return

    if _scheduler and _scheduler.running:
        return

    _scheduler = BackgroundScheduler(timezone=EGYPT_TZ)

    _scheduler.add_job(
        job_daily_scrapers,
        CronTrigger(day_of_week="mon-fri", hour=1, minute=0, timezone=EGYPT_TZ),
        id="daily", name="Daily Scrapers", replace_existing=True,
    )
    _scheduler.add_job(
        job_weekly_scrapers,
        CronTrigger(day_of_week="fri", hour=1, minute=30, timezone=EGYPT_TZ),
        id="weekly", name="Weekly Scrapers (IC4WSA)", replace_existing=True,
    )
    _scheduler.add_job(
        job_monthly_scrapers,
        CronTrigger(day=10, hour=2, minute=0, timezone=EGYPT_TZ),
        id="monthly", name="Monthly Scrapers", replace_existing=True,
    )

    _scheduler.start()
    logger.info("✅ [Scheduler] Started — timezone: Africa/Cairo")
    for job in _scheduler.get_jobs():
        logger.info(f"   📌 {job.name} → next: {job.next_run_time}")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("🛑 [Scheduler] Stopped.")
        _scheduler = None
