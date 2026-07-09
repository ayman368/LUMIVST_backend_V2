"""
Admin System Router
Scraper management, status checks, and data freshness endpoints.
Mount at: /api/admin/system
"""

import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

from app.services.scraper_service import (
    get_scraper_status,
    run_daily_scrapers,
    run_full_backfill,
    run_fred,
    run_sp500_price,
    run_sp500_pe,
    run_sp500_ey,
    run_treasury_gov,
    run_tasi_components,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/system", tags=["Admin — System"])


class RunScraperBody(BaseModel):
    scraper:  str                       # 'fred', 'sp500_price', 'sp500_pe', 'sp500_ey',
                                        # 'treasury_gov', 'tasi', 'daily_all', 'full_backfill'
    mode:     Optional[str] = "incremental"
    force:    Optional[bool] = False
    symbols:  Optional[list[str]] = None   # for tasi only


@router.get("/stats", summary="Data freshness and scraper status")
def get_system_stats():
    """
    Returns the last-run timestamp and success status for every scraper,
    plus a quick count of rows in key tables.
    """
    from app.core.database import SessionLocal
    from app.models.economic_indicators import EconomicIndicator, TreasuryYieldCurve, SP500History
    from app.models.tasi_components import TasiComponent
    from app.models.eps_estimates import EpsEstimate
    from sqlalchemy import func

    db = SessionLocal()
    try:
        def latest_date(model, date_col):
            val = db.query(func.max(date_col)).scalar()
            return val.isoformat() if val else None

        table_stats = {
            "economic_indicators": {
                "row_count":   db.query(EconomicIndicator).count(),
                "latest_date": latest_date(EconomicIndicator, EconomicIndicator.report_date),
            },
            "treasury_yield_curve": {
                "row_count":   db.query(TreasuryYieldCurve).count(),
                "latest_date": latest_date(TreasuryYieldCurve, TreasuryYieldCurve.report_date),
            },
            "sp500_history": {
                "row_count":   db.query(SP500History).count(),
                "latest_date": latest_date(SP500History, SP500History.trade_date),
            },
            "tasi_components": {
                "active":   db.query(TasiComponent).filter(TasiComponent.is_active == True).count(),
                "inactive": db.query(TasiComponent).filter(TasiComponent.is_active == False).count(),
            },
            "eps_estimates": {
                "row_count": db.query(EpsEstimate).count(),
            },
        }
    finally:
        db.close()

    return {
        "table_stats":      table_stats,
        "scraper_status":   get_scraper_status(),
    }


@router.post("/run-scraper", summary="Trigger a scraper manually")
def trigger_scraper(body: RunScraperBody, background_tasks: BackgroundTasks):
    """
    Triggers a scraper in the background and returns immediately.
    Poll /api/admin/system/stats to check completion.
    """
    scraper_map = {
        "fred":          lambda: run_fred(),
        "sp500_price":   lambda: run_sp500_price(mode=body.mode),
        "sp500_pe":      lambda: run_sp500_pe(),
        "sp500_ey":      lambda: run_sp500_ey(force=body.force),
        "treasury_gov":  lambda: run_treasury_gov(mode=body.mode),
        "tasi":          lambda: run_tasi_components(symbols=body.symbols),
        "daily_all":     lambda: run_daily_scrapers(),
        "full_backfill": lambda: run_full_backfill(),
    }

    if body.scraper not in scraper_map:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown scraper '{body.scraper}'. Valid options: {list(scraper_map.keys())}",
        )

    background_tasks.add_task(scraper_map[body.scraper])
    return {"status": "started", "scraper": body.scraper, "mode": body.mode}
