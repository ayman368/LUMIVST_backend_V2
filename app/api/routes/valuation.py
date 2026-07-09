"""
Valuation API Router
Provides endpoints for all 8 tabs of the valuation system.
Mount at: /api/valuation
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.valuation_service import ValuationService
from app.core.database import SessionLocal
from app.models.economic_indicators import TreasuryYieldCurve

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/valuation", tags=["Valuation"])

_service = ValuationService()


# ── Tab 1: Bond Dashboard ─────────────────────────────────────────────────────

@router.get("/bond-dashboard", summary="Tab 1 — Bond & Macro Dashboard")
def get_bond_dashboard():
    """
    Returns all key macro signals in a single response:
    bond yields (A, BBB, BB, B), S&P 500 price + EY, labor market
    indicators, treasury spread, and the KSA growth assumption.
    """
    try:
        return _service.get_bond_dashboard()
    except Exception as e:
        logger.error(f"bond-dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/valuation-copy", summary="Tab 1 — Valuation Copy Sheet (History Grid)")
def get_valuation_copy():
    """
    Returns historical arrays for all the main indicators so the frontend
    can render the side-by-side Excel-like 'Valuation - Copy' sheet.
    """
    try:
        return _service.get_valuation_copy_sheet(limit=10)
    except Exception as e:
        logger.error(f"valuation-copy error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ── Tab 2: Daily Treasury Yields ──────────────────────────────────────────────

@router.get("/treasury/daily", summary="Tab 2 — Daily Treasury Yield Data (TRD)")
def get_treasury_daily(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end:   Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    page:  int           = Query(1, ge=1),
    page_size: int       = Query(60, ge=1, le=500),
):
    """
    Returns paginated daily Treasury yield curve rows for all maturities.
    Defaults to the most recent 60 trading days when no date range is given.
    """
    from datetime import date, timedelta
    import datetime as dt

    db = SessionLocal()
    try:
        query = db.query(TreasuryYieldCurve)

        if start:
            query = query.filter(TreasuryYieldCurve.report_date >= start)
        if end:
            query = query.filter(TreasuryYieldCurve.report_date <= end)
        if not start and not end:
            default_start = date.today() - timedelta(days=90)
            query = query.filter(TreasuryYieldCurve.report_date >= default_start)

        total = query.count()
        rows  = (
            query.order_by(TreasuryYieldCurve.report_date.desc())
                 .offset((page - 1) * page_size)
                 .limit(page_size)
                 .all()
        )

        def _f(v):
            return round(float(v), 4) if v is not None else None

        data = [
            {
                "date":    row.report_date.isoformat(),
                "month_1": _f(row.month_1),
                "month_2": _f(row.month_2),
                "month_3": _f(row.month_3),
                "month_4": _f(row.month_4),
                "month_6": _f(row.month_6),
                "year_1":  _f(row.year_1),
                "year_2":  _f(row.year_2),
                "year_3":  _f(row.year_3),
                "year_5":  _f(row.year_5),
                "year_7":  _f(row.year_7),
                "year_10": _f(row.year_10),
                "year_20": _f(row.year_20),
                "year_30": _f(row.year_30),
            }
            for row in rows
        ]

        return {
            "data":      data,
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     (total + page_size - 1) // page_size,
        }
    finally:
        db.close()


@router.get("/treasury/latest", summary="Most recent Treasury yield row")
def get_treasury_latest():
    """Returns only the single most-recent TreasuryYieldCurve row."""
    db = SessionLocal()
    try:
        row = (
            db.query(TreasuryYieldCurve)
              .order_by(TreasuryYieldCurve.report_date.desc())
              .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="No treasury data found")

        def _f(v):
            return round(float(v), 4) if v is not None else None

        return {
            "date":    row.report_date.isoformat(),
            "month_1": _f(row.month_1),
            "month_3": _f(row.month_3),
            "month_6": _f(row.month_6),
            "year_1":  _f(row.year_1),
            "year_2":  _f(row.year_2),
            "year_5":  _f(row.year_5),
            "year_10": _f(row.year_10),
            "year_20": _f(row.year_20),
            "year_30": _f(row.year_30),
        }
    finally:
        db.close()


# ── Tab 3: Monthly Yield Curve ────────────────────────────────────────────────

@router.get("/treasury/monthly-curve", summary="Tab 3 — Monthly Treasury Yield Curve (TYC)")
def get_monthly_yield_curve():
    """
    Returns monthly-averaged yield curve data for the last 13 months.
    Each row is one calendar month with averages across all maturities.
    """
    try:
        return _service.get_monthly_yield_curve()
    except Exception as e:
        logger.error(f"monthly-curve error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Tab 4: Economy Assessment ─────────────────────────────────────────────────

@router.get("/economy-assessment", summary="Tab 4 — Economy Assessment")
def get_economy_assessment():
    """
    Evaluates each macro indicator against configurable thresholds
    and returns structured verdicts plus the S&P 500 price zone classification.
    """
    try:
        return _service.get_economy_assessment()
    except Exception as e:
        logger.error(f"economy-assessment error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Tab 5: SP-500 Valuation Scenarios ────────────────────────────────────────

@router.get("/sp500-scenarios", summary="Tab 5 — S&P 500 Valuation Scenarios (SP-Vlu)")
def get_sp500_scenarios(
    n_years:  int = Query(2,    description="TVM holding period in years (2 or 3)"),
):
    """
    Computes all 10 Fair Value scenarios for the S&P 500 and the
    annualised IRR return for the chosen holding period.
    Always uses 2027 EPS.
    """
    if n_years not in (2, 3):
        raise HTTPException(status_code=400, detail="n_years must be 2 or 3")

    try:
        return _service.calculate_sp500_scenarios(n_years=n_years)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"sp500-scenarios error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Tab 6: Historical P/E ─────────────────────────────────────────────────────

@router.get("/historical-pe", summary="Tab 6 — Historical P/E Analysis (SP-PE)")
def get_historical_pe(
    limit: int = Query(10, ge=5, le=20, description="Number of historical years to return"),
):
    """
    Returns year-by-year P/E, Earnings Yield, and EY/A ratio,
    plus forward estimates and target price calculations.
    """
    try:
        return _service.get_historical_pe(limit=limit)
    except Exception as e:
        logger.error(f"historical-pe error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Tab 7: TASI Market Weight ─────────────────────────────────────────────────

@router.get("/tasi-market-weight", summary="Tab 7 — TASI Market Weight")
def get_tasi_market_weight():
    """
    Computes the TASI weighted-average EPS and P/E using the index component
    weights with a configurable weight cap. Returns both full-index and
    top-70% adjusted views.
    """
    try:
        return _service.get_tasi_market_weight()
    except Exception as e:
        logger.error(f"tasi-market-weight error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Tab 8: Report (TASI summary) ─────────────────────────────────────────────

@router.get("/report", summary="Tab 8 — Executive Report (TASI Summary)")
def get_report():
    """
    Returns a minimal summary of the TASI valuation — the two numbers
    (current weight P/E and top-70% adjusted P/E) intended for printing
    or embedding in a report.
    """
    try:
        full = _service.get_tasi_market_weight()
        return {
            "summary_current": full["summary_current"],
            "summary_top70":   full["summary_top70"],
        }
    except Exception as e:
        logger.error(f"report error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
