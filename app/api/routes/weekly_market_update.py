"""Weekly Market Update API routes."""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models.weekly_market_report import WeeklyMarketReport
from app.schemas.weekly_market_report import (
    GenerateReportResponse,
    WeeklyReportListResponse,
    WeeklyReportResponse,
    WeeklyReportSummary,
)
from app.services.weekly_report.data_loader import resolve_week_end
from app.services.weekly_report.persistence import generate_and_save

router = APIRouter(prefix="/weekly-market-update", tags=["Weekly Market Update"])


@router.get("/latest", response_model=WeeklyReportResponse)
def get_latest_report(db: Session = Depends(get_db)):
    row = (
        db.query(WeeklyMarketReport)
        .order_by(desc(WeeklyMarketReport.week_end))
        .first()
    )
    if not row:
        raise HTTPException(
            status_code=404,
            detail="No weekly report generated yet. Run generate_weekly_report.py first.",
        )
    return WeeklyReportResponse(
        id=row.id,
        week_start=row.week_start,
        week_end=row.week_end,
        week_label=row.week_label,
        generated_at=row.generated_at,
        report=row.report_data,
    )


@router.get("/list", response_model=WeeklyReportListResponse)
def list_reports(
    limit: int = Query(52, ge=1, le=260),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(WeeklyMarketReport)
        .order_by(desc(WeeklyMarketReport.week_end))
        .limit(limit)
        .all()
    )
    return WeeklyReportListResponse(
        reports=[
            WeeklyReportSummary(
                id=r.id,
                week_start=r.week_start,
                week_end=r.week_end,
                week_label=r.week_label,
                generated_at=r.generated_at,
            )
            for r in rows
        ],
        total=len(rows),
    )


@router.get("/{week_end}", response_model=WeeklyReportResponse)
def get_report_by_week(week_end: date, db: Session = Depends(get_db)):
    row = (
        db.query(WeeklyMarketReport)
        .filter(WeeklyMarketReport.week_end == week_end)
        .first()
    )
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No report found for week ending {week_end}"
        )
    return WeeklyReportResponse(
        id=row.id,
        week_start=row.week_start,
        week_end=row.week_end,
        week_label=row.week_label,
        generated_at=row.generated_at,
        report=row.report_data,
    )


@router.post("/generate", response_model=GenerateReportResponse)
def generate_report(
    week_end: Optional[date] = Query(None, description="Week end date (defaults to latest price date)"),
    db: Session = Depends(get_db),
):
    target = resolve_week_end(db, week_end)
    try:
        row = generate_and_save(db, target)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return GenerateReportResponse(
        success=True,
        week_end=row.week_end,
        week_label=row.week_label or "",
        message=f"Weekly report saved for {row.week_label}",
    )
