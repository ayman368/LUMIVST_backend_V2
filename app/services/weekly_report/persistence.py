"""Persist and retrieve weekly market reports."""

from __future__ import annotations

import json
import math
from datetime import date

import numpy as np
from sqlalchemy.orm import Session

from app.models.weekly_market_report import WeeklyMarketReport
from app.services.weekly_report.report_builder import build_weekly_report


def _json_safe(obj):
    """Recursively convert numpy types to native Python for JSON serialization, and replace NaNs with None."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        if math.isnan(obj):
            return None
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def save_report(db: Session, report: dict) -> WeeklyMarketReport:
    report = _json_safe(report)
    week_end = date.fromisoformat(report["week_end"])
    week_start = date.fromisoformat(report["week_start"])

    row = (
        db.query(WeeklyMarketReport)
        .filter(WeeklyMarketReport.week_end == week_end)
        .first()
    )
    if row:
        row.week_start = week_start
        row.week_label = report.get("week_label")
        row.report_data = report
    else:
        row = WeeklyMarketReport(
            week_start=week_start,
            week_end=week_end,
            week_label=report.get("week_label"),
            report_data=report,
        )
        db.add(row)

    db.commit()
    db.refresh(row)
    return row


def generate_and_save(db: Session, week_end: date) -> WeeklyMarketReport:
    report = build_weekly_report(db, week_end)
    return save_report(db, report)
