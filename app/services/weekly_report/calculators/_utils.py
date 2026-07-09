"""Shared helpers for weekly report calculators."""

from __future__ import annotations

from datetime import date, datetime


def format_short_date(value: date | datetime | str) -> str:
    """Cross-platform short date e.g. 'Jun 7'."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value[:10])
    if isinstance(value, datetime):
        value = value.date()
    return f"{value.strftime('%b')} {value.day}"


def week_label(week_start: date, week_end: date) -> str:
    week_num = week_end.isocalendar()[1]
    return (
        f"Week {week_num}: "
        f"{format_short_date(week_start)} - "
        f"{format_short_date(week_end)}, {week_end.year}"
    )
