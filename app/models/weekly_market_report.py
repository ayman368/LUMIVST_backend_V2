"""SQLAlchemy model for persisted weekly market update reports."""

from sqlalchemy import Column, Date, DateTime, Integer, JSON, String, Index
from sqlalchemy.sql import func

from app.core.database import Base


class WeeklyMarketReport(Base):
    __tablename__ = "weekly_market_reports"

    id = Column(Integer, primary_key=True, index=True)
    week_start = Column(Date, nullable=False, index=True)
    week_end = Column(Date, nullable=False, unique=True, index=True)
    week_label = Column(String(120), nullable=True)
    report_data = Column(JSON, nullable=False)
    generated_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_weekly_reports_week_end", "week_end"),
    )

    def __repr__(self) -> str:
        return f"<WeeklyMarketReport(week_end={self.week_end})>"
