"""Pre-aggregated daily screener counts for Minervini trend chart (read-heavy, O(1) API)."""

from sqlalchemy import Column, Date, DateTime, Integer
from sqlalchemy.sql import func

from app.core.database import Base


class ScreenerDailyTrend(Base):
    __tablename__ = "screener_daily_trend_counts"

    date = Column(Date, primary_key=True)
    trend_1m = Column(Integer, nullable=False, default=0)
    trend_4m = Column(Integer, nullable=False, default=0)
    trend_5m_wide = Column(Integer, nullable=False, default=0)
    alrayan = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
