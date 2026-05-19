"""
tasi_settings.py
================
SQLAlchemy ORM — tasi_settings table.

Stores the user-configurable market parameters that affect
Market Pulse calculations (buy switch, disposal days, etc.).
Only ONE row should ever exist (singleton settings pattern).
"""

from sqlalchemy import Column, Integer, Boolean, DateTime, func
from app.core.database import Base


class TasiSettings(Base):
    __tablename__ = "tasi_settings"

    id              = Column(Integer, primary_key=True, index=True)
    buy_switch      = Column(Boolean, nullable=False, default=True)
    breathing_rule  = Column(Boolean, nullable=False, default=False)
    power_trend     = Column(Boolean, nullable=False, default=True)
    market_exposure = Column(Integer, nullable=False, default=100)     # 0-100 %
    disposal_days   = Column(Integer, nullable=False, default=5)       # 1-30
    updated_at      = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
