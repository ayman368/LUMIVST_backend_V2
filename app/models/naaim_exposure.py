"""
NAAIM Exposure Index Model
Stores weekly NAAIM survey data alongside S&P 500 close values.
"""

from sqlalchemy import Column, Integer, Date, DateTime, Float, String
from sqlalchemy.sql import func
from app.core.database import Base


class NaaimExposure(Base):
    """Weekly NAAIM Exposure Index data with corresponding S&P 500 levels."""
    __tablename__ = "naaim_exposure"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, unique=True, nullable=False, index=True)  # Survey date (Wednesday)

    # Core metrics
    naaim_index = Column(Float, nullable=False)                    # Mean/Average NAAIM Number
    sp500 = Column(Float, nullable=True)                           # S&P 500 close on survey date

    # Detailed survey stats (from the detailed table)
    bearish = Column(Float, nullable=True)                         # Most bearish response
    quartile_1 = Column(Float, nullable=True)                      # 25th percentile
    quartile_2 = Column(Float, nullable=True)                      # Median (50th percentile)
    quartile_3 = Column(Float, nullable=True)                      # 75th percentile
    bullish = Column(Float, nullable=True)                         # Most bullish response
    std_deviation = Column(Float, nullable=True)                   # Standard deviation

    # Computed fields
    yoy_pct = Column(Float, nullable=True)                         # Year-over-year % change

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class NaaimMetadata(Base):
    """Stores the latest scraped metadata for the NAAIM page."""
    __tablename__ = "naaim_metadata"

    id = Column(Integer, primary_key=True, index=True)
    last_quarter_avg = Column(Float, nullable=True)
    last_quarter_label = Column(String(50), nullable=True)
    posted_on = Column(String(255), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())