from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, UniqueConstraint, BigInteger
from sqlalchemy.sql import func
from app.core.database import Base

class EconomicIndicator(Base):
    __tablename__ = "economic_indicators"
    id = Column(Integer, primary_key=True, index=True)
    report_date = Column(Date, nullable=False, index=True)
    indicator_code = Column(String(50), nullable=False, index=True) # e.g. UNRATE, PAYEMS, IC4WSA
    value = Column(Numeric(precision=15, scale=6), nullable=True)
    yoy_pct = Column(Numeric(precision=10, scale=4), nullable=True)  # Year-over-year % change (scraped directly from source)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (UniqueConstraint('report_date', 'indicator_code', name='uq_economic_indicator_date_code'),)


class TreasuryYieldCurve(Base):
    __tablename__ = "treasury_yield_curves"
    id = Column(Integer, primary_key=True, index=True)
    report_date = Column(Date, nullable=False, unique=True, index=True)
    
    month_1 = Column(Numeric(precision=8, scale=4), nullable=True)
    month_1_5 = Column(Numeric(precision=8, scale=4), nullable=True)
    month_2 = Column(Numeric(precision=8, scale=4), nullable=True)
    month_3 = Column(Numeric(precision=8, scale=4), nullable=True)
    month_4 = Column(Numeric(precision=8, scale=4), nullable=True)
    month_6 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_1 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_2 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_3 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_5 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_7 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_10 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_20 = Column(Numeric(precision=8, scale=4), nullable=True)
    year_30 = Column(Numeric(precision=8, scale=4), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

class SP500History(Base):
    __tablename__ = "sp500_history"
    id         = Column(Integer, primary_key=True, index=True)
    trade_date = Column(Date, unique=True, nullable=False, index=True)
    open       = Column(Numeric(precision=12, scale=4), nullable=True)
    high       = Column(Numeric(precision=12, scale=4), nullable=True)
    low        = Column(Numeric(precision=12, scale=4), nullable=True)
    close      = Column(Numeric(precision=12, scale=4), nullable=True)
    volume     = Column(BigInteger, nullable=True)
    pe_ratio   = Column(Numeric(precision=10, scale=4), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SofrFutures(Base):
    """Daily snapshot of SOFR futures contracts from Barchart."""
    __tablename__ = "sofr_futures"
    id           = Column(Integer, primary_key=True, index=True)
    scrape_date  = Column(Date, nullable=False, index=True)        # The day we scraped
    contract     = Column(String(50), nullable=False, index=True)  # e.g. SRJ26 (Apr '26)
    last_price   = Column(Numeric(precision=10, scale=4), nullable=True)
    change       = Column(Numeric(precision=10, scale=4), nullable=True)
    open_price   = Column(Numeric(precision=10, scale=4), nullable=True)
    high         = Column(Numeric(precision=10, scale=4), nullable=True)
    low          = Column(Numeric(precision=10, scale=4), nullable=True)
    previous     = Column(Numeric(precision=10, scale=4), nullable=True)
    volume       = Column(BigInteger, nullable=True)
    open_interest = Column(Integer, nullable=True)
    updated_time = Column(String(30), nullable=True)               # e.g. "02:46 CT"

    created_at   = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint('scrape_date', 'contract', name='uq_sofr_futures_date_contract'),)


class CmeFedwatch(Base):
    """Daily snapshot of CME FedWatch probabilities for upcoming FOMC meetings."""
    __tablename__ = "cme_fedwatch"
    id            = Column(Integer, primary_key=True, index=True)
    scrape_date   = Column(Date, nullable=False, index=True)        # The day we scraped
    meeting_date  = Column(String(50), nullable=False, index=True)  # e.g., "1 May 2024"
    rate_range    = Column(String(50), nullable=False)              # e.g., "5.25 - 5.50"
    probability   = Column(Numeric(precision=5, scale=2), nullable=False)                   # e.g., 95.5 (percentage)
    
    created_at    = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint('scrape_date', 'meeting_date', 'rate_range', name='uq_fedwatch_date_meeting_rate'),)

