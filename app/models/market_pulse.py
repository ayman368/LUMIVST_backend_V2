"""
market_pulse.py
===============
SQLAlchemy ORM — market_pulse table.

Numeric(p,s) everywhere instead of Float = exact DECIMAL, no IEEE-754 drift.
String lengths are intentionally tight (signal codes are 2-4 chars max).
Composite indexes cover the common WHERE + ORDER BY patterns in the router.

Source data (columns A–H) comes from the ``historical_reports`` table
(report_date, open_price, high_price, …), parsed from strings into Numerics.
"""

from sqlalchemy import Column, Index, Integer, String, Date, Numeric
from app.core.database import Base

P12_6 = Numeric(12, 6)   # prices / MAs          e.g. 11031.320000
P10_8 = Numeric(10, 8)   # ratios / percentages   e.g.  0.00749365
P20_2 = Numeric(20, 2)   # volume / value         e.g. 298895998.00
P6_2  = Numeric(6,  2)   # small counts           e.g.         9.00


class MarketPulse(Base):
    __tablename__ = "market_pulse"

    id = Column(Integer, primary_key=True, index=True)

    # A–H  (sourced from historical_reports)
    date          = Column(Date,    nullable=False, unique=True, index=True)
    open          = Column(P12_6,   nullable=False)
    high          = Column(P12_6,   nullable=False)
    low           = Column(P12_6,   nullable=False)
    close         = Column(P12_6,   nullable=False)
    volume_traded = Column(P20_2,   nullable=False)
    value_traded  = Column(P20_2,   nullable=True)
    no_of_trades  = Column(Integer, nullable=True)

    # I–K
    change            = Column(P12_6, nullable=True)
    change_pct        = Column(P10_8, nullable=True)
    volume_change_pct = Column(P10_8, nullable=True)

    # L–O
    ema_21  = Column(P12_6, nullable=True)
    sma_50  = Column(P12_6, nullable=True)
    sma_150 = Column(P12_6, nullable=True)
    sma_200 = Column(P12_6, nullable=True)

    # P–S
    market_pulse = Column(String(40), nullable=True)
    buy_switch   = Column(String(4),  nullable=True)
    rd           = Column(String(4),  nullable=True)
    rd_count     = Column(Integer,    nullable=True)

    # T–V
    ftd     = Column(String(4), nullable=True)
    ftd_low = Column(P12_6,    nullable=True)
    rd_low  = Column(P12_6,    nullable=True)

    # Sell signals W–AC
    ftd_undercut             = Column(String(4), nullable=True)
    failed_rally_attempt     = Column(String(4), nullable=True)
    day_undercut_21          = Column(String(4), nullable=True)
    overdue_break_below_21ma = Column(String(4), nullable=True)
    trending_below_21ma      = Column(String(4), nullable=True)
    living_below_21ma        = Column(String(4), nullable=True)
    break_below_50ma         = Column(String(4), nullable=True)

    # Buy signals AD, AF–AI
    ftd_1               = Column(String(4), nullable=True)
    additional_ftd      = Column(String(4), nullable=True)
    low_above_21ma      = Column(String(4), nullable=True)
    trending_above_21ma = Column(String(4), nullable=True)
    living_above_21ma   = Column(String(4), nullable=True)
    low_above_50ma      = Column(String(4), nullable=True)

    # AJ
    s11 = Column(String(4), nullable=True)

    # AK–AS
    dd_sd                    = Column(String(4), nullable=True)
    distribution_days        = Column(P6_2,      nullable=True)
    cluster                  = Column(P6_2,      nullable=True)
    current_outlook          = Column(String(4), nullable=True)
    distribution_days_2      = Column(P6_2,      nullable=True)
    cluster_1                = Column(P6_2,      nullable=True)
    distribution_day_fall_of = Column(P10_8,     nullable=True)

    # AU–AV
    year  = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)

    # AX–BF
    day_v_close_21        = Column(P10_8, nullable=True)
    atr_pct               = Column(P10_8, nullable=True)
    atr                   = Column(P12_6, nullable=True)
    tr                    = Column(P12_6, nullable=True)
    high_minus_low        = Column(P12_6, nullable=True)
    high_minus_prev_close = Column(P12_6, nullable=True)
    prev_close_minus_low  = Column(P12_6, nullable=True)
    opn_close             = Column(P12_6, nullable=True)
    close_pct             = Column(P10_8, nullable=True)

    # BO–BP
    mv    = Column(P10_8, nullable=True)
    ftd_r = Column(P10_8, nullable=True)

    __table_args__ = (
        Index("ix_mp_date_outlook", "date", "current_outlook"),
        Index("ix_mp_date_pulse",   "date", "market_pulse"),
        Index("ix_mp_year_month",   "year", "month"),
    )
