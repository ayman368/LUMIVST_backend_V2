from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime
from app.core.database import Base

class AporiaAnalytics(Base):
    __tablename__ = "aporia_analytics"

    id = Column(Integer, primary_key=True, index=True)
    filter_category = Column(String, index=True) # e.g., 'all_analytics', 'breakouts'
    
    ticker = Column(String, index=True)
    name = Column(String)
    sector = Column(String)
    market_cap = Column(String)
    val_avg_3mo = Column(String)
    trailingPE = Column(String)
    last = Column(String)
    mtd_rtn = Column(String)
    mo3_rtn = Column(String)
    year_rtn = Column(String)
    daily_trend = Column(String)
    weekly_trend = Column(String)
    monthly_trend = Column(String)
    trend_rank = Column(String)
    pfh_250 = Column(String)
    days_since_high_250 = Column(String)
    breakout = Column(String)
    longest_consolidation_window = Column(String)
    position = Column(String)
    price_extreme = Column(String)
    vol_5_day_chng = Column(String)
    vol_20_day_chng = Column(String)
    
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AporiaChart(Base):
    __tablename__ = "aporia_charts"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    chart_type = Column(String, index=True)
    chart_data = Column(JSON)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
