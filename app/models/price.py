from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, Index, BigInteger
from app.core.database import Base
from datetime import datetime

class Price(Base):
    """
    جدول الأسعار التاريخية للأسهم
    يخزن بيانات OHLCV اليومية
    """
    __tablename__ = "prices"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # معلومات السهم
    industry_group = Column(String(100))
    sector = Column(String(100))
    industry = Column(String(100))
    sub_industry = Column(String(100))
    symbol = Column(String(10), nullable=False, index=True)
    company_name = Column(String(200))
    
    # التاريخ
    date = Column(Date, nullable=False, index=True)
    
    # بيانات السعر - NUMERIC(12, 4) للحفاظ على دقة البيانات
    open = Column(Numeric(12, 4))
    high = Column(Numeric(12, 4))
    low = Column(Numeric(12, 4))
    close = Column(Numeric(12, 4), nullable=False)
    
    # بيانات التداول
    change = Column(Numeric(14, 4))
    change_percent = Column(Numeric(8, 4))
    volume_traded = Column(BigInteger)
    value_traded_sar = Column(Numeric(18, 2))
    no_of_trades = Column(Integer)
    market_cap = Column(Numeric(20, 2))
    
    # Technical Indicators (Red Columns)
    price_minus_sma_10 = Column(Numeric(14, 4))
    price_minus_sma_21 = Column(Numeric(14, 4))
    price_minus_sma_50 = Column(Numeric(14, 4))
    price_minus_sma_150 = Column(Numeric(14, 4))
    price_minus_sma_200 = Column(Numeric(14, 4))
    
    fifty_two_week_high = Column(Numeric(14, 4))
    fifty_two_week_low = Column(Numeric(14, 4))
    average_volume_50 = Column(BigInteger)
    
    # Technical Indicators (Percentages)
    price_vs_sma_10_percent = Column(Numeric(14, 4))
    price_vs_sma_21_percent = Column(Numeric(14, 4))
    price_vs_sma_50_percent = Column(Numeric(14, 4))
    price_vs_sma_150_percent = Column(Numeric(14, 4))
    price_vs_sma_200_percent = Column(Numeric(14, 4))
    
    price_vs_ema_10_percent = Column(Numeric(14, 4))
    price_vs_ema_21_percent = Column(Numeric(14, 4))
    
    percent_off_52w_high = Column(Numeric(14, 4))
    percent_off_52w_low = Column(Numeric(14, 4))
    vol_diff_50_percent = Column(Numeric(14, 4))
    
    # Moving Averages (Daily)
    sma_10 = Column(Numeric(14, 4))
    sma_21 = Column(Numeric(14, 4))
    sma_50 = Column(Numeric(14, 4))
    sma_150 = Column(Numeric(14, 4))
    sma_200 = Column(Numeric(14, 4))
    ema_10 = Column(Numeric(14, 4))
    ema_21 = Column(Numeric(14, 4))
    
    # Historical 200MA (for moving average comparisons)
    sma_200_1m_ago = Column(Numeric(14, 4))
    sma_200_2m_ago = Column(Numeric(14, 4))
    sma_200_3m_ago = Column(Numeric(14, 4))
    sma_200_4m_ago = Column(Numeric(14, 4))
    sma_200_5m_ago = Column(Numeric(14, 4))
    
    # Weekly Moving Averages
    sma_30w = Column(Numeric(14, 4))
    sma_40w = Column(Numeric(14, 4))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes للأداء
    __table_args__ = (
        Index('idx_prices_symbol_date', 'symbol', 'date', unique=True),
        Index('idx_prices_date_desc', 'date', postgresql_using='btree'),
    )
    
    def __repr__(self):
        return f"<Price(symbol={self.symbol}, date={self.date}, close={self.close})>"
