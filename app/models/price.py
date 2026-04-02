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

    # Static / quarterly fields (update every ~3 months via Excel import)
    approval_with_controls = Column(String(150), nullable=True)
    purge_amount = Column(Numeric(18, 6), nullable=True)
    marginable_percent = Column(Numeric(10, 4), nullable=True)

    # Pure market data — technical stats moved to stock_indicators
    # NOTE: sma_*, fifty_two_week_*, average_volume_50, price_vs_sma_*,
    #        price_minus_sma_*, percent_off_52w_*, vol_diff_50_percent,
    #        sma_200_Xm_ago, sma_30w, sma_40w → now in stock_indicators

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
