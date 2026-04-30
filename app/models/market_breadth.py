"""
Market Breadth Model
Stores pre-computed % of stocks above each Moving Average by date
"""

from sqlalchemy import Column, Date, Numeric, Index
from app.core.database import Base


class MarketBreadth(Base):
    __tablename__ = "market_breadth"
    
    date = Column(Date, primary_key=True, index=True)
    pct_above_20 = Column(Numeric(5, 2), nullable=True)
    pct_above_50 = Column(Numeric(5, 2), nullable=True)
    pct_above_150 = Column(Numeric(5, 2), nullable=True)
    pct_above_200 = Column(Numeric(5, 2), nullable=True)
    
    __table_args__ = (
        Index('idx_market_breadth_date', 'date'),
    )
    
    def __repr__(self):
        return f"<MarketBreadth(date={self.date})>"
