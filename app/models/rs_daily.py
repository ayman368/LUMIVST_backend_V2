from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, Boolean, Index, text
from app.core.database import Base
from datetime import datetime

class RSDaily(Base):
    """
    جدول RS اليومي - Schema محدثة
    يخزن مؤشر القوة النسبية والرتب التفصيلية
    """
    __tablename__ = "rs_daily_v2"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    
    # التقييم الرئيسي
    rs_rating = Column(Integer)               # RS Rating (1-99)
    rs_raw = Column(Numeric(10, 6))           # القيمة الخام
    
    # تصنيفات المجموعات (IBD Style Letter Ratings A+ to E)
    sector_rs_rating = Column(String(5))         # Sector Rating
    industry_group_rs_rating = Column(String(5)) # Industry Group Rating
    industry_rs_rating = Column(String(5))       # Industry Rating
    sub_industry_rs_rating = Column(String(5))   # Sub-Industry Rating
    
    # التجميع/التصريف
    acc_dis_rating = Column(String(5))           # Accumulation/Distribution Rating (A-E)
    
    # العوائد
    return_1m = Column(Numeric(10, 6))
    return_3m = Column(Numeric(10, 6))
    return_6m = Column(Numeric(10, 6))
    return_9m = Column(Numeric(10, 6))
    return_12m = Column(Numeric(10, 6))
    
    # الرتب التفصيلية (New)
    rank_1m = Column(Integer)
    rank_3m = Column(Integer)
    rank_6m = Column(Integer)
    rank_9m = Column(Integer)
    rank_12m = Column(Integer)
    
    # بيانات وصفية
    company_name = Column(String(255))
    industry_group = Column(String(255))
    
    # Generated column for filtering (Optimized)
    
    # Indexes defined in DB directly via script, but we define them here for SQLAlchemy metadata
    __table_args__ = (
        Index('idx_rs_daily_v2_symbol_date', 'symbol', 'date', unique=True),
        Index('idx_rs_daily_v2_date_rating', 'date', text('rs_rating DESC')),
        Index('idx_rs_daily_v2_date_rank_3m', 'date', text('rank_3m DESC')),
        Index('idx_rs_daily_v2_date_rank_12m', 'date', text('rank_12m DESC'))
    )
    
    def __repr__(self):
        return f"<RSDaily(symbol={self.symbol}, date={self.date}, rating={self.rs_rating})>"
