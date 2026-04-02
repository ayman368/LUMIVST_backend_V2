from sqlalchemy import Column, String, Numeric
from app.core.database import Base

class StaticStockInfo(Base):
    __tablename__ = "static_stock_info"

    symbol = Column(String(50), primary_key=True, index=True)
    approval_with_controls = Column(String(255), nullable=True)
    purge_amount = Column(Numeric(18, 6), nullable=True)
    marginable_percent = Column(Numeric(10, 4), nullable=True)
