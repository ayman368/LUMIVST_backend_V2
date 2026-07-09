from sqlalchemy import Column, Integer, Numeric, String, DateTime
from sqlalchemy.sql import func
from app.core.database import Base


class ValuationZone(Base):
    __tablename__ = "valuation_zones"

    id = Column(Integer, primary_key=True, index=True)
    label = Column(String(100), nullable=False)
    label_ar = Column(String(100), nullable=True)
    price_from = Column(Numeric(10, 2), nullable=False)
    price_to = Column(Numeric(10, 2), nullable=False)
    return_pct_low = Column(Integer, nullable=True)
    return_pct_high = Column(Integer, nullable=True)
    color_code = Column(String(20), nullable=True)   # 'green', 'yellow', 'orange', 'red'
    description = Column(String(500), nullable=True)
    order_seq = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def contains_price(self, price: float) -> bool:
        """Return True if price falls within this zone."""
        return float(self.price_from) <= price < float(self.price_to)

    def __repr__(self):
        return f"<ValuationZone label={self.label} from={self.price_from} to={self.price_to}>"
