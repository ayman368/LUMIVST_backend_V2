from sqlalchemy import Column, Integer, Numeric, String, Boolean, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from app.core.database import Base


class TasiComponent(Base):
    __tablename__ = "tasi_components"

    id = Column(Integer, primary_key=True, index=True)

    # Identity
    symbol = Column(String(10), nullable=False)
    company_name = Column(String(200), nullable=False)
    company_name_ar = Column(String(200), nullable=True)
    sector = Column(String(100), nullable=True)
    sector_ar = Column(String(100), nullable=True)

    # Market data (updated daily)
    current_price = Column(Numeric(10, 4), nullable=True)
    market_cap = Column(Numeric(20, 2), nullable=True)
    weight_in_index = Column(Numeric(10, 6), nullable=True)   # original index weight (%)
    weight_adjusted = Column(Numeric(10, 6), nullable=True)   # after applying the cap

    # Financial data
    eps = Column(Numeric(10, 4), nullable=True)               # earnings per share (SAR)
    pe_ratio = Column(Numeric(10, 2), nullable=True)
    dividend_yield = Column(Numeric(8, 4), nullable=True)

    is_active = Column(Boolean, default=True, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", name="uq_tasi_symbol"),
        Index("idx_tasi_symbol", "symbol"),
        Index("idx_tasi_sector", "sector"),
        Index("idx_tasi_weight", "weight_in_index"),
    )

    def weighted_eps(self, use_adjusted: bool = False) -> float:
        """Return the EPS contribution weighted by this component's index weight."""
        weight = float(self.weight_adjusted if use_adjusted else self.weight_in_index) or 0.0
        eps = float(self.eps) if self.eps else 0.0
        return weight * eps / 100.0   # weight is stored as percentage

    def __repr__(self):
        return f"<TasiComponent symbol={self.symbol} weight={self.weight_in_index} eps={self.eps}>"
