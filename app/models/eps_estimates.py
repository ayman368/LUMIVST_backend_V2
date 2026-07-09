from sqlalchemy import Column, Integer, Numeric, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from app.core.database import Base


class EpsEstimate(Base):
    __tablename__ = "eps_estimates"

    id = Column(Integer, primary_key=True, index=True)
    year = Column(Integer, nullable=False)
    value = Column(Numeric(10, 2), nullable=False)
    type = Column(String(20), nullable=True)       # 'actual' or 'estimate'
    source = Column(String(100), nullable=True)    # 'Yardeni', 'FactSet', 'S&P', etc.
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by = Column(String(100), nullable=True)

    __table_args__ = (
        UniqueConstraint("year", name="uq_eps_year"),
    )

    def __repr__(self):
        return f"<EpsEstimate year={self.year} value={self.value} type={self.type}>"
