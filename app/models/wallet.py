from datetime import date
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func, text
from app.core.database import Base


class WalletPosition(Base):
    __tablename__ = "wallet_positions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    name = Column(String(255), nullable=True)
    qty = Column(Numeric(18, 4), nullable=False, default=0)
    buy_price = Column(Numeric(18, 4), nullable=False, default=0)
    stop_price = Column(Numeric(18, 4), nullable=True)
    portfolio_name = Column(String(100), nullable=False, default="Default")
    entry_date = Column(Date, nullable=False, server_default=func.current_date())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class WalletTrade(Base):
    __tablename__ = "wallet_trades"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    realized_pnl = Column(Numeric(18, 4), nullable=False)
    pnl_pct = Column(Numeric(18, 6), nullable=False)
    days_held = Column(Integer, nullable=False, default=0)
    exit_date = Column(Date, nullable=False, server_default=func.current_date())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class WalletSetting(Base):
    __tablename__ = "wallet_settings"

    key = Column(String(100), primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True, nullable=False, index=True)
    value = Column(JSONB, nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class WalletWeeklyStudy(Base):
    __tablename__ = "wallet_weekly_studies"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    stem_reading = Column(String(20), nullable=True)
    stem_date = Column(Date, nullable=False, server_default=func.current_date())
    spy_model_25 = Column(String(255), nullable=True)
    spy_model_33 = Column(String(255), nullable=True)
    market_components = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
