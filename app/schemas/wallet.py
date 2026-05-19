"""
schemas.py
==========
Pydantic v2 request / response models for all 5 sheets.
"""

from __future__ import annotations
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field, model_validator


# ─────────────────────────────────────────────────────────────
#  RISK FINANCE CALCULATOR
# ─────────────────────────────────────────────────────────────

class RiskFinanceRequest(BaseModel):
    buy_price: float    = Field(..., gt=0, description="Original buy price per share")
    num_shares: float   = Field(..., gt=0, description="Total shares held")
    stop_price: float   = Field(..., gt=0, description="Hard stop-loss price")
    current_price: float= Field(..., gt=0, description="Current market price")

    @model_validator(mode="after")
    def validate_prices(self) -> "RiskFinanceRequest":
        if self.stop_price >= self.buy_price:
            raise ValueError("stop_price must be below buy_price")
        return self


class RiskFinanceRowResponse(BaseModel):
    risk_financed_pct: float   = Field(..., description="Percentage of gain to lock in (1.0, 0.75, 0.50, 0.25)")
    shares_to_sell: float      = Field(..., description="Number of shares to sell")
    effective_stop: float      = Field(..., description="New effective stop % after partial exit")


class RiskFinanceResponse(BaseModel):
    stop_loss_pct: float                    = Field(..., description="Current stop-loss as % of price")
    rows: list[RiskFinanceRowResponse]


# ─────────────────────────────────────────────────────────────
#  RBAF
# ─────────────────────────────────────────────────────────────

class RBAFRequest(BaseModel):
    portfolio_size: float   = Field(..., gt=0, description="Total portfolio capital in SAR")
    portfolio_pct: float    = Field(..., gt=0, le=1, description="Fraction of portfolio to deploy")
    desired_return: float   = Field(..., gt=0, description="Target return multiplier (e.g. 1.0 = 100 %)")
    avg_pct_gain: float     = Field(..., gt=0, description="Average % gain on winning trades")
    avg_pct_loss: float     = Field(..., gt=0, description="Average % loss on losing trades")
    win_rate: float         = Field(..., gt=0, lt=1, description="Historical win rate (0-1)")
    risk_of_rote: float     = Field(0.01, description="Risk of ruin tolerance")
    optimal_f: float        = Field(0.25, description="Optimal f input")


class RBAFResponse(BaseModel):
    avg_gain_on_winners: float
    num_winning_trades: float
    avg_loss_on_losers: float
    num_losing_trades: float
    gain_loss_ratio: float
    position_size: float
    expected_net_pct_per_trade: float
    expected_net_return_per_trade: float
    goal: float
    trades_to_reach_goal: int
    adjusted_gain_loss_ratio: float
    optimal_f: float
    stop_loss: float
    monthly_trades_to_goal: float
    quarter_position_sar: float
    half_position_sar: float
    full_position_sar: float


# ─────────────────────────────────────────────────────────────
#  PORTFOLIO
# ─────────────────────────────────────────────────────────────

class PortfolioPositionIn(BaseModel):
    symbol: str
    name: str
    shares_held: float   = Field(..., ge=0)
    avg_cost: float      = Field(..., ge=0)
    current_price: float = Field(..., ge=0)
    buy_price: float     = Field(..., ge=0)
    stop_price: Optional[float]  = None
    sell_price: Optional[float]  = None
    month_sold: Optional[int]    = None


class PortfolioRequest(BaseModel):
    positions: list[PortfolioPositionIn]


class PortfolioPositionDBCreate(BaseModel):
    symbol: str
    name: str
    qty: float = Field(..., ge=0)
    buy_price: float = Field(..., ge=0)
    stop_price: Optional[float] = None
    portfolio_name: Optional[str] = Field("Default")
    entry_date: Optional[date] = None


class PortfolioPositionDBUpdate(BaseModel):
    name: Optional[str] = None
    qty: Optional[float] = None
    buy_price: Optional[float] = None
    stop_price: Optional[float] = None
    portfolio_name: Optional[str] = None
    entry_date: Optional[date] = None


class PortfolioPositionDB(BaseModel):
    id: int
    symbol: str
    name: Optional[str]
    qty: float
    buy_price: float
    stop_price: Optional[float]
    current_price: Optional[float] = None
    portfolio_name: str
    entry_date: date
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class PortfolioPositionOut(BaseModel):
    symbol: str
    name: str
    cost_basis: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    portfolio_weight: float
    risk_pct: float
    risk_to_reward: Optional[float]
    risk_financed_100pct: float
    risk_financed_75pct: float
    risk_financed_50pct: float
    risk_financed_25pct: float
    eff_stop_100pct: float
    eff_stop_75pct: float
    eff_stop_50pct: float
    eff_stop_25pct: float


class PortfolioSummary(BaseModel):
    total_cost_basis: float
    total_unrealized_pnl: float
    total_unrealized_pnl_pct: float
    num_positions: int
    positions: list[PortfolioPositionOut]


# ─────────────────────────────────────────────────────────────
#  MONTHLY TRACKER
# ─────────────────────────────────────────────────────────────

class WalletTradeCreate(BaseModel):
    symbol: str
    realized_pnl: float
    pnl_pct: float
    days_held: int = Field(0, ge=0)
    exit_date: Optional[date] = None


class WalletTradeResponse(BaseModel):
    id: int
    symbol: str
    realized_pnl: float
    pnl_pct: float
    days_held: int
    exit_date: date
    created_at: datetime

    model_config = {"from_attributes": True}


class MonthlyStatsRow(BaseModel):
    month: int           = Field(..., ge=1, le=13, description="1-12 for months, 13 for full portfolio")
    label: str
    investment: float    = Field(0.0)
    total_gain: float    = Field(0.0)
    total_loss: float    = Field(0.0)
    trades_gain: int     = Field(0)
    trades_loss: int     = Field(0)
    large_gain: float    = Field(0.0)
    large_loss: float    = Field(0.0)
    avg_gain: float      = Field(0.0)
    avg_loss: float      = Field(0.0)
    win_pct: float       = Field(0.0)
    total_trades: int    = Field(0)
    avg_days_gain: float = Field(0.0)
    avg_days_loss: float = Field(0.0)
    win_loss_ratio: float= Field(0.0)
    adjusted_wl_ratio: float = Field(0.0)


class MonthlyTrackerResponse(BaseModel):
    year: int
    rows: list[MonthlyStatsRow]
    summary_win_rate: float
    summary_avg_gain: float
    summary_avg_loss: float
    summary_wl_ratio: float
    summary_adj_wl_ratio: float


# ─────────────────────────────────────────────────────────────
#  WEEKLY STUDY
# ─────────────────────────────────────────────────────────────

class MarketComponent(BaseModel):
    name: str
    status: str   # "Positive" | "Neutral" | "Negative"


class WeeklyStudyResponse(BaseModel):
    spy_model_25: Optional[str]
    spy_model_33: Optional[str]
    stem_reading: Optional[str]
    stem_date: Optional[str]
    market_components: list[MarketComponent]


# ─────────────────────────────────────────────────────────────
#  SHARED / ERROR
# ─────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    detail: str
