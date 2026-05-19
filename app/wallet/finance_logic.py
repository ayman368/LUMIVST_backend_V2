"""
finance_logic.py
================
All quantitative formulas extracted from the Excel workbook (NEW_ADD.xlsx).
Each function is pure Python with no side effects — safe to call from any FastAPI route.

Sheet mapping:
  - Risk Finance Calculator  → risk_finance_*
  - RBAF                     → rbaf_*
  - Portfolio                → portfolio_*
  - Monthly Tracker          → monthly_tracker_*
"""

from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional


# ─────────────────────────────────────────────────────────────
#  DATA CONTAINERS  (mirror the Excel input ranges)
# ─────────────────────────────────────────────────────────────

@dataclass
class RiskFinanceInputs:
    buy_price: float        # C4  – price at which position was opened
    num_shares: float       # C5  – total shares held
    stop_price: float       # C6  – hard stop-loss price
    current_price: float    # C7  – today's market price


@dataclass
class RiskFinanceRow:
    risk_financed_pct: float      # percentage of position to finance (1.0, 0.75, 0.50, 0.25)
    shares_to_sell: float         # F-column
    effective_stop: float         # G-column  (new stop % expressed as fraction)


@dataclass
class RiskFinanceResult:
    stop_loss_pct: float          # G2 / C8
    rows: list[RiskFinanceRow]


@dataclass
class RBAFInputs:
    portfolio_size: float         # C3  – total capital in SAR
    portfolio_pct: float          # C4  – fraction of portfolio to deploy (e.g. 0.25)
    desired_return: float         # C5  – target return on portfolio (e.g. 1.0 = 100 %)
    avg_pct_gain: float           # C6  – average % gain on winning trades
    avg_pct_loss: float           # C7  – average % loss on losing trades
    win_rate: float               # C8  – historical win rate (0-1)
    risk_of_rote: float           # C9  – risk of ruin tolerance
    optimal_f: float              # F16/C12-ish - Optimal f input
    quarter_position: float       # C10 = C4/4  (derived, but kept for clarity)
    half_position: float          # C11 = C4/2
    full_position: float          # C12 = C4


@dataclass
class RBAFResult:
    # Computed intermediaries
    avg_gain_on_winners: float
    num_winning_trades: float
    avg_loss_on_losers: float
    num_losing_trades: float
    gain_loss_ratio: float
    # Position sizing
    position_size: float
    expected_net_pct_per_trade: float
    expected_net_return_per_trade: float
    # Goal
    goal: float
    trades_to_reach_goal: int
    adjusted_gain_loss_ratio: float
    optimal_f: float
    stop_loss: float
    monthly_trades_to_goal: float
    # Sized positions
    quarter_position_sar: float
    half_position_sar: float
    full_position_sar: float


@dataclass
class PortfolioPosition:
    symbol: str
    name: str
    shares_held: float           # Z column
    avg_cost: float              # I column (cost per share)
    current_price: float         # H column
    sell_price: Optional[float]  # AB column (if sold)
    stop_price: Optional[float]  # AI column
    buy_price: float             # AW column (original buy price)
    month_sold: Optional[int]    # BI column


@dataclass
class PortfolioPositionResult:
    symbol: str
    # Unrealized P&L (while still open, AC=0)
    unrealized_pnl: float        # AF3 = (current_price - avg_cost) * shares
    unrealized_pnl_pct: float    # AG3 = unrealized_pnl / cost_basis
    cost_basis: float            # AA3 = avg_cost * shares
    portfolio_weight: float      # D3  = cost_basis / total_portfolio_cost
    # Risk finance metrics
    risk_financed_100pct: float  # AM3
    risk_financed_75pct: float   # AN3
    risk_financed_50pct: float   # AO3
    risk_financed_25pct: float   # AP3
    # Effective stop after partial sell
    eff_stop_100pct: float       # AQ3
    eff_stop_75pct: float        # AR3
    eff_stop_50pct: float        # AS3
    eff_stop_25pct: float        # AT3
    # Risk sizing
    risk_pct: float              # AK3
    risk_to_reward: float        # AJ3


# ─────────────────────────────────────────────────────────────
#  RISK FINANCE CALCULATOR  (sheet: "Risk Finance Calculator")
# ─────────────────────────────────────────────────────────────

# Cell reference guide:
#   C4 = buy_price, C5 = num_shares, C6 = stop_price, C7 = current_price
#
#   G2 / C8:  stop_loss_pct  = IF(current>0, current_pct, (buy-stop)/buy)
#   F5  (Breakeven 100%):    shares_to_sell = ((buy - stop) * shares) / (current - stop)
#   G5  (Breakeven eff stop) = (buy - ((shares_sold*current + (shares-shares_sold)*stop) / shares)) / buy
#   F6,F7,F8 (75/50/25%)  :  shares_to_sell = ((buy - stop) * shares) * pct / (current - stop)
#   G6,G7,G8 (eff stop)   :  same formula as G5 with respective shares_sold


def risk_finance_stop_loss_pct(inp: RiskFinanceInputs) -> float:
    """
    Excel C8 / G2:
    The original risk percentage based on the buy price.
    Formula: (buy_price - stop_price) / buy_price
    """
    if inp.buy_price <= 0:
        return 0.0
    return (inp.buy_price - inp.stop_price) / inp.buy_price


def risk_finance_shares_to_sell(
    inp: RiskFinanceInputs,
    risk_financed_pct: float,
) -> float:
    """
    Excel F5  (100%): = ((buy - stop) * shares) / (current - stop)
    Excel F6-F8 (75/50/25%): = ((buy - stop) * shares) * pct / (current - stop)

    Unified formula: shares_to_sell = ((buy - stop) * shares * pct) / (current - stop)
    At 100%, pct = 1, which matches the breakeven formula exactly.
    """
    numerator = (inp.buy_price - inp.stop_price) * inp.num_shares * risk_financed_pct
    denominator = inp.current_price - inp.stop_price
    if denominator == 0:
        raise ValueError("current_price equals stop_price — division by zero")
    return numerator / denominator


def risk_finance_effective_stop(
    inp: RiskFinanceInputs,
    shares_sold: float,
) -> float:
    """
    Excel G5-G8:
      effective_stop = (buy - ((shares_sold*current + (shares-shares_sold)*stop) / shares)) / buy

    Gives the new effective stop % after selling `shares_sold` shares at current price.
    A value of 0 means breakeven — your remaining cost is fully covered.
    """
    weighted_avg_exit = (
        shares_sold * inp.current_price
        + (inp.num_shares - shares_sold) * inp.stop_price
    ) / inp.num_shares
    return (inp.buy_price - weighted_avg_exit) / inp.buy_price


def calculate_risk_finance(inp: RiskFinanceInputs) -> RiskFinanceResult:
    """
    Compute all four risk-finance rows (100 %, 75 %, 50 %, 25 %) plus the
    overall stop-loss percentage.
    """
    stop_pct = risk_finance_stop_loss_pct(inp)

    rows: list[RiskFinanceRow] = []
    for pct in [1.0, 0.75, 0.50, 0.25]:
        shares_sold = risk_finance_shares_to_sell(inp, pct)
        eff_stop    = risk_finance_effective_stop(inp, shares_sold)
        rows.append(RiskFinanceRow(
            risk_financed_pct=pct,
            shares_to_sell=shares_sold,
            effective_stop=eff_stop,
        ))

    return RiskFinanceResult(stop_loss_pct=stop_pct, rows=rows)


# ─────────────────────────────────────────────────────────────
#  RBAF  (Risk-Based Allocation Framework)
# ─────────────────────────────────────────────────────────────

# Cell reference guide:
#   C3=portfolio_size, C4=portfolio_pct, C5=desired_return
#   C6=avg_pct_gain,   C7=avg_pct_loss,  C8=win_rate
#   C9=risk_of_rote,   C10=C4/4,         C11=C4/2, C12=C4
#
#   F3  (avg gain on winners)     = C3 * C4 * C6
#   F4  (# winning trades)        = F14 * C8
#   F5  (avg loss on losers)      = C3 * C4 * C7
#   F6  (# losing trades)         = F14 - F4
#   F7  (gain/loss ratio)         = F3 / F5
#   F9  (position size)           = C3 * C4
#   F10 (exp net % per trade)     = (C6 - C7) * C8
#   F11 (exp net return/trade)    = F9 * F10
#   F13 (goal)                    = C3 * C5
#   F14 (trades to goal)          = ROUNDUP((C5/C4) / ((C6*C8) - (C7*(1-C8))), 0)
#   F15 (optimal f)               = (C6 * C8) / (C7 * (1 - C8))
#   F17 (stop loss)               = C7   [direct reference]
#   F18 (monthly trades to goal)  = F14 / 12
#   F20 (quarter pos SAR)         = C3 * (C4/4)
#   F21 (half pos SAR)            = C3 * (C4/2)
#   F22 (full pos SAR)            = C3 * C4


def calculate_rbaf(inp: RBAFInputs) -> RBAFResult:
    """
    Full RBAF calculation — all derived outputs from the RBAF sheet.
    """
    position_size               = inp.portfolio_size * inp.portfolio_pct          # F9
    avg_gain_on_winners         = inp.portfolio_size * inp.portfolio_pct * inp.avg_pct_gain  # F3
    avg_loss_on_losers          = inp.portfolio_size * inp.portfolio_pct * inp.avg_pct_loss  # F5
    gain_loss_ratio             = avg_gain_on_winners / avg_loss_on_losers if avg_loss_on_losers else 0.0  # F7

    expected_net_pct_per_trade  = (inp.avg_pct_gain - inp.avg_pct_loss) * inp.win_rate  # F10
    expected_net_return_per_trade = position_size * expected_net_pct_per_trade            # F11

    goal = inp.portfolio_size * inp.desired_return                                       # F13

    # F14: ROUNDUP((desired_return/portfolio_pct) / ((avg_gain*win_rate) - (avg_loss*(1-win_rate))), 0)
    edge = (inp.avg_pct_gain * inp.win_rate) - (inp.avg_pct_loss * (1 - inp.win_rate))
    if edge <= 0:
        raise ValueError("Negative expected value — system not viable")
    trades_to_goal_raw          = (inp.desired_return / inp.portfolio_pct) / edge
    trades_to_goal              = math.ceil(trades_to_goal_raw)                          # F14

    num_winning_trades          = trades_to_goal * inp.win_rate                          # F4
    num_losing_trades           = trades_to_goal - num_winning_trades                    # F6

    # F15 is actually "Adjusted Gain / Loss Ratio" in the sheet, giving 3.33
    adjusted_gain_loss_ratio    = (inp.avg_pct_gain * inp.win_rate) / (inp.avg_pct_loss * (1 - inp.win_rate))

    # F16 "Optimal f" in the sheet is 25%, which matches Portfolio Size % (C4).
    # Now it is provided as a direct input from the user.
    optimal_f                   = inp.optimal_f

    # F18
    monthly_trades_to_goal      = trades_to_goal / 12

    # F20, F21, F22
    quarter_pos_sar             = inp.portfolio_size * (inp.portfolio_pct / 4)
    half_pos_sar                = inp.portfolio_size * (inp.portfolio_pct / 2)
    full_pos_sar                = inp.portfolio_size * inp.portfolio_pct

    return RBAFResult(
        avg_gain_on_winners=avg_gain_on_winners,
        num_winning_trades=num_winning_trades,
        avg_loss_on_losers=avg_loss_on_losers,
        num_losing_trades=num_losing_trades,
        gain_loss_ratio=gain_loss_ratio,
        position_size=position_size,
        expected_net_pct_per_trade=expected_net_pct_per_trade,
        expected_net_return_per_trade=expected_net_return_per_trade,
        goal=goal,
        trades_to_reach_goal=trades_to_goal,
        adjusted_gain_loss_ratio=adjusted_gain_loss_ratio,
        optimal_f=optimal_f,
        stop_loss=inp.avg_pct_loss,
        monthly_trades_to_goal=monthly_trades_to_goal,
        quarter_position_sar=quarter_pos_sar,
        half_position_sar=half_pos_sar,
        full_position_sar=full_pos_sar,
    )


# ─────────────────────────────────────────────────────────────
#  PORTFOLIO  (sheet: "Portfolio")
# ─────────────────────────────────────────────────────────────

# Key formula translations:
#   cost_basis (AA3)   = avg_cost * shares_held
#   unrealized_pnl (AF3) = (current_price - avg_cost) * shares  [when not sold]
#   pnl_pct (AG3)      = unrealized_pnl / cost_basis
#   weight (D3)        = cost_basis / total_portfolio_cost
#   risk_pct (AK3)     = (avg_cost - stop_price) / avg_cost  [when long]
#   risk_to_reward (AJ3) = (current_price - avg_cost) / (avg_cost - stop_price)
#
#   Risk-financed shares at X% (AM/AN/AO/AP):
#     shares_at_pct = ((avg_cost - stop) * shares * pct) / (buy_price - stop)
#
#   Effective stop after partial exit (AQ/AR/AS/AT):
#     eff_stop = (avg_cost - ((shares_sold*buy_price + (shares-shares_sold)*stop) / shares)) / avg_cost


def portfolio_cost_basis(avg_cost: float, shares: float) -> float:
    """AA3 = avg_cost * shares"""
    return avg_cost * shares


def portfolio_unrealized_pnl(
    current_price: float,
    avg_cost: float,
    shares: float,
) -> float:
    """AF3 = (current_price - avg_cost) * shares  (only valid when position is open)"""
    return (current_price - avg_cost) * shares


def portfolio_unrealized_pnl_pct(unrealized_pnl: float, cost_basis: float) -> float:
    """AG3 = unrealized_pnl / cost_basis"""
    if cost_basis == 0:
        return 0.0
    return unrealized_pnl / cost_basis


def portfolio_weight(cost_basis: float, total_portfolio_cost: float) -> float:
    """D3 = cost_basis / total_portfolio_cost"""
    if total_portfolio_cost == 0:
        return 0.0
    return cost_basis / total_portfolio_cost


def portfolio_risk_pct(avg_cost: float, stop_price: float) -> float:
    """AK3 = (avg_cost - stop_price) / avg_cost  [long position]"""
    if avg_cost == 0:
        return 0.0
    return (avg_cost - stop_price) / avg_cost


def portfolio_risk_to_reward(
    current_price: float,
    avg_cost: float,
    stop_price: float,
) -> Optional[float]:
    """AJ3 = (current_price - avg_cost) / (avg_cost - stop_price)"""
    denom = avg_cost - stop_price
    if denom <= 0:
        return None
    ratio = (current_price - avg_cost) / denom
    return ratio if ratio >= 0 else None


def portfolio_risk_financed_shares(
    avg_cost: float,
    stop_price: float,
    shares: float,
    buy_price: float,
    pct: float,
) -> float:
    """
    AM3 (100%) / AN3 (75%) / AO3 (50%) / AP3 (25%):
      shares_sold = ((avg_cost - stop) * shares * pct) / (buy_price - stop)
    """
    denom = buy_price - stop_price
    if denom == 0:
        return 0.0
    return ((avg_cost - stop_price) * shares * pct) / denom


def portfolio_effective_stop_after_sell(
    avg_cost: float,
    buy_price: float,
    stop_price: float,
    shares: float,
    shares_sold: float,
) -> float:
    """
    AQ3-AT3:
      eff_stop = (avg_cost - ((shares_sold*buy_price + (shares-shares_sold)*stop) / shares)) / avg_cost
    """
    if shares == 0 or avg_cost == 0:
        return 0.0
    blended = (shares_sold * buy_price + (shares - shares_sold) * stop_price) / shares
    return (avg_cost - blended) / avg_cost


def calculate_portfolio_position(
    pos: PortfolioPosition,
    total_portfolio_cost: float,
) -> PortfolioPositionResult:
    """Compute all derived metrics for a single portfolio position."""
    cost_basis   = portfolio_cost_basis(pos.avg_cost, pos.shares_held)
    u_pnl        = portfolio_unrealized_pnl(pos.current_price, pos.avg_cost, pos.shares_held)
    u_pnl_pct    = portfolio_unrealized_pnl_pct(u_pnl, cost_basis)
    weight       = portfolio_weight(cost_basis, total_portfolio_cost)
    risk_pct     = portfolio_risk_pct(pos.avg_cost, pos.stop_price or 0.0)
    r2r          = portfolio_risk_to_reward(
        pos.current_price, pos.avg_cost, pos.stop_price or 0.0
    )

    stop = pos.stop_price or 0.0

    shares_100 = portfolio_risk_financed_shares(pos.avg_cost, stop, pos.shares_held, pos.buy_price, 1.00)
    shares_75  = portfolio_risk_financed_shares(pos.avg_cost, stop, pos.shares_held, pos.buy_price, 0.75)
    shares_50  = portfolio_risk_financed_shares(pos.avg_cost, stop, pos.shares_held, pos.buy_price, 0.50)
    shares_25  = portfolio_risk_financed_shares(pos.avg_cost, stop, pos.shares_held, pos.buy_price, 0.25)

    eff_100 = portfolio_effective_stop_after_sell(pos.avg_cost, pos.buy_price, stop, pos.shares_held, shares_100)
    eff_75  = portfolio_effective_stop_after_sell(pos.avg_cost, pos.buy_price, stop, pos.shares_held, shares_75)
    eff_50  = portfolio_effective_stop_after_sell(pos.avg_cost, pos.buy_price, stop, pos.shares_held, shares_50)
    eff_25  = portfolio_effective_stop_after_sell(pos.avg_cost, pos.buy_price, stop, pos.shares_held, shares_25)

    return PortfolioPositionResult(
        symbol=pos.symbol,
        unrealized_pnl=u_pnl,
        unrealized_pnl_pct=u_pnl_pct,
        cost_basis=cost_basis,
        portfolio_weight=weight,
        risk_financed_100pct=shares_100,
        risk_financed_75pct=shares_75,
        risk_financed_50pct=shares_50,
        risk_financed_25pct=shares_25,
        eff_stop_100pct=eff_100,
        eff_stop_75pct=eff_75,
        eff_stop_50pct=eff_50,
        eff_stop_25pct=eff_25,
        risk_pct=risk_pct,
        risk_to_reward=r2r,
    )


# ─────────────────────────────────────────────────────────────
#  MONTHLY TRACKER  (sheet: "Monthly Tracker")
# ─────────────────────────────────────────────────────────────

def monthly_win_rate(winning_trades: int, total_trades: int) -> float:
    """L = winning_trades / total_trades"""
    return winning_trades / total_trades if total_trades else 0.0


def monthly_win_loss_ratio(avg_gain: float, avg_loss: float) -> float:
    """R = avg_gain / avg_loss"""
    return avg_gain / avg_loss if avg_loss else 0.0


def monthly_adjusted_win_loss_ratio(
    avg_gain: float,
    win_rate: float,
    avg_loss: float,
) -> float:
    """
    S = (avg_gain * win_rate) / (avg_loss * (1 - win_rate))
    Excel: =IFERROR((J*L)/(K*(100%-L)),0)
    """
    denom = avg_loss * (1 - win_rate)
    return (avg_gain * win_rate) / denom if denom else 0.0
