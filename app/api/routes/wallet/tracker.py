"""
routers/tracker.py  -  /api/wallet/tracker
يستخدم بيانات الصفقات المغلقة من جدول wallet_trades لحساب احصاءات الشهرية.
"""

from datetime import date
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy.orm import Session
from app.schemas.wallet import (
    MonthlyTrackerResponse,
    MonthlyStatsRow,
    WalletTradeCreate,
    WalletTradeResponse,
)
from app.wallet.finance_logic import (
    monthly_win_rate,
    monthly_win_loss_ratio,
    monthly_adjusted_win_loss_ratio,
)
from app.models.wallet import WalletTrade
from app.models.user import User
from app.core.database import get_db
from app.api.deps import get_current_user

router = APIRouter()

MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


class ClosedTrade(BaseModel):
    month: int
    realized_pnl: float
    pnl_pct: float
    days_held: Optional[int] = 0


class TrackerRequest(BaseModel):
    year: int
    trades: list[ClosedTrade]


@router.post(
    "/calculate",
    response_model=MonthlyTrackerResponse,
    summary="احسب احصاءات كل شهر من قائمة الصفقات المغلقة",
)
def calculate_tracker(req: TrackerRequest) -> MonthlyTrackerResponse:
    monthly: dict[int, list[ClosedTrade]] = {i: [] for i in range(1, 13)}
    for trade in req.trades:
        if 1 <= trade.month <= 12:
            monthly[trade.month].append(trade)

    rows: list[MonthlyStatsRow] = []

    for month_num in range(1, 13):
        trades = monthly[month_num]
        gains = [t for t in trades if t.realized_pnl > 0]
        losses = [t for t in trades if t.realized_pnl < 0]

        total_trades = len(trades)
        trades_gain = len(gains)
        trades_loss = len(losses)

        total_gain = sum(t.realized_pnl for t in gains)
        total_loss = abs(sum(t.realized_pnl for t in losses))

        large_gain = max((t.realized_pnl for t in gains), default=0.0)
        large_loss = abs(min((t.realized_pnl for t in losses), default=0.0))

        avg_gain = (sum(t.pnl_pct for t in gains) / trades_gain) if trades_gain else 0.0
        avg_loss = (abs(sum(t.pnl_pct for t in losses)) / trades_loss) if trades_loss else 0.0

        win_pct = monthly_win_rate(trades_gain, total_trades)

        avg_days_gain = (sum(t.days_held or 0 for t in gains) / trades_gain) if trades_gain else 0.0
        avg_days_loss = (sum(t.days_held or 0 for t in losses) / trades_loss) if trades_loss else 0.0

        wl_ratio = monthly_win_loss_ratio(avg_gain, avg_loss)
        adj_ratio = monthly_adjusted_win_loss_ratio(avg_gain, win_pct, avg_loss)

        rows.append(MonthlyStatsRow(
            month=month_num,
            label=MONTH_LABELS[month_num - 1],
            investment=sum(abs(t.realized_pnl) / t.pnl_pct for t in trades if t.pnl_pct != 0),
            total_gain=total_gain,
            total_loss=total_loss,
            trades_gain=trades_gain,
            trades_loss=trades_loss,
            large_gain=large_gain,
            large_loss=large_loss,
            avg_gain=avg_gain,
            avg_loss=avg_loss,
            win_pct=win_pct,
            total_trades=total_trades,
            avg_days_gain=avg_days_gain,
            avg_days_loss=avg_days_loss,
            win_loss_ratio=wl_ratio,
            adjusted_wl_ratio=adj_ratio,
        ))

    all_gains = [t for t in req.trades if t.realized_pnl > 0]
    all_losses = [t for t in req.trades if t.realized_pnl < 0]
    total_all = len(req.trades)

    summary_wr = monthly_win_rate(len(all_gains), total_all)
    summary_avg_gain = (sum(t.pnl_pct for t in all_gains) / len(all_gains)) if all_gains else 0.0
    summary_avg_loss = (abs(sum(t.pnl_pct for t in all_losses) / len(all_losses))) if all_losses else 0.0
    summary_wl = monthly_win_loss_ratio(summary_avg_gain, summary_avg_loss)
    summary_adj = monthly_adjusted_win_loss_ratio(summary_avg_gain, summary_wr, summary_avg_loss)

    return MonthlyTrackerResponse(
        year=req.year,
        rows=rows,
        summary_win_rate=summary_wr,
        summary_avg_gain=summary_avg_gain,
        summary_avg_loss=summary_avg_loss,
        summary_wl_ratio=summary_wl,
        summary_adj_wl_ratio=summary_adj,
    )


@router.get(
    "/{year}",
    response_model=MonthlyTrackerResponse,
    summary="Aggregate closed trades from wallet_trades for the selected year",
)
def get_yearly_tracker(
    year: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MonthlyTrackerResponse:
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    trades = (
        db.query(WalletTrade)
        .filter(
            WalletTrade.user_id == current_user.id,
            WalletTrade.exit_date >= year_start,
            WalletTrade.exit_date <= year_end,
        )
        .order_by(WalletTrade.exit_date)
        .all()
    )

    monthly: dict[int, list[WalletTrade]] = {i: [] for i in range(1, 13)}
    for trade in trades:
        month = trade.exit_date.month if trade.exit_date else 0
        if 1 <= month <= 12:
            monthly[month].append(trade)

    rows: list[MonthlyStatsRow] = []
    for month_num in range(1, 13):
        trades_month = monthly[month_num]
        gains = [t for t in trades_month if float(t.realized_pnl) > 0]
        losses = [t for t in trades_month if float(t.realized_pnl) < 0]

        total_trades = len(trades_month)
        trades_gain = len(gains)
        trades_loss = len(losses)

        total_gain = sum(float(t.realized_pnl) for t in gains)
        total_loss = abs(sum(float(t.realized_pnl) for t in losses))

        large_gain = max((float(t.realized_pnl) for t in gains), default=0.0)
        large_loss = abs(min((float(t.realized_pnl) for t in losses), default=0.0))

        avg_gain = (sum(float(t.pnl_pct) for t in gains) / trades_gain) if trades_gain else 0.0
        avg_loss = (abs(sum(float(t.pnl_pct) for t in losses)) / trades_loss) if trades_loss else 0.0
        win_pct = monthly_win_rate(trades_gain, total_trades)
        avg_days_gain = (sum(t.days_held or 0 for t in gains) / trades_gain) if trades_gain else 0.0
        avg_days_loss = (sum(t.days_held or 0 for t in losses) / trades_loss) if trades_loss else 0.0

        wl_ratio = monthly_win_loss_ratio(avg_gain, avg_loss)
        adj_ratio = monthly_adjusted_win_loss_ratio(avg_gain, win_pct, avg_loss)

        rows.append(MonthlyStatsRow(
            month=month_num,
            label=MONTH_LABELS[month_num - 1],
            investment=sum(abs(float(t.realized_pnl)) / float(t.pnl_pct) for t in trades_month if float(t.pnl_pct) != 0),
            total_gain=total_gain,
            total_loss=total_loss,
            trades_gain=trades_gain,
            trades_loss=trades_loss,
            large_gain=large_gain,
            large_loss=large_loss,
            avg_gain=avg_gain,
            avg_loss=avg_loss,
            win_pct=win_pct,
            total_trades=total_trades,
            avg_days_gain=avg_days_gain,
            avg_days_loss=avg_days_loss,
            win_loss_ratio=wl_ratio,
            adjusted_wl_ratio=adj_ratio,
        ))

    all_gains = [t for t in trades if float(t.realized_pnl) > 0]
    all_losses = [t for t in trades if float(t.realized_pnl) < 0]
    total_all = len(trades)

    summary_wr = monthly_win_rate(len(all_gains), total_all)
    summary_avg_gain = (sum(float(t.pnl_pct) for t in all_gains) / len(all_gains)) if all_gains else 0.0
    summary_avg_loss = (abs(sum(float(t.pnl_pct) for t in all_losses) / len(all_losses))) if all_losses else 0.0
    summary_wl = monthly_win_loss_ratio(summary_avg_gain, summary_avg_loss)
    summary_adj = monthly_adjusted_win_loss_ratio(summary_avg_gain, summary_wr, summary_avg_loss)

    return MonthlyTrackerResponse(
        year=year,
        rows=rows,
        summary_win_rate=summary_wr,
        summary_avg_gain=summary_avg_gain,
        summary_avg_loss=summary_avg_loss,
        summary_wl_ratio=summary_wl,
        summary_adj_wl_ratio=summary_adj,
    )


@router.post(
    "/trades",
    response_model=WalletTradeResponse,
    status_code=201,
    summary="Add a closed trade to wallet_trades",
)
def create_trade(
    req: WalletTradeCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> WalletTradeResponse:
    trade = WalletTrade(
        user_id=current_user.id,
        symbol=req.symbol,
        realized_pnl=req.realized_pnl,
        pnl_pct=req.pnl_pct,
        days_held=req.days_held,
        exit_date=req.exit_date,
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    return trade


@router.post(
    "/trades/batch",
    response_model=List[WalletTradeResponse],
    status_code=201,
    summary="Add multiple closed trades to wallet_trades",
)
def create_trades(
    req: List[WalletTradeCreate],
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[WalletTradeResponse]:
    created: list[WalletTrade] = []
    for item in req:
        trade = WalletTrade(
            user_id=current_user.id,
            symbol=item.symbol,
            realized_pnl=item.realized_pnl,
            pnl_pct=item.pnl_pct,
            days_held=item.days_held,
            exit_date=item.exit_date,
        )
        db.add(trade)
        created.append(trade)
    db.commit()
    for trade in created:
        db.refresh(trade)
    return created


@router.delete(
    "/trades/all",
    status_code=204,
    summary="Delete all closed trades for the current user (reset tracker)",
)
def delete_all_trades(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    db.query(WalletTrade).filter(WalletTrade.user_id == current_user.id).delete()
    db.commit()
    return None
