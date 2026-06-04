"""
routers/portfolio.py  –  /api/wallet/portfolio
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from app.schemas.wallet import (
    PortfolioPositionDB,
    PortfolioPositionDBCreate,
    PortfolioPositionDBUpdate,
    PortfolioPositionAddRequest,
    PortfolioPositionSellRequest,
)
from app.models.wallet import WalletPosition
from app.models.price import Price
from app.models.stock_indicators import StockIndicator
from app.models.market_reports import NetShortPosition
from app.models.static_stock_info import StaticStockInfo
from app.models.user import User
from app.core.database import get_db
from app.api.deps import get_current_user

router = APIRouter()


@router.get(
    "/positions",
    response_model=List[dict],
    summary="Retrieve all active wallet positions with latest market prices and indicators",
)
def list_positions(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[dict]:
    positions = (
        db.query(WalletPosition)
        .filter(WalletPosition.user_id == current_user.id)
        .order_by(WalletPosition.created_at.desc())
        .all()
    )

    result = []
    for pos in positions:
        # Latest price record
        latest_price = db.query(Price).filter(
            Price.symbol == pos.symbol
        ).order_by(Price.date.desc()).first()

        # Latest indicators record
        latest_ind = db.query(StockIndicator).filter(
            StockIndicator.symbol == pos.symbol
        ).order_by(StockIndicator.date.desc()).first()

        # Latest short position record
        latest_short = db.query(NetShortPosition).filter(
            NetShortPosition.symbol == pos.symbol
        ).order_by(NetShortPosition.report_date.desc()).first()

        # Static info (including marginable_percent)
        static_info = db.query(StaticStockInfo).filter(
            StaticStockInfo.symbol == pos.symbol
        ).first()

        current_price = float(latest_price.close) if latest_price else float(pos.buy_price)
        prev_close = float(latest_price.open) if latest_price and latest_price.open else current_price
        change_pct = float(latest_price.change_percent) / 100 if latest_price and latest_price.change_percent else 0.0

        # Get marginable_percent from StaticStockInfo (if available), otherwise from prices table
        marginable_percent = None
        if static_info and static_info.marginable_percent:
            marginable_percent = float(static_info.marginable_percent)
        elif latest_price and latest_price.marginable_percent:
            marginable_percent = float(latest_price.marginable_percent)

        pos_dict = {
            "id": pos.id,
            "symbol": pos.symbol,
            "name": pos.name or (latest_price.company_name if latest_price and latest_price.company_name else ""),
            "qty": float(pos.qty),
            "buy_price": float(pos.buy_price),
            "stop_price": float(pos.stop_price) if pos.stop_price else None,
            "current_price": current_price,
            "portfolio_name": pos.portfolio_name,
            "entry_date": pos.entry_date.isoformat() if pos.entry_date else None,
            "created_at": pos.created_at.isoformat() if pos.created_at else None,
            "updated_at": pos.updated_at.isoformat() if pos.updated_at else None,
            # Enriched from prices table
            "sector": latest_price.sector if latest_price else None,
            "industry_group": latest_price.industry_group if latest_price else None,
            "change_percent": change_pct,
            "marginable_percent": marginable_percent,
            "short_percent": float(latest_short.percent_over_free_float) if latest_short and latest_short.percent_over_free_float else None,
            # Enriched from stock_indicators table
            "percent_change_20d": float(latest_ind.percent_change_20d) if latest_ind and latest_ind.percent_change_20d else None,
            "percent_change_63d": float(latest_ind.percent_change_63d) if latest_ind and latest_ind.percent_change_63d else None,
            "percent_change_126d": float(latest_ind.percent_change_126d) if latest_ind and latest_ind.percent_change_126d else None,
            "percent_change_15d": float(latest_ind.percent_change_15d) if latest_ind and latest_ind.percent_change_15d else None,
            "sma_150": float(latest_ind.sma_150) if latest_ind and latest_ind.sma_150 else None,
            "trend_signal": bool(latest_ind.trend_signal) if latest_ind else False,
            "final_signal": bool(latest_ind.final_signal) if latest_ind else False,
            "score": int(latest_ind.score) if latest_ind and latest_ind.score else 0,
        }
        result.append(pos_dict)

    return result


@router.post(
    "/positions",
    response_model=PortfolioPositionDB,
    status_code=status.HTTP_201_CREATED,
    summary="Add a new wallet position",
)
def create_position(
    req: PortfolioPositionDBCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PortfolioPositionDB:
    position = WalletPosition(
        user_id=current_user.id,
        symbol=req.symbol,
        name=req.name,
        qty=req.qty,
        buy_price=req.buy_price,
        stop_price=req.stop_price,
        portfolio_name=req.portfolio_name or "Default",
        entry_date=req.entry_date,
    )
    db.add(position)
    db.commit()
    db.refresh(position)
    return position


@router.get(
    "/positions/{position_id}",
    response_model=PortfolioPositionDB,
    summary="Retrieve a wallet position by ID",
)
def get_position(
    position_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PortfolioPositionDB:
    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")
    return position


@router.put(
    "/positions/{position_id}",
    response_model=PortfolioPositionDB,
    summary="Update an existing wallet position",
)
def update_position(
    position_id: int,
    req: PortfolioPositionDBUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> PortfolioPositionDB:
    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")

    for field, value in req.model_dump(exclude_unset=True).items():
        setattr(position, field, value)

    db.commit()
    db.refresh(position)
    return position


@router.delete(
    "/positions/{position_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a wallet position",
)
def delete_position(
    position_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")
    db.delete(position)
    db.commit()
    return None


@router.post(
    "/positions/{position_id}/close",
    summary="Close a position and move it to the tracker",
)
def close_position(
    position_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.models.wallet import WalletTrade
    from datetime import date

    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")

    latest_price = db.query(Price).filter(Price.symbol == position.symbol).order_by(Price.date.desc()).first()
    current_price = float(latest_price.close) if latest_price else float(position.buy_price)

    qty = float(position.qty)
    buy_price = float(position.buy_price)

    realized_pnl = (current_price - buy_price) * qty
    pnl_pct = realized_pnl / (buy_price * qty) if buy_price * qty > 0 else 0.0

    entry_date = position.entry_date or date.today()
    exit_date = date.today()
    days_held = max((exit_date - entry_date).days, 0)

    trade = WalletTrade(
        user_id=current_user.id,
        symbol=position.symbol,
        realized_pnl=realized_pnl,
        pnl_pct=pnl_pct,
        days_held=days_held,
        exit_date=exit_date,
    )

    db.add(trade)
    db.delete(position)
    db.commit()

    return {"message": "Position closed successfully", "realized_pnl": realized_pnl}


@router.post(
    "/positions/{position_id}/add",
    response_model=PortfolioPositionDB,
    summary="Add shares to an existing position (Scaling in)",
)
def add_shares_to_position(
    position_id: int,
    req: PortfolioPositionAddRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")

    old_qty = float(position.qty)
    old_price = float(position.buy_price)
    new_qty = req.qty
    new_price = req.buy_price

    total_qty = old_qty + new_qty
    weighted_price = ((old_qty * old_price) + (new_qty * new_price)) / total_qty

    position.qty = total_qty
    position.buy_price = weighted_price

    # record transaction
    txs = list(position.transactions) if position.transactions else []
    txs.append({
        "type": "add",
        "date": req.trade_date.isoformat(),
        "qty": new_qty,
        "price": new_price
    })
    position.transactions = txs

    db.commit()
    db.refresh(position)
    return position


@router.post(
    "/positions/{position_id}/sell",
    summary="Partial sell shares from an existing position",
)
def partial_sell_position(
    position_id: int,
    req: PortfolioPositionSellRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.models.wallet import WalletTrade
    from datetime import date

    position = (
        db.query(WalletPosition)
        .filter(WalletPosition.id == position_id, WalletPosition.user_id == current_user.id)
        .first()
    )
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")

    old_qty = float(position.qty)
    sell_qty = req.qty

    if sell_qty > old_qty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot sell more than held quantity")

    buy_price = float(position.buy_price)
    realized_pnl = (req.sell_price - buy_price) * sell_qty
    pnl_pct = realized_pnl / (buy_price * sell_qty) if buy_price * sell_qty > 0 else 0.0

    entry_date = position.entry_date or date.today()
    days_held = max((req.trade_date - entry_date).days, 0)

    # create a wallet trade for the sold part
    trade = WalletTrade(
        user_id=current_user.id,
        symbol=position.symbol,
        realized_pnl=realized_pnl,
        pnl_pct=pnl_pct,
        days_held=days_held,
        exit_date=req.trade_date,
    )
    db.add(trade)

    if sell_qty == old_qty:
        db.delete(position)
        db.commit()
        return {"message": "Position fully closed", "realized_pnl": realized_pnl, "closed": True}
    else:
        position.qty = old_qty - sell_qty
        txs = list(position.transactions) if position.transactions else []
        txs.append({
            "type": "sell",
            "date": req.trade_date.isoformat(),
            "qty": sell_qty,
            "price": req.sell_price,
            "pnl": realized_pnl
        })
        position.transactions = txs
        db.commit()
        db.refresh(position)
        return {"message": "Position partially sold", "realized_pnl": realized_pnl, "closed": False}
