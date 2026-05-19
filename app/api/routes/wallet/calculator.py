"""
routers/calculator.py  –  /api/wallet/calculator
"""

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.schemas.wallet import RiskFinanceRequest, RiskFinanceResponse, RiskFinanceRowResponse, ErrorResponse
from app.wallet.finance_logic import (
    RiskFinanceInputs,
    calculate_risk_finance,
)
from app.models.price import Price
from app.core.database import get_db

router = APIRouter()


class PriceResponse(BaseModel):
    symbol: str
    close: float
    date: str


@router.post(
    "/calculate",
    response_model=RiskFinanceResponse,
    responses={422: {"model": ErrorResponse}},
    summary="Calculate risk-financed shares to sell at 4 levels (100/75/50/25 %)",
)
def calculate(req: RiskFinanceRequest) -> RiskFinanceResponse:
    try:
        inp = RiskFinanceInputs(
            buy_price=req.buy_price,
            num_shares=req.num_shares,
            stop_price=req.stop_price,
            current_price=req.current_price,
        )
        result = calculate_risk_finance(inp)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return RiskFinanceResponse(
        stop_loss_pct=result.stop_loss_pct,
        rows=[
            RiskFinanceRowResponse(
                risk_financed_pct=r.risk_financed_pct,
                shares_to_sell=r.shares_to_sell,
                effective_stop=r.effective_stop,
            )
            for r in result.rows
        ],
    )


@router.get(
    "/price/{symbol}",
    response_model=PriceResponse,
    summary="Fetch the latest closing price for a symbol",
)
def get_latest_price(symbol: str, db: Session = Depends(get_db)) -> PriceResponse:
    latest_price = db.query(Price).filter(
        Price.symbol == symbol.upper()
    ).order_by(Price.date.desc()).first()
    
    if not latest_price:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No price data found for symbol {symbol}"
        )
    
    return PriceResponse(
        symbol=latest_price.symbol,
        close=float(latest_price.close),
        date=latest_price.date.isoformat(),
    )
