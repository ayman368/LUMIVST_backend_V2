"""
routers/rbaf.py  –  /api/wallet/rbaf
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas.wallet import RBAFRequest, RBAFResponse, ErrorResponse
from app.wallet.finance_logic import RBAFInputs, calculate_rbaf
from app.core.database import get_db
from app.models.wallet import WalletSetting
from app.models.user import User
from app.api.deps import get_current_user

router = APIRouter()
RBAF_SETTINGS_KEY = "rbaf_defaults"


def persist_rbaf_settings(db: Session, req: RBAFRequest, user_id: int) -> None:
    values = req.model_dump()
    existing = db.query(WalletSetting).filter(
        WalletSetting.key == RBAF_SETTINGS_KEY,
        WalletSetting.user_id == user_id,
    ).first()
    if existing:
        existing.value = values
    else:
        existing = WalletSetting(key=RBAF_SETTINGS_KEY, user_id=user_id, value=values)
        db.add(existing)
    db.commit()


@router.post(
    "/calculate",
    response_model=RBAFResponse,
    responses={422: {"model": ErrorResponse}},
    summary="Run Risk-Based Allocation Framework — returns Optimal f, position sizing, trade targets",
)
def calculate(
    req: RBAFRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> RBAFResponse:
    try:
        inp = RBAFInputs(
            portfolio_size=req.portfolio_size,
            portfolio_pct=req.portfolio_pct,
            desired_return=req.desired_return,
            avg_pct_gain=req.avg_pct_gain,
            avg_pct_loss=req.avg_pct_loss,
            win_rate=req.win_rate,
            risk_of_rote=req.risk_of_rote,
            optimal_f=req.optimal_f,
            quarter_position=req.portfolio_pct / 4,
            half_position=req.portfolio_pct / 2,
            full_position=req.portfolio_pct,
        )
        result = calculate_rbaf(inp)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    persist_rbaf_settings(db, req, current_user.id)
    return RBAFResponse(**result.__dict__)


@router.get(
    "/settings",
    response_model=RBAFRequest,
    summary="Get persisted RBAF settings",
)
def get_settings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    setting = db.query(WalletSetting).filter(
        WalletSetting.key == RBAF_SETTINGS_KEY,
        WalletSetting.user_id == current_user.id,
    ).first()
    if setting and setting.value:
        return RBAFRequest(**setting.value)

    # Return defaults if nothing saved
    return RBAFRequest(
        portfolio_size=700000,
        portfolio_pct=0.25,
        desired_return=1.0,
        avg_pct_gain=0.20,
        avg_pct_loss=0.04,
        win_rate=0.40,
        risk_of_rote=0.01,
        optimal_f=0.25,
    )
