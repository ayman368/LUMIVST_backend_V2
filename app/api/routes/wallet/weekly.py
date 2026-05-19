"""
routers/weekly.py  -  /api/wallet/weekly
يستخدم جدول wallet_weekly_studies لحفظ آخر قراءة سوقية واسترجاعها.
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from app.schemas.wallet import WeeklyStudyResponse, MarketComponent
from app.models.wallet import WalletWeeklyStudy
from app.models.user import User
from app.core.database import get_db
from app.api.deps import get_current_user

router = APIRouter()

VALID_STATUSES = {"Positive", "Neutral", "Negative"}
VALID_STEM = {"GREEN", "YELLOW", "RED"}

DEFAULT_COMPONENTS = [
    "Major Indexes",
    "UP/Down Volume",
    "New Highs/New Lows",
    "Individual Stock Participation",
]


class WeeklyStudyRequest(BaseModel):
    spy_model_25: Optional[str] = None
    spy_model_33: Optional[str] = None
    stem_reading: Optional[str] = None   # GREEN / YELLOW / RED
    stem_date: Optional[str] = None
    market_components: list[MarketComponent] = []


def build_default_weekly_response() -> WeeklyStudyResponse:
    return WeeklyStudyResponse(
        spy_model_25=None,
        spy_model_33=None,
        stem_reading=None,
        stem_date=None,
        market_components=[MarketComponent(name=c, status="Neutral") for c in DEFAULT_COMPONENTS],
    )


@router.get(
    "/latest",
    response_model=WeeklyStudyResponse,
    summary="رجع آخر قراءة للسوق",
)
def get_latest(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> WeeklyStudyResponse:
    record = (
        db.query(WalletWeeklyStudy)
        .filter(WalletWeeklyStudy.user_id == current_user.id)
        .order_by(WalletWeeklyStudy.created_at.desc())
        .first()
    )
    if not record:
        return build_default_weekly_response()

    return WeeklyStudyResponse(
        spy_model_25=record.spy_model_25,
        spy_model_33=record.spy_model_33,
        stem_reading=record.stem_reading,
        stem_date=record.stem_date.isoformat() if record.stem_date else None,
        market_components=record.market_components or [
            MarketComponent(name=c, status="Neutral") for c in DEFAULT_COMPONENTS
        ],
    )


@router.post(
    "/update",
    response_model=WeeklyStudyResponse,
    summary="حدّث قراءة السوق الأسبوعية",
)
def update_reading(
    req: WeeklyStudyRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> WeeklyStudyResponse:
    stem = req.stem_reading.upper() if req.stem_reading else None
    if stem and stem not in VALID_STEM:
        stem = None

    components = [
        MarketComponent(name=c.name, status=c.status)
        for c in req.market_components
        if c.status in VALID_STATUSES
    ]

    if not components:
        components = [
            MarketComponent(name=c, status="Neutral") for c in DEFAULT_COMPONENTS
        ]

    record = WalletWeeklyStudy(
        user_id=current_user.id,
        spy_model_25=req.spy_model_25,
        spy_model_33=req.spy_model_33,
        stem_reading=stem,
        stem_date=req.stem_date,
        market_components=[c.model_dump() for c in components],
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    return WeeklyStudyResponse(
        spy_model_25=record.spy_model_25,
        spy_model_33=record.spy_model_33,
        stem_reading=record.stem_reading,
        stem_date=record.stem_date.isoformat() if record.stem_date else None,
        market_components=record.market_components or [
            MarketComponent(name=c.name, status=c.status) for c in components
        ],
    )
