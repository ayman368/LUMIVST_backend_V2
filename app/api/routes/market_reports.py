from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.core.database import get_db
from app.models.market_reports import (
    SubstantialShareholder,
    NetShortPosition,
    ForeignHeadroom,
    ShareBuyback,
    SBLPosition,
)
from app.schemas.market_reports import (
    SubstantialShareholderResponse,
    NetShortPositionResponse,
    ForeignHeadroomResponse,
    ShareBuybackResponse,
    SBLPositionResponse,
)

router = APIRouter()

@router.get("/substantial-shareholders", response_model=List[SubstantialShareholderResponse])
def get_substantial_shareholders(
    report_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(SubstantialShareholder)
    if report_date:
        query = query.filter(SubstantialShareholder.report_date == report_date)
    else:
        latest = db.query(SubstantialShareholder.report_date).order_by(desc(SubstantialShareholder.report_date)).first()
        if latest:
            query = query.filter(SubstantialShareholder.report_date == latest[0])
            
    return query.order_by(SubstantialShareholder.company_name).all()

@router.get("/net-short-positions", response_model=List[NetShortPositionResponse])
def get_net_short_positions(
    report_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(NetShortPosition)
    if report_date:
        query = query.filter(NetShortPosition.report_date == report_date)
    else:
        latest = db.query(NetShortPosition.report_date).order_by(desc(NetShortPosition.report_date)).first()
        if latest:
            query = query.filter(NetShortPosition.report_date == latest[0])
            
    return query.order_by(NetShortPosition.symbol).all()

@router.get("/foreign-headroom", response_model=List[ForeignHeadroomResponse])
def get_foreign_headroom(
    report_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(ForeignHeadroom)
    if report_date:
        query = query.filter(ForeignHeadroom.report_date == report_date)
    else:
        latest = db.query(ForeignHeadroom.report_date).order_by(desc(ForeignHeadroom.report_date)).first()
        if latest:
            query = query.filter(ForeignHeadroom.report_date == latest[0])
            
    return query.order_by(ForeignHeadroom.symbol).all()

@router.get("/share-buybacks", response_model=List[ShareBuybackResponse])
def get_share_buybacks(
    report_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(ShareBuyback)
    if report_date:
        query = query.filter(ShareBuyback.report_date == report_date)
    else:
        latest = db.query(ShareBuyback.report_date).order_by(desc(ShareBuyback.report_date)).first()
        if latest:
            query = query.filter(ShareBuyback.report_date == latest[0])
            
    return query.order_by(ShareBuyback.symbol).all()

@router.get("/sbl-positions", response_model=List[SBLPositionResponse])
def get_sbl_positions(
    report_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(SBLPosition)
    if report_date:
        query = query.filter(SBLPosition.report_date == report_date)
    else:
        latest = db.query(SBLPosition.report_date).order_by(desc(SBLPosition.report_date)).first()
        if latest:
            query = query.filter(SBLPosition.report_date == latest[0])
            
    return query.order_by(SBLPosition.symbol).all()
