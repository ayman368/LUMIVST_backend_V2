from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc
import threading
import logging

from app.core.security import verify_internal_key
from scripts.update_market_reports import main as update_market_reports_main

logger = logging.getLogger(__name__)

from app.core.database import get_db
from app.models.market_reports import (
    SubstantialShareholder,
    NetShortPosition,
    ForeignHeadroom,
    ShareBuyback,
    SBLPosition,
    HistoricalReport,
)
from app.schemas.market_reports import (
    SubstantialShareholderResponse,
    NetShortPositionResponse,
    ForeignHeadroomResponse,
    ShareBuybackResponse,
    SBLPositionResponse,
    HistoricalReportResponse,
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

@router.get("/historical-reports", response_model=List[HistoricalReportResponse])
def get_historical_reports(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    query = db.query(HistoricalReport)
    if start_date:
        query = query.filter(HistoricalReport.report_date >= start_date)
    if end_date:
        query = query.filter(HistoricalReport.report_date <= end_date)
            
    return query.order_by(desc(HistoricalReport.report_date)).all()

@router.get("/scrape")
def trigger_scrape_market_reports(_: bool = Depends(verify_internal_key)):
    def scrape_with_error_handling():
        try:
            logger.info("Starting market reports scraping via API trigger")
            update_market_reports_main()
            logger.info("Market reports scraping completed successfully")
        except Exception as e:
            logger.error(f"Market reports scraping failed: {e}")
            
    thread = threading.Thread(target=scrape_with_error_handling, daemon=True)
    thread.start()
    return {"message": "Market reports scraping started in background"}


@router.get("/scrape/daily-financial-indicators")
def trigger_scrape_daily_financial_indicators(_: bool = Depends(verify_internal_key)):
    def scrape_with_error_handling():
        try:
            logger.info("Starting Daily Financial Indicators scraping via API trigger")
            from app.scrapers.daily_financial_indicators_scraper import run_scraper_and_save_to_db
            run_scraper_and_save_to_db()
            logger.info("Daily Financial Indicators scraping completed successfully")
        except Exception as e:
            logger.error(f"Daily Financial Indicators scraping failed: {e}")

    thread = threading.Thread(target=scrape_with_error_handling, daemon=True)
    thread.start()
    return {"message": "Daily Financial Indicators scraping started in background"}

