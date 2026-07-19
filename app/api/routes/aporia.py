from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import os
import csv
from app.api.deps import get_current_user
from app.core.database import get_db
from app.models.aporia import AporiaAnalytics, AporiaChart
from app.schemas.aporia import AporiaAnalyticsResponse
from app.api.deps import get_current_user

router = APIRouter()

# The directory where aporia scraper saves its output
APORIA_OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "aporia_out")
APORIA_CHARTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "aporia_charts")

VALID_FILTERS = {
    "all_analytics": "all_metrics",
    "largest_market_cap": "largest",
    "strongest_uptrends": "strongest_uptrends",
    "strongest_downtrends": "strongest_downtrends",
    "breakouts": "breakouts",
    "consolidations": "consolidations"
}

@router.get("/saudi-analytics", response_model=List[AporiaAnalyticsResponse])
async def get_aporia_data(filter_by: str = "all_analytics", db: Session = Depends(get_db), _: Any = Depends(get_current_user)):
    """
    Returns the parsed Saudi stock analytics data for the given filter from DB.
    """
    if filter_by not in VALID_FILTERS:
        raise HTTPException(status_code=400, detail=f"Invalid filter. Must be one of {list(VALID_FILTERS.keys())}")
    
    db_filter = VALID_FILTERS[filter_by]
    
    try:
        records = db.query(AporiaAnalytics).filter(AporiaAnalytics.filter_category == db_filter).all()
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data from database: {str(e)}")

from fastapi.responses import JSONResponse

@router.get("/chart/{ticker}")
async def get_aporia_chart(ticker: str, chart_type: str = "trend", db: Session = Depends(get_db), _: Any = Depends(get_current_user)):
    """
    Returns the Highcharts JSON configuration for the requested ticker and chart type from DB.
    """
    valid_chart_types = ["trend", "breakout", "longest_consolidation_window", "volume", "price_extreme"]
    if chart_type not in valid_chart_types:
        raise HTTPException(status_code=400, detail=f"Invalid chart_type. Must be one of {valid_chart_types}")
        
    try:
        record = db.query(AporiaChart).filter(AporiaChart.ticker == ticker, AporiaChart.chart_type == chart_type).first()
        if not record:
            raise HTTPException(status_code=404, detail="Chart data not found in database. Run Scrape_aporia_charts.py for this ticker.")
        
        return JSONResponse(content=record.chart_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading chart from database: {str(e)}")
