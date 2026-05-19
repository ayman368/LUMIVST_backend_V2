# app/api/routes/rs_line.py
"""
RS Line API Router
  POST /api/indicators/rs-line/       → full RS Line data + summary
  GET  /api/indicators/rs-line/latest/{symbol}  → latest summary only
"""

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from app.schemas.rs_line import RSLineRequest, RSLineResponse, RSLineSummary
from app.services.rs_line import calculate_rs_line, df_to_response
from app.core.database import get_db

router = APIRouter(prefix="/api/indicators/rs-line", tags=["RS Line"])


@router.post("/", response_model=RSLineResponse)
async def get_rs_line_slash(req: RSLineRequest, db: Session = Depends(get_db)):
    return await _get_rs_line(req, db)


@router.post("")
async def get_rs_line(req: RSLineRequest, db: Session = Depends(get_db)):
    return await _get_rs_line(req, db)


async def _get_rs_line(req: RSLineRequest, db: Session):
    """
    يحسب RS Line + MA Crossover لأي سهم سعودي
    البيانات من جداول prices + market_pulse

    Example request:
    {
        "symbol":     "2222.SR",
        "benchmark":  "^TASI.SR",
        "start_date": "2022-01-01",
        "ma1_type":   "EMA",
        "ma1_period": 8,
        "ma2_type":   "SMA",
        "ma2_period": 50
    }
    """
    try:
        end = str(req.end_date) if req.end_date else datetime.today().strftime("%Y-%m-%d")
        df = calculate_rs_line(
            db           = db,
            symbol       = req.symbol,
            benchmark    = req.benchmark,
            start_date   = str(req.start_date),
            end_date     = end,
            ma1_type     = req.ma1_type,
            ma1_period   = req.ma1_period,
            ma2_type     = req.ma2_type,
            ma2_period   = req.ma2_period,
            lookback     = req.lookback,
            scale_factor = req.scale_factor,
        )
        return df_to_response(df, req.symbol, req.benchmark)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest/{symbol}", response_model=RSLineSummary)
async def get_latest_rs_slash(symbol: str, benchmark: str = "^TASI.SR", db: Session = Depends(get_db)):
    return await _get_latest_rs(symbol, benchmark, db)


@router.get("/latest/{symbol}")
async def get_latest_rs(symbol: str, benchmark: str = "^TASI.SR", db: Session = Depends(get_db)):
    return await _get_latest_rs(symbol, benchmark, db)


async def _get_latest_rs(symbol: str, benchmark: str, db: Session):
    """
    يجيب آخر قيم RS Line بس (أسرع)
    GET /api/indicators/rs-line/latest/2222.SR
    """
    try:
        end   = datetime.today().strftime("%Y-%m-%d")
        start = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        df    = calculate_rs_line(db=db, symbol=symbol, benchmark=benchmark,
                                  start_date=start, end_date=end)
        result = df_to_response(df, symbol, benchmark)
        return result["summary"]
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
