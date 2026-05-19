# app/api/routes/sata.py
"""
SATA & Stage Analysis API endpoints.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.sata import calculate_sata, df_to_response

router = APIRouter()


@router.get("")
def get_sata(
    symbol: str = Query(..., example="2222.SR"),
    benchmark: str = Query("^TASI.SR"),
    start_date: str = Query("2018-01-01"),
    end_date: str = Query(None),
    db: Session = Depends(get_db),
):
    """
    SATA (Stage Analysis Technical Attributes) — 10-band scoring system.

    Returns:
      - summary: current score (0-10), stage, and individual band status
      - data: weekly historical scores for charting
    """
    try:
        clean_symbol = symbol.replace(".SR", "")
        df = calculate_sata(
            db=db,
            symbol=clean_symbol,
            benchmark=benchmark,
            start_date=start_date,
            end_date=end_date,
        )
        return df_to_response(df, symbol, benchmark)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latest/{symbol}")
def get_latest_sata(
    symbol: str,
    benchmark: str = Query("^TASI.SR"),
    db: Session = Depends(get_db),
):
    """
    Latest SATA score + stage for a symbol.
    GET /api/indicators/sata/latest/2222.SR
    """
    try:
        clean_symbol = symbol.replace(".SR", "")
        df = calculate_sata(
            db=db,
            symbol=clean_symbol,
            benchmark=benchmark,
            start_date="2020-01-01",
        )
        result = df_to_response(df, symbol, benchmark)
        return result["summary"]
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
