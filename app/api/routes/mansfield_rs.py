from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.services.mansfield_rs import calculate_mansfield_rs, df_to_response

router = APIRouter()

@router.get("/")
@router.get("")
def get_mansfield_rs(
    symbol: str = Query(..., description="Stock symbol (e.g., 2222)"),
    benchmark: str = Query("^TASI.SR"),
    start_date: str = Query("2018-01-01"),
    end_date: str = Query(None),
    ma_length: int = Query(52),
    db: Session = Depends(get_db)
):
    try:
        # إزالة .SR لو موجودة عشان تناسب الـ Database
        clean_symbol = symbol.replace(".SR", "")
        df = calculate_mansfield_rs(
            db=db,
            symbol=clean_symbol,
            benchmark=benchmark,
            start_date=start_date,
            end_date=end_date,
            ma_length=ma_length
        )
        return df_to_response(df, symbol, benchmark, ma_length)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest/{symbol}")
def get_latest_mansfield_rs(
    symbol: str,
    benchmark: str = Query("^TASI.SR"),
    db: Session = Depends(get_db)
):
    try:
        clean_symbol = symbol.replace(".SR", "")
        df = calculate_mansfield_rs(
            db=db,
            symbol=clean_symbol,
            benchmark=benchmark,
            start_date="2018-01-01",
            end_date=None,
            ma_length=52
        )
        data = df_to_response(df, symbol, benchmark, 52)
        return {"success": True, "data": data["summary"]}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
