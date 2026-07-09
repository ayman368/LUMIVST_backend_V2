"""
Load prices + stock_indicators + TASI index data into pandas DataFrames
for the weekly report calculators.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.market_pulse import MarketPulse
from app.models.market_reports import HistoricalReport
from app.models.price import Price
from app.models.stock_indicators import StockIndicator
from app.models.tasi_components import TasiComponent

logger = logging.getLogger(__name__)

EXCLUDED_SYMBOLS = {"TASI", "TASI.SR", "^TASI", "TASI.CM", "NOMUC"}


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        value = value.replace(",", "").strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def trading_week_bounds(week_end: date) -> tuple[date, date]:
    """Return (Sunday, Thursday) of the most recent *complete* Saudi trading week.

    Saudi trading week runs Sunday → Thursday.
    If ``week_end`` falls on Fri/Sat/Sun/Mon/Tue/Wed the function snaps back
    to the last Thursday that has already passed, then derives Sunday as
    four days before that Thursday.

    Examples (2026-06-14 is a Sunday):
        trading_week_bounds(date(2026, 6, 14))  → (Jun 7, Jun 11)   # prev week
        trading_week_bounds(date(2026, 6, 11))  → (Jun 7, Jun 11)   # Thu
        trading_week_bounds(date(2026, 6, 13))  → (Jun 7, Jun 11)   # Sat
    """
    wd = week_end.weekday()  # Mon=0 … Sun=6
    # How many days since the most recent Thursday (weekday 3)?
    days_since_thu = (wd - 3) % 7
    last_thu = week_end - timedelta(days=days_since_thu)
    # Sunday of that same trading week is 4 days before Thursday
    week_start = last_thu - timedelta(days=4)
    return week_start, last_thu


def resolve_week_end(db: Session, week_end: date | None = None) -> date:
    if week_end is not None:
        return week_end
    latest = db.query(func.max(Price.date)).scalar()
    if not latest:
        raise ValueError("No price data in database")
    return latest


def _pct_below_high(raw: Optional[float]) -> Optional[float]:
    """Convert stored percent_off_52w_high (negative below high) to positive % below."""
    if raw is None:
        return None
    val = float(raw)
    if val <= 0:
        return round(abs(val), 2)
    return round(val, 2)


def _compute_weekly_fields(closes: pd.Series) -> pd.DataFrame:
    """Resample daily closes to weekly and forward-fill weekly SMA fields."""
    s = closes.copy()
    s.index = pd.to_datetime(s.index)
    weekly = s.resample("W-SUN").last().dropna()
    out = pd.DataFrame({"close_w": weekly})
    out["sma9_w"] = out["close_w"].rolling(9, min_periods=1).mean()
    out["sma_trend_weekly"] = out["close_w"].rolling(26, min_periods=1).mean()
    return out


from datetime import timedelta, date

def load_stocks_dataframe(db: Session, week_end: date = None) -> pd.DataFrame:
    """Load stock history joined with indicators (excludes index/ETF rows)."""
    logger.info("Loading stock history for weekly report…")

    valid_symbols = db.query(StockIndicator.symbol).filter(StockIndicator.is_etf_or_index.is_(False)).distinct()

    query = (
        db.query(
            Price.symbol,
            Price.date,
            Price.open,
            Price.high,
            Price.low,
            Price.close,
            Price.volume_traded,
            Price.industry_group.label('sector'),
            Price.market_cap,
            Price.company_name,
            StockIndicator.sma_50,
            StockIndicator.sma_200,
            StockIndicator.close_w,
            StockIndicator.sma9_w,
            StockIndicator.wma45_close_w,
            StockIndicator.percent_off_52w_high,
            func.coalesce(TasiComponent.company_name, Price.company_name).label("company_name_full")
        )
        .outerjoin(
            StockIndicator,
            (Price.symbol == StockIndicator.symbol) & (Price.date == StockIndicator.date),
        )
        .outerjoin(
            TasiComponent,
            Price.symbol == TasiComponent.symbol
        )
        .filter(Price.symbol.in_(valid_symbols))
    )

    query = query.statement

    df = pd.read_sql(query, db.connection())

    if df.empty:
        raise ValueError("No joined price/indicator rows found")

    df['symbol'] = df['symbol'].astype(str).str.strip()
    df = df[~df['symbol'].str.upper().isin(EXCLUDED_SYMBOLS)].copy()
    
    # EXCLUDE NOMU (Parallel Market) stocks - they start with 9
    df = df[~df['symbol'].str.startswith('9')].copy()

    df = df.rename(columns={
        "volume_traded": "volume",
        "company_name_full": "stock_name"
    })
    if "company_name" in df.columns:
        df.drop(columns=["company_name"], inplace=True, errors="ignore")
    
    df['stock_name'] = df['stock_name'].fillna(df['symbol'])
    df['sector'] = df['sector'].fillna("Unknown")

    float_cols = ['open', 'high', 'low', 'close', 'volume', 'market_cap', 
                  'sma_50', 'sma_200', 'close_w', 'sma9_w', 'wma45_close_w', 'percent_off_52w_high']
    
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['volume'] = df['volume'].fillna(0.0)
    df['market_cap'] = df['market_cap'].fillna(0.0)

    df['sma_trend_weekly'] = df['wma45_close_w'].fillna(df['sma9_w'])

    def _fix_pct(x):
        if pd.isna(x):
            return None
        return round(abs(x), 2) if x <= 0 else round(x, 2)

    df['percent_off_52w_high'] = df['percent_off_52w_high'].apply(_fix_pct)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    logger.info("Loaded %s stock rows (%s symbols)", len(df), df["symbol"].nunique())
    return df


def load_tasi_dataframe(db: Session) -> pd.DataFrame:
    records_dict = {}

    # 1. Fetch old history from Price table (e.g. ^TASI or TASI.SR)
    price_rows = (
        db.query(Price.date, Price.close)
        .filter(Price.symbol.in_(['^TASI', 'TASI.SR', 'TASI']))
        .all()
    )
    for r in price_rows:
        close = _to_float(r.close)
        if close is not None:
            records_dict[r.date] = {
                "symbol": "TASI",
                "date": r.date,
                "close": close,
                "sma_50": None,
                "sma_200": None,
            }

    # 2. Fetch from MarketPulse (newer data, will overwrite/merge)
    pulse_rows = db.query(MarketPulse).all()
    for r in pulse_rows:
        close = _to_float(r.close)
        if close is not None:
            if r.date in records_dict:
                records_dict[r.date]["sma_50"] = _to_float(r.sma_50)
                records_dict[r.date]["sma_200"] = _to_float(r.sma_200)
            else:
                records_dict[r.date] = {
                    "symbol": "TASI",
                    "date": r.date,
                    "close": close,
                    "sma_50": _to_float(r.sma_50),
                    "sma_200": _to_float(r.sma_200),
                }

    # 3. Always fetch from HistoricalReport to fill in any older data (pre-2010) and get true market volume
    hr_rows = db.query(HistoricalReport).all()
    for r in hr_rows:
        close = _to_float(r.close_price)
        vol = _to_float(r.volume_traded)
        if close is not None:
            if r.report_date not in records_dict:
                records_dict[r.report_date] = {
                    "symbol": "TASI",
                    "date": r.report_date,
                    "close": close,
                    "sma_50": None,
                    "sma_200": None,
                    "volume": vol or 0.0,
                }
            else:
                records_dict[r.report_date]["volume"] = vol or 0.0

    if not records_dict:
        raise ValueError("No TASI index data found")

    df = pd.DataFrame(list(records_dict.values()))
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    if df["sma_50"].isna().all():
        df["sma_50"] = df["close"].rolling(50, min_periods=1).mean()
    if df["sma_200"].isna().all():
        df["sma_200"] = df["close"].rolling(200, min_periods=1).mean()

    daily = df.set_index("date").sort_index()
    weekly = _compute_weekly_fields(daily["close"])
    for col in ("close_w", "sma9_w", "sma_trend_weekly"):
        daily[col] = weekly[col].reindex(daily.index).ffill()
    df = daily.reset_index()

    return df


def tasi_weekly_return(db: Session, week_start: date, week_end: date, df_tasi: pd.DataFrame = None) -> Optional[float]:
    """Actual TASI index return from df_tasi."""
    if df_tasi is not None and not df_tasi.empty:
        ws = pd.to_datetime(week_start)
        we = pd.to_datetime(week_end)
        tasi_prev = df_tasi[df_tasi["date"] < ws].sort_values("date")
        tasi_week = df_tasi[(df_tasi["date"] >= ws) & (df_tasi["date"] <= we)].sort_values("date")
        if not tasi_prev.empty and not tasi_week.empty:
            start = float(tasi_prev.iloc[-1]["close"])
            end = float(tasi_week.iloc[-1]["close"])
            if start > 0:
                return round((end - start) / start * 100, 2)
    return None


def market_cap_groups(df: pd.DataFrame, week_end: date) -> dict[str, list[str]]:
    """Split symbols into Large / Medium / Small cap tertiles by latest market cap."""
    week_end_dt = pd.to_datetime(week_end)
    latest = (
        df[df["date"] <= week_end_dt]
        .sort_values("date")
        .groupby("symbol")
        .last()
        .reset_index()
    )
    valid = latest[latest["market_cap"] > 0].sort_values(
        "market_cap", ascending=False
    )
    n = len(valid)
    if n < 3:
        syms = valid["symbol"].tolist()
        return {"Large Cap": syms, "Medium Cap": [], "Small Cap": []}

    third = n // 3
    return {
        "Large Cap": valid.iloc[:third]["symbol"].tolist(),
        "Medium Cap": valid.iloc[third : 2 * third]["symbol"].tolist(),
        "Small Cap": valid.iloc[2 * third :]["symbol"].tolist(),
    }

def get_market_volume(db, start_date, end_date):
    """Get true total market volume including ETFs/Indices for a given date range."""
    from sqlalchemy import func
    from app.models.price import Price
    vol = db.query(func.sum(Price.volume_traded)).filter(
        Price.date >= start_date,
        Price.date <= end_date,
        Price.symbol != "^TASI" # Sum all components
    ).scalar()
    return float(vol) if vol else 0.0
