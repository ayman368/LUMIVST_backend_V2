# app/services/sata.py
"""
SATA — Stage Analysis Technical Attributes (Stan Weinstein)
============================================================
10 bands scored 0 or 1.  Total 0-10.

Band layout (top → bottom in the visual grid):
  10  Breakout / Breakdown
   9  Close > 30w SMA
   8  Close > 40w SMA
   7  30w SMA rising
   6  40w SMA rising
   5  Mansfield RS > 0
   4  MACD > Signal (momentum)
   3  RSI(14) > 50  (momentum)
   2  Volume expansion
   1  Overhead resistance cleared

Stage mapping:
  Score 7-10              → Stage 2  (Advancing / Markup)
  Score 4-6 after > 7     → Stage 3  (Distribution / Topping)
  Score 4-6 after < 3     → Stage 1  (Accumulation / Basing)
  Score 0-3               → Stage 4  (Declining / Markdown)
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)

# ── helpers (reuse from rs_line) ─────────────────────────────
from app.services.rs_line import _fetch_stock_close, _fetch_tasi_close


def _fetch_stock_ohlcv(db: Session, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch OHLCV for a stock from the prices table."""
    from app.models.price import Price
    from sqlalchemy import asc

    db_symbol = symbol.replace(".SR", "")
    results = (
        db.query(Price.date, Price.open, Price.high, Price.low, Price.close, Price.volume_traded)
        .filter(Price.symbol == db_symbol)
        .filter(Price.date >= start_date)
        .filter(Price.date <= end_date)
        .order_by(asc(Price.date))
        .all()
    )
    if not results:
        raise ValueError(f"No OHLCV data for {db_symbol}")

    df = pd.DataFrame(results, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df = df.astype(float)
    return df


def _resample_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample daily OHLCV to weekly (Thursday close for Saudi market)."""
    weekly = df.resample("W-THU").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()
    return weekly


def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder RSI."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _calc_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD line and signal line."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


# ── Main calculator ──────────────────────────────────────────
def calculate_sata(
    db: Session,
    symbol: str,
    benchmark: str = "^TASI.SR",
    start_date: str = "2018-01-01",
    end_date: Optional[str] = None,
    ma_length: int = 52,  # for Mansfield RS zero line
) -> pd.DataFrame:
    """
    Calculate all 10 SATA bands + total score + stage on weekly data.
    Returns a DataFrame with one row per week.
    """
    end = end_date or datetime.today().strftime("%Y-%m-%d")
    logger.info(f"📊 SATA: calculating for {symbol} vs {benchmark}")

    # 1. Fetch daily OHLCV + benchmark
    stock_ohlcv = _fetch_stock_ohlcv(db, symbol, start_date, end)
    bench_close = _fetch_tasi_close(db, start_date, end)

    # 2. Align daily data
    daily = stock_ohlcv.copy()
    daily["bench"] = bench_close
    daily["bench"] = daily["bench"].ffill()
    daily["close"] = daily["close"].ffill()
    daily = daily.dropna(subset=["close", "bench"])

    # 3. Resample stock to weekly
    weekly = _resample_weekly(daily[["open", "high", "low", "close", "volume"]])

    # Resample bench to weekly (last close of the week)
    bench_weekly = daily["bench"].resample("W-THU").last().dropna()
    weekly["bench"] = bench_weekly
    weekly["bench"] = weekly["bench"].ffill()
    weekly = weekly.dropna(subset=["bench"])

    if len(weekly) < 52:
        raise ValueError(f"Not enough weekly data ({len(weekly)} weeks, need >= 52)")

    # ── Calculate components ─────────────────────────────────

    c = weekly["close"]
    v = weekly["volume"]

    # Weighted Moving Averages (Weinstein uses WMA, not SMA)
    weights_30 = np.arange(1, 31)
    wma_30w = c.rolling(30).apply(lambda x: np.dot(x, weights_30) / weights_30.sum(), raw=True)
    weights_40 = np.arange(1, 41)
    wma_40w = c.rolling(40).apply(lambda x: np.dot(x, weights_40) / weights_40.sum(), raw=True)

    # Mansfield RS
    ratio = (c / weekly["bench"]) * 100
    zero_line = ratio.rolling(ma_length).mean()
    mansfield_rs = ((ratio / zero_line) - 1) * 100

    # RSI (14 weeks)
    rsi = _calc_rsi(c, 14)

    # MACD (12, 26, 9 on weekly)
    macd_line, macd_signal = _calc_macd(c)

    # Volume average (40-week)
    vol_avg = v.rolling(40).mean()

    # 52-week high (shifted to exclude current week)
    high_52w_prev = weekly["high"].rolling(52).max().shift(1)

    # 30-week high (for breakout detection)
    high_30w = weekly["high"].rolling(30).max().shift(1)  # shift to exclude current

    # ── Score each band ──────────────────────────────────────

    # Band 10: Breakout / Breakdown — close > previous 30-week high
    weekly["b10_breakout"] = (c >= high_30w).astype(int)

    # Band 9: Close > 30w WMA
    weekly["b09_above_30w"] = (c > wma_30w).astype(int)

    # Band 8: Close > 40w WMA
    weekly["b08_above_40w"] = (c > wma_40w).astype(int)

    # Band 7: 30w WMA rising
    weekly["b07_30w_rising"] = (wma_30w > wma_30w.shift(1)).astype(int)

    # Band 6: 30w WMA > 40w WMA (Golden Cross condition)
    weekly["b06_40w_rising"] = (wma_30w > wma_40w).astype(int)

    # Band 5: Mansfield RS > 0
    weekly["b05_mrs_positive"] = (mansfield_rs > 0).astype(int)

    # Band 4: MACD > Signal (momentum)
    weekly["b04_macd_bull"] = (macd_line > macd_signal).astype(int)

    # Band 3: RSI > 50 (momentum)
    weekly["b03_rsi_above50"] = (rsi > 50).astype(int)

    # Band 2: Volume expanding — current week volume > average
    weekly["b02_vol_expand"] = (v > vol_avg).astype(int)

    # Band 1: Overhead resistance cleared — close > previous 52w high
    weekly["b01_resistance_clear"] = (c >= high_52w_prev).astype(int)

    # ── Total Score ──────────────────────────────────────────
    band_cols = [
        "b10_breakout", "b09_above_30w", "b08_above_40w",
        "b07_30w_rising", "b06_40w_rising", "b05_mrs_positive",
        "b04_macd_bull", "b03_rsi_above50", "b02_vol_expand",
        "b01_resistance_clear",
    ]
    weekly["sata_score"] = weekly[band_cols].sum(axis=1)

    # ── Store extra columns for response ─────────────────────
    weekly["mansfield_rs"] = mansfield_rs
    weekly["rsi"] = rsi
    weekly["macd"] = macd_line
    weekly["macd_signal"] = macd_signal
    weekly["sma_30w"] = wma_30w   # actually WMA, keeping column name for API compat
    weekly["sma_40w"] = wma_40w

    # ── Stage determination ──────────────────────────────────
    weekly["stage"] = _determine_stages(weekly["sata_score"])

    # Drop warmup rows (first 52 weeks have NaN)
    weekly = weekly.dropna(subset=["sma_40w", "mansfield_rs"])

    logger.info(f"✅ SATA: {len(weekly)} weeks, latest score = {int(weekly['sata_score'].iloc[-1])}")
    return weekly


def _determine_stages(scores: pd.Series) -> pd.Series:
    """
    Determine Weinstein Stage from SATA score history.

    Logic:
      - Score >= 7 → Stage 2
      - Score <= 3 → Stage 4
      - Score 4-6:
          • If recently (last 8 weeks) had score >= 7 → Stage 3 (distribution)
          • If recently (last 8 weeks) had score <= 3 → Stage 1 (accumulation)
          • Otherwise → Stage 1 if score is rising, Stage 3 if falling
    """
    stages = pd.Series(index=scores.index, dtype="object")
    lookback = 8

    for i in range(len(scores)):
        score = scores.iloc[i]

        if pd.isna(score):
            stages.iloc[i] = None
            continue

        score = int(score)

        if score >= 7:
            stages.iloc[i] = "stage_2"
        elif score <= 3:
            stages.iloc[i] = "stage_4"
        else:
            # Neutral zone (4-6): look at recent history
            start_idx = max(0, i - lookback)
            recent = scores.iloc[start_idx:i]

            if len(recent) > 0 and recent.max() >= 7:
                stages.iloc[i] = "stage_3"
            elif len(recent) > 0 and recent.min() <= 3:
                stages.iloc[i] = "stage_1"
            else:
                # Fallback: check trend of score
                if i > 0 and score > scores.iloc[i - 1]:
                    stages.iloc[i] = "stage_1"
                else:
                    stages.iloc[i] = "stage_3"

    return stages


# ── Response builder ─────────────────────────────────────────
BAND_LABELS = {
    "b10_breakout": "Breakout / Breakdown",
    "b09_above_30w": "Close > 30w WMA",
    "b08_above_40w": "Close > 40w WMA",
    "b07_30w_rising": "30w WMA Rising",
    "b06_40w_rising": "30w > 40w WMA",
    "b05_mrs_positive": "Mansfield RS > 0",
    "b04_macd_bull": "MACD Bullish",
    "b03_rsi_above50": "RSI > 50",
    "b02_vol_expand": "Volume > Avg",
    "b01_resistance_clear": "New 52w High",
}

STAGE_LABELS = {
    "stage_1": "Stage 1 — Accumulation",
    "stage_2": "Stage 2 — Advancing",
    "stage_3": "Stage 3 — Distribution",
    "stage_4": "Stage 4 — Declining",
}

STAGE_EMOJI = {
    "stage_1": "🟡",
    "stage_2": "🟢",
    "stage_3": "🟠",
    "stage_4": "🔴",
}

BAND_COLS = list(BAND_LABELS.keys())


def df_to_response(df: pd.DataFrame, symbol: str, benchmark: str) -> dict:
    """Convert SATA DataFrame to API response."""
    last = df.iloc[-1]

    # Current bands status
    bands_now = {}
    for col, label in BAND_LABELS.items():
        val = int(last[col]) if pd.notna(last[col]) else 0
        bands_now[col] = {
            "label": label,
            "score": val,
            "status": "positive" if val == 1 else "negative",
        }

    stage = last["stage"] if pd.notna(last["stage"]) else "stage_1"
    score = int(last["sata_score"]) if pd.notna(last["sata_score"]) else 0

    summary = {
        "last_date": str(df.index[-1].date()),
        "sata_score": score,
        "stage": stage,
        "stage_label": STAGE_LABELS.get(stage, "Unknown"),
        "stage_emoji": STAGE_EMOJI.get(stage, "❓"),
        "mansfield_rs": round(float(last["mansfield_rs"]), 2) if pd.notna(last["mansfield_rs"]) else None,
        "rsi": round(float(last["rsi"]), 1) if pd.notna(last["rsi"]) else None,
        "bands": bands_now,
    }

    # Historical data (last 200 weeks max)
    display = df.tail(200)
    data = []
    for dt, row in display.iterrows():
        point = {
            "date": str(dt.date()),
            "open": round(float(row["open"]), 2) if pd.notna(row["open"]) else None,
            "high": round(float(row["high"]), 2) if pd.notna(row["high"]) else None,
            "low": round(float(row["low"]), 2) if pd.notna(row["low"]) else None,
            "close": round(float(row["close"]), 2),
            "sma_30w": round(float(row["sma_30w"]), 2) if pd.notna(row["sma_30w"]) else None,
            "sma_40w": round(float(row["sma_40w"]), 2) if pd.notna(row["sma_40w"]) else None,
            "sata_score": int(row["sata_score"]) if pd.notna(row["sata_score"]) else 0,
            "stage": row["stage"],
        }
        # Add each band score
        for col in BAND_COLS:
            point[col] = int(row[col]) if pd.notna(row[col]) else 0
        data.append(point)

    return {
        "symbol": symbol,
        "benchmark": benchmark,
        "timeframe": "1W",
        "summary": summary,
        "data": data,
        "total_bars": len(data),
    }
