import pandas as pd
from typing import Optional
from sqlalchemy.orm import Session
from datetime import datetime

from app.services.rs_line import _fetch_stock_close, _fetch_tasi_close

def calculate_mansfield_rs(
    db: Session,
    symbol: str,
    benchmark: str = "^TASI.SR",
    start_date: str = "2018-01-01",
    end_date: Optional[str] = None,
    ma_length: int = 52,
) -> pd.DataFrame:
    """
    يحسب Mansfield RS على الفريم الأسبوعي باستخدام البيانات من قاعدة البيانات.
    """
    end = end_date or datetime.today().strftime("%Y-%m-%d")

    # جلب البيانات اليومية
    stock_close = _fetch_stock_close(db, symbol, start_date, end)
    bench_close = _fetch_tasi_close(db, start_date, end)

    # دمج البيانات مع Forward Fill للسهم والبنشمارك (عشان لو سهم قفل الأربعاء والسوق شغال للخميس)
    df = pd.DataFrame({"stock": stock_close, "bench": bench_close})
    df["stock"] = df["stock"].ffill()
    df["bench"] = df["bench"].ffill()
    df = df.dropna()

    if df.empty:
        raise ValueError(f"No overlapping data between {symbol} and {benchmark}")

    # تحويل البيانات إلى أسبوعية (كل خميس بنهاية الأسبوع للسوق السعودي)
    df = df.resample('W-THU').last().dropna()

    # ── خطوة 1: نسبة السهم للـ Benchmark ─────────────────
    df["stock_div_bench"] = (df["stock"] / df["bench"]) * 100

    # ── خطوة 2: Zero Line = SMA(52) ──────────────────────
    df["zero_line"] = df["stock_div_bench"].rolling(window=ma_length).mean()

    # ── خطوة 3: Mansfield RS ──────────────────────────────
    df["mansfield_rs"] = ((df["stock_div_bench"] / df["zero_line"]) - 1) * 100

    # ── خطوة 4: Zero Line اتجاه (صاعد/هابط) ──────────────
    df["ma_rising"]  = df["zero_line"] > df["zero_line"].shift(1)
    df["ma_falling"] = df["zero_line"] < df["zero_line"].shift(1)

    # ── خطوة 5: Background Zone ───────────────────────────
    def get_zone(row):
        above = row["stock_div_bench"] > row["zero_line"]
        rising = row["ma_rising"]
        if pd.isna(row["zero_line"]):
            return "negative"
        if above and rising:
            return "positive"       # 🟢 أخضر
        elif not above and rising:
            return "neutral_rising" # 🔵 أزرق
        elif above and not rising:
            return "neutral_falling"# ⚫ رمادي
        else:
            return "negative"       # 🔴 أحمر

    df["zone"] = df.apply(get_zone, axis=1)

    # ── خطوة 6: Zero Line Crossovers ──────────────────────
    df["cross_above_zero"] = (df["mansfield_rs"] > 0) & (df["mansfield_rs"].shift(1) <= 0)
    df["cross_below_zero"] = (df["mansfield_rs"] < 0) & (df["mansfield_rs"].shift(1) >= 0)

    # ── خطوة 7: RS Direction ──────────────────────────────
    df["rs_up"] = df["mansfield_rs"] > df["mansfield_rs"].shift(1)

    return df

def df_to_response(df: pd.DataFrame, symbol: str, benchmark: str, ma_length: int) -> dict:
    # أخذ الصفوف بعد تسخين SMA 
    df_valid = df.dropna(subset=["zero_line"])
    
    if df_valid.empty:
        raise ValueError("Not enough data to calculate 52-week moving average.")

    last  = df_valid.iloc[-1]
    bulls = df_valid[df_valid["cross_above_zero"]]
    bears = df_valid[df_valid["cross_below_zero"]]

    summary = {
        "last_date":          str(df_valid.index[-1].date()),
        "mansfield_rs":       round(float(last["mansfield_rs"]), 4),
        "stock_div_bench":    round(float(last["stock_div_bench"]), 4),
        "zero_line":          round(float(last["zero_line"]), 4),
        "zone":               str(last["zone"]),
        "ma_rising":          bool(last["ma_rising"]),
        "above_zero":         bool(last["mansfield_rs"] > 0),
        "rs_up":              bool(last["rs_up"]),
        "last_cross_above":   str(bulls.index[-1].date()) if not bulls.empty else None,
        "last_cross_below":   str(bears.index[-1].date()) if not bears.empty else None,
    }

    data = []
    for dt, row in df_valid.iterrows():
        data.append({
            "date":             str(dt.date()),
            "stock_close":      round(float(row["stock"]),           4),
            "bench_close":      round(float(row["bench"]),           2),
            "stock_div_bench":  round(float(row["stock_div_bench"]), 4),
            "zero_line":        round(float(row["zero_line"]),       4),
            "mansfield_rs":     round(float(row["mansfield_rs"]),    4),
            "zone":             str(row["zone"]),
            "ma_rising":        bool(row["ma_rising"]),
            "cross_above_zero": bool(row["cross_above_zero"]),
            "cross_below_zero": bool(row["cross_below_zero"]),
            "rs_up":            bool(row["rs_up"]),
        })

    return {
        "symbol":     symbol,
        "benchmark":  benchmark,
        "ma_length":  ma_length,
        "timeframe":  "1W",
        "summary":    summary,
        "data":       data,
        "total_bars": len(data),
    }
