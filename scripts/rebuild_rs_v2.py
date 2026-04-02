"""
rebuild_rs_v2.py
================
هذا الاسكريبت يقوم بـ:
1. حذف جدول rs_daily_v3 المؤقت (إن وُجد)
2. حذف كل البيانات التاريخية من rs_daily_v2
3. إعادة الحساب الكامل بالمعادلات الصحيحة المطابقة للإكسيل:
   - Rank: MIN(ROUND(PERCENTRANK.INC(range, val)*100, 0), 99)
   - RS:   ROUNDUP(R3m*40% + R6m*20% + R9m*20% + R12m*20%, 0)
4. حفظ النتائج في rs_daily_v2 باستخدام COPY (الأسرع)

ملحوظة: هذا الاسكريبت سيستغرق دقيقة واحدة تقريباً
"""

import sys
import time
from pathlib import Path
from io import StringIO

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))
from app.core.config import settings
from app.core.database import engine


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def excel_percentrank_inc(series_or_group):
    """
    Matches Excel: =MIN(ROUND(PERCENTRANK.INC(range, val)*100, 0), 99)
    = (rank - 1) / (N - 1) * 100  (0 to 100)
    """
    pass  # Implemented inline below via groupby


def get_psycopg2_connection():
    """Parse DATABASE_URL and connect using psycopg2 directly for COPY."""
    db_url = str(settings.DATABASE_URL).replace("postgresql://", "")
    user_pass, host_db = db_url.split("@")
    user, password = user_pass.split(":")
    host_port, dbname = host_db.split("/")
    if "?" in dbname:
        dbname = dbname.split("?")[0]
    if ":" in host_port:
        host, port = host_port.split(":")
    else:
        host, port = host_port, "5432"

    return psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port,
        sslmode="require"
    )


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def rebuild_rs_v2():
    total_start = time.time()
    
    print("=" * 60)
    print("🔄  RS Daily V2 — Full Rebuild with Corrected Math")
    print("=" * 60)

    # ── Step 1: Drop rs_daily_v3 (temp table) ─────────────────
    print("\n[1/5] 🗑️  Dropping temporary rs_daily_v3 table...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS rs_daily_v3 CASCADE"))
    print("      ✅ rs_daily_v3 dropped.")

    # ── Step 2: Clear rs_daily_v2 ─────────────────────────────
    print("\n[2/5] 🗑️  Clearing all data from rs_daily_v2...")
    with engine.begin() as conn:
        result = conn.execute(text("DELETE FROM rs_daily_v2"))
        print(f"      ✅ Deleted {result.rowcount:,} rows from rs_daily_v2.")

    # ── Step 3: Load price history ────────────────────────────
    print("\n[3/5] 📥  Loading full price history from prices table...")
    with engine.connect() as conn:
        df_prices = pd.read_sql(
            text("SELECT date, symbol, close FROM prices WHERE date >= '2000-01-01' AND close > 0 ORDER BY date"),
            conn
        )
    print(f"      ✅ Loaded {len(df_prices):,} price records.")

    # ── Step 4: Calculate RS ──────────────────────────────────
    print("\n[4/5] 🧮  Calculating RS with corrected Excel formulas...")

    # Pivot
    df_wide = df_prices.pivot(index="date", columns="symbol", values="close").sort_index()
    df_wide = df_wide.replace(0, 0.000001)
    df_wide = df_wide.ffill(limit=10)  # FIX: Sparsity Bug

    periods = {"3m": 63, "6m": 126, "9m": 189, "12m": 252}

    returns_dfs = {}
    for name, days in periods.items():
        ret = df_wide.pct_change(periods=days).replace([np.inf, -np.inf], np.nan)
        returns_dfs[name] = ret

    def melt_col(df_col, col_name):
        s = df_col.stack(dropna=False)
        s.name = col_name
        return s

    df_all = pd.concat([
        melt_col(returns_dfs["3m"], "return_3m"),
        melt_col(returns_dfs["6m"], "return_6m"),
        melt_col(returns_dfs["9m"], "return_9m"),
        melt_col(returns_dfs["12m"], "return_12m"),
        melt_col(df_wide, "current_price"),
    ], axis=1).reset_index()

    df_all = df_all.dropna(subset=["return_3m"])
    print(f"      Valid rows: {len(df_all):,}")

    # FIX: PERCENTRANK.INC = (rank-1) / (N-1) * 100
    for p in periods.keys():
        col = f"return_{p}"
        rank_col = f"rank_{p}"
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

        ranks  = df_all.groupby("date")[col].rank(method="average", na_option="keep")
        counts = df_all.groupby("date")[col].transform("count")

        pct = np.where(counts > 1, ((ranks - 1) / (counts - 1)) * 100, 50)
        pct_series = pd.Series(pct, index=df_all.index)
        pct_series = pct_series.where(df_all[col].notna(), np.nan)

        # MIN(ROUND(..., 0), 99) -> np.floor(x + 0.5) matches Excel
        df_all[rank_col] = np.minimum(np.floor(pct_series + 0.5), 99)

    # ROUNDUP(weighted_avg, 0)
    weights = {"3m": 0.40, "6m": 0.20, "9m": 0.20, "12m": 0.20}
    numerator   = np.zeros(len(df_all))
    denominator = np.zeros(len(df_all))

    for p, w in weights.items():
        rank_col = f"rank_{p}"
        mask = df_all[rank_col].notna()
        numerator   += df_all[rank_col].fillna(0) * (mask * w)
        denominator += mask * w
        df_all[rank_col] = df_all[rank_col].fillna(-1).astype(int).replace({-1: None})

    raw = np.where(denominator > 0, numerator / denominator, np.nan)
    df_all["rs_raw"]    = raw
    df_all["rs_rating"] = np.ceil(raw)         # ROUNDUP(..., 0)
    df_all["rs_rating"] = df_all["rs_rating"].fillna(-1).astype(int).replace({-1: None})

    # Add company meta
    with engine.connect() as conn:
        df_meta = pd.read_sql(
            text("SELECT DISTINCT symbol, company_name, industry_group FROM prices"),
            conn
        ).drop_duplicates(subset=["symbol"], keep="last")

    df_final = pd.merge(df_all, df_meta, on="symbol", how="left")
    print(f"      ✅ Calculated {len(df_final):,} rows.")

    # ── Step 5: Save via COPY ──────────────────────────────────
    print("\n[5/5] 💾  Saving to rs_daily_v2 via fast COPY...")

    cols = [
        "symbol", "date", "rs_rating", "rs_raw",
        "return_3m", "return_6m", "return_9m", "return_12m",
        "rank_3m", "rank_6m", "rank_9m", "rank_12m",
        "company_name", "industry_group",
    ]
    for c in cols:
        if c not in df_final.columns:
            df_final[c] = None

    df_clean = df_final[cols].copy()
    df_clean["date"] = df_clean["date"].astype(str)
    df_clean = df_clean.fillna("\\N")

    csv_buf = StringIO()
    df_clean.to_csv(csv_buf, header=False, index=False, sep="\t", na_rep="\\N")
    csv_buf.seek(0)

    pg_conn = get_psycopg2_connection()
    cur = pg_conn.cursor()
    cur.copy_from(csv_buf, "rs_daily_v2", sep="\t", columns=cols, null="\\N")
    pg_conn.commit()

    # Recreate indexes
    print("      🔨 Recreating indexes...")
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_rs_v2_symbol_date ON rs_daily_v2(symbol, date)",
        "CREATE INDEX IF NOT EXISTS idx_rs_v2_date_rating ON rs_daily_v2(date, rs_rating DESC)",
        "CREATE INDEX IF NOT EXISTS idx_rs_v2_date        ON rs_daily_v2(date)",
    ]:
        cur.execute(idx_sql)
    pg_conn.commit()
    cur.close()
    pg_conn.close()

    elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    print(f"🎉  DONE! rs_daily_v2 rebuilt in {elapsed:.1f}s")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    confirm = input(
        "⚠️  هذا سيمسح كل بيانات rs_daily_v2 ويعيد حسابها من الصفر.\n"
        "    هل أنت متأكد؟ اكتب YES للتأكيد: "
    )
    if confirm.strip().upper() == "YES":
        rebuild_rs_v2()
    else:
        print("❌ تم الإلغاء.")
