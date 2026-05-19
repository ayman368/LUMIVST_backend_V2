"""
fix_missing_columns.py
======================
يضيف الأعمدة الناقصة لجدول rs_daily_v2 ويحسب return_1m و rank_1m.

آمن تماماً:
  - يستخدم ALTER TABLE ADD COLUMN IF NOT EXISTS
  - يحسب return_1m (21 يوم تداول) و rank_1m
  - يحدث عن طريق Temp Table + UPDATE (لا TRUNCATE)
"""

import sys
import time
from pathlib import Path
from io import StringIO

import pandas as pd
import numpy as np
import psycopg2

sys.path.append(str(Path(__file__).resolve().parent.parent))
from app.core.config import settings


def get_pg_connection():
    """Parse DATABASE_URL and connect using psycopg2 directly."""
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
        dbname=dbname, user=user, password=password,
        host=host, port=port, sslmode="require"
    )


def main():
    print("🔧 Fix Missing Columns in rs_daily_v2")
    print("=" * 60)
    start_time = time.time()

    pg_conn = get_pg_connection()
    cur = pg_conn.cursor()

    # ── Step 1: Add missing columns ──────────────────────────────────────
    print("\n[1/4] 📋 Adding missing columns...")

    columns_to_add = [
        ("return_1m", "DECIMAL(10, 6)"),
        ("rank_1m", "INTEGER"),
        ("sector_rs_rating", "VARCHAR(5)"),
        ("industry_group_rs_rating", "VARCHAR(5)"),
        ("industry_rs_rating", "VARCHAR(5)"),
        ("sub_industry_rs_rating", "VARCHAR(5)"),
    ]

    for col_name, col_type in columns_to_add:
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'rs_daily_v2' AND column_name = '{col_name}'
                ) THEN
                    ALTER TABLE rs_daily_v2 ADD COLUMN {col_name} {col_type};
                    RAISE NOTICE 'Added column {col_name}';
                END IF;
            END $$;
        """)
    pg_conn.commit()
    print("      ✅ All missing columns added/verified.")

    # ── Step 2: Load price data for 1-month return ───────────────────────
    print("\n[2/4] 📥 Loading prices for 1-month return calculation...")
    df_prices = pd.read_sql(
        "SELECT date, symbol, close FROM prices WHERE date >= '2000-01-01' AND close > 0 ORDER BY date",
        pg_conn
    )
    print(f"      📊 Loaded {len(df_prices):,} price records.")

    # ── Step 3: Calculate return_1m and rank_1m ──────────────────────────
    print("\n[3/4] 🧮 Calculating return_1m (21 trading days) & rank_1m...")

    # Pivot to wide format
    df_wide = df_prices.pivot(index="date", columns="symbol", values="close").sort_index()
    df_wide = df_wide.replace(0, 0.000001)
    df_wide = df_wide.ffill(limit=10)

    # 1-month return (21 trading days)
    ret_1m = df_wide.pct_change(periods=21).replace([np.inf, -np.inf], np.nan)

    # Melt back to long format
    ret_1m_long = ret_1m.stack(dropna=False).reset_index()
    ret_1m_long.columns = ["date", "symbol", "return_1m"]
    ret_1m_long = ret_1m_long.dropna(subset=["return_1m"])

    # Calculate rank_1m using PERCENTRANK.INC formula
    ret_1m_long["return_1m"] = pd.to_numeric(ret_1m_long["return_1m"], errors="coerce")
    ranks = ret_1m_long.groupby("date")["return_1m"].rank(method="average", na_option="keep")
    counts = ret_1m_long.groupby("date")["return_1m"].transform("count")

    pct = np.where(counts > 1, ((ranks - 1) / (counts - 1)) * 100, 50)
    pct_series = pd.Series(pct, index=ret_1m_long.index)
    pct_series = pct_series.where(ret_1m_long["return_1m"].notna(), np.nan)

    ret_1m_long["rank_1m"] = np.minimum(np.floor(pct_series + 0.5), 99)

    print(f"      ✅ Calculated {len(ret_1m_long):,} rows.")

    # ── Step 4: Upload via temp table + UPDATE ───────────────────────────
    print("\n[4/4] 📤 Uploading return_1m & rank_1m to rs_daily_v2...")

    # Prepare data
    upload_df = ret_1m_long[["symbol", "date", "return_1m", "rank_1m"]].copy()
    upload_df["date"] = upload_df["date"].astype(str)
    upload_df["rank_1m"] = upload_df["rank_1m"].fillna(-1).astype(int).replace({-1: None})

    # Handle NaN for COPY
    upload_df = upload_df.fillna("\\N")

    csv_buf = StringIO()
    upload_df.to_csv(csv_buf, header=False, index=False, sep="\t", na_rep="\\N")
    csv_buf.seek(0)

    # Create temp table
    cur.execute("""
        CREATE TEMP TABLE tmp_1m_fix (
            symbol VARCHAR(20),
            date DATE,
            return_1m DECIMAL(10, 6),
            rank_1m INTEGER
        )
    """)

    # COPY to temp table
    cur.copy_from(csv_buf, "tmp_1m_fix", sep="\t",
                  columns=["symbol", "date", "return_1m", "rank_1m"], null="\\N")
    pg_conn.commit()
    print(f"      ✅ Uploaded {len(upload_df):,} rows to temp table.")

    # Create index for faster JOIN
    cur.execute("CREATE INDEX tmp_1m_idx ON tmp_1m_fix(symbol, date)")
    pg_conn.commit()

    # UPDATE
    cur.execute("""
        UPDATE rs_daily_v2
        SET return_1m = t.return_1m,
            rank_1m = t.rank_1m
        FROM tmp_1m_fix t
        WHERE rs_daily_v2.symbol = t.symbol
          AND rs_daily_v2.date = t.date
    """)
    updated = cur.rowcount
    pg_conn.commit()

    cur.close()
    pg_conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"✅ DONE in {elapsed:.1f}s")
    print(f"   ✔ Added 6 missing columns to rs_daily_v2")
    print(f"   ✔ Updated {updated:,} rows with return_1m & rank_1m")
    print(f"   ℹ️  sector/industry ratings: run calculate_industry_groups.py")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
