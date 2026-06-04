"""
backfill_ad_rating.py
=====================
حساب تقييم A/D Rating تاريخياً وتحديثه في rs_daily_v2.

الافتراضي: من 2002 (مثل market breadth) — يحتاج ~63 يوم أسعار قبل أول تاريخ إخراج.

⚠️  هذا الاسكريبت آمن تماماً:
    - لا يمسح أي بيانات من الجدول الرئيسي
    - يستخدم Temp Table + UPDATE لعمود acc_dis_rating فقط
    - حتى لو فشل في أي خطوة، البيانات الأصلية تبقى سليمة

بعد التشغيل: امسح كاش Redis (market_breadth:ad_rating:* و screener:historical:ad_rating_*)
أو انتظر انتهاء TTL (~1 ساعة).
"""

# أقدم تاريخ في شارتات market breadth؛ نحمّل أسعاراً قبله لنافذة 63 يوم
OUTPUT_FROM = "2002-01-01"
PRICE_LOAD_FROM = "2001-09-01"

import sys
import time
from pathlib import Path
import pandas as pd
import numpy as np
import psycopg2
from io import StringIO

sys.path.append(str(Path(__file__).resolve().parent.parent))
from app.core.config import settings


def get_letter_grades_series(percentiles: pd.Series) -> pd.Series:
    """
    Same grading as calculate_ibd_metrics.py:
    A+ (>=93), A (>=85), A- (>=77), B+ (>=70), B (>=63), B- (>=56),
    C+ (>=49), C (>=42), C- (>=35), D+ (>=28), D (>=21), D- (>=14), E (<14)
    """
    bins = [-np.inf, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 85, 93, np.inf]
    labels = ['E', 'D-', 'D', 'D+', 'C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+']
    return pd.cut(percentiles, bins=bins, labels=labels, right=False).astype(str)


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
    print(f"🚀 Starting A/D Rating Backfill from {OUTPUT_FROM} (SAFE mode)...")
    start_time = time.time()

    # ── Step 1: Load price data ──────────────────────────────────────────
    print(f"\n[1/5] 📥 Loading prices (since {PRICE_LOAD_FROM} for 63d warmup)...")
    pg_conn = get_pg_connection()
    df = pd.read_sql(
        "SELECT symbol, date, close, high, low, volume_traded "
        f"FROM prices WHERE date >= '{PRICE_LOAD_FROM}' ORDER BY symbol, date ASC",
        pg_conn
    )
    print(f"      📊 Loaded {len(df):,} price records.")

    # ── Step 2: Calculate A/D Rating ─────────────────────────────────────
    print("\n[2/5] 🧮 Calculating CLV + MFV + Rolling 13-week sum...")

    # CLV = ((Close - Low) - (High - Close)) / (High - Low)
    denom = df['high'] - df['low']
    df['clv'] = np.where(denom == 0, 0,
                         ((df['close'] - df['low']) - (df['high'] - df['close'])) / denom)

    # Money Flow Volume = CLV * Volume
    df['mfv'] = df['clv'] * df['volume_traded']

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    # Rolling 63-day (13 weeks) sum per symbol
    df = df.set_index('date')
    rolling_mfv = df.groupby('symbol')['mfv'].rolling(
        window=63, min_periods=10
    ).sum().reset_index()
    df = df.reset_index()
    df = df.merge(rolling_mfv, on=['symbol', 'date'], suffixes=('', '_13w'))

    # ── Step 3: Rank & Grade ─────────────────────────────────────────────
    print("\n[3/5] 🏆 Ranking per date & applying letter grades...")
    valid = df.dropna(subset=['mfv_13w']).copy()
    valid['percentile'] = valid.groupby('date')['mfv_13w'].rank(pct=True) * 100
    valid['grade'] = get_letter_grades_series(valid['percentile'])

    valid = valid[valid['date'] >= OUTPUT_FROM]
    print(f"      ✅ Calculated {len(valid):,} A/D ratings.")

    # ── Step 4: Prepare data for upload ──────────────────────────────────
    print("\n[4/5] 📤 Uploading to temp table via COPY...")
    valid['date_str'] = valid['date'].dt.strftime('%Y-%m-%d')
    upload_df = valid[['symbol', 'date_str', 'grade']].copy()

    csv_buf = StringIO()
    upload_df.to_csv(csv_buf, header=False, index=False, sep="\t")
    csv_buf.seek(0)

    cur = pg_conn.cursor()

    # Create temp table (auto-dropped when connection closes)
    cur.execute("""
        CREATE TEMP TABLE tmp_ad_backfill (
            symbol VARCHAR(20),
            date DATE,
            acc_dis_rating VARCHAR(5)
        )
    """)

    # Fast COPY into temp table
    cur.copy_from(csv_buf, "tmp_ad_backfill", sep="\t",
                  columns=["symbol", "date", "acc_dis_rating"])
    pg_conn.commit()
    print(f"      ✅ Uploaded {len(upload_df):,} rows to temp table.")

    # Create index on temp table for faster JOIN
    cur.execute("CREATE INDEX tmp_ad_idx ON tmp_ad_backfill(symbol, date)")
    pg_conn.commit()

    # ── Step 5: UPDATE only acc_dis_rating column ────────────────────────
    print("\n[5/5] 🔄 Updating acc_dis_rating in rs_daily_v2 (safe UPDATE)...")

    # Ensure column exists (it may be missing if table was rebuilt without it)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'rs_daily_v2' AND column_name = 'acc_dis_rating'
            ) THEN
                ALTER TABLE rs_daily_v2 ADD COLUMN acc_dis_rating VARCHAR(5);
            END IF;
        END $$;
    """)
    pg_conn.commit()
    print("      ✅ Column acc_dis_rating verified/created.")
    cur.execute("""
        UPDATE rs_daily_v2
        SET acc_dis_rating = t.acc_dis_rating
        FROM tmp_ad_backfill t
        WHERE rs_daily_v2.symbol = t.symbol
          AND rs_daily_v2.date = t.date
    """)
    updated_count = cur.rowcount
    pg_conn.commit()

    # Cleanup
    cur.close()
    pg_conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"✅ DONE in {elapsed:.1f}s")
    print(f"   Updated {updated_count:,} rows in rs_daily_v2.acc_dis_rating")
    print(f"   ⚠️  No other columns were touched!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
