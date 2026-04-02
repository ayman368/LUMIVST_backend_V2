"""
فحص جدول stock_indicators:
  - كم يوم محفوظ؟
  - أقدم وأحدث تاريخ؟
  - عدد الأسهم لكل يوم (عينة من أول 5 وآخر 5)
  - هل الأعمدة (sma_50, sma_150, sma_200) موجودة تاريخياً؟
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, create_engine
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

with engine.connect() as conn:
    # 1. عدد الأيام المحفوظة
    r = conn.execute(text("SELECT COUNT(DISTINCT date) FROM stock_indicators")).scalar()
    print(f"📅 عدد الأيام المحفوظة في stock_indicators: {r}")

    # 2. أقدم وأحدث تاريخ
    r = conn.execute(text("SELECT MIN(date), MAX(date) FROM stock_indicators")).fetchone()
    print(f"📆 أقدم تاريخ: {r[0]}")
    print(f"📆 أحدث تاريخ: {r[1]}")

    # 3. إجمالي الصفوف
    r = conn.execute(text("SELECT COUNT(*) FROM stock_indicators")).scalar()
    print(f"📊 إجمالي الصفوف: {r:,}")

    # 4. عينة: أول 5 أيام وعدد الأسهم في كل يوم
    print("\n--- أقدم 5 أيام ---")
    rows = conn.execute(text("""
        SELECT date, COUNT(*) as cnt,
               COUNT(sma_50) as has_sma50,
               COUNT(sma_200) as has_sma200
        FROM stock_indicators
        GROUP BY date ORDER BY date ASC LIMIT 5
    """)).fetchall()
    for row in rows:
        print(f"  {row[0]}  →  {row[1]} سهم  |  sma_50: {row[2]}  |  sma_200: {row[3]}")

    # 5. آخر 5 أيام
    print("\n--- أحدث 5 أيام ---")
    rows = conn.execute(text("""
        SELECT date, COUNT(*) as cnt,
               COUNT(sma_50) as has_sma50,
               COUNT(sma_200) as has_sma200
        FROM stock_indicators
        GROUP BY date ORDER BY date DESC LIMIT 5
    """)).fetchall()
    for row in rows:
        print(f"  {row[0]}  →  {row[1]} سهم  |  sma_50: {row[2]}  |  sma_200: {row[3]}")

    # 6. هل يوجد أيام بدون sma_200 نهائياً؟
    print("\n--- أيام فيها sma_200 = NULL لكل الأسهم (لو موجودة) ---")
    rows = conn.execute(text("""
        SELECT date, COUNT(*) as total, COUNT(sma_200) as filled
        FROM stock_indicators
        GROUP BY date
        HAVING COUNT(sma_200) = 0
        ORDER BY date DESC LIMIT 10
    """)).fetchall()
    if rows:
        for row in rows:
            print(f"  {row[0]}  →  {row[1]} سهم  |  sma_200 filled: {row[2]}")
    else:
        print("  ✅ كل الأيام فيها sma_200 لسهم واحد على الأقل")

print("\n✅ Done!")
