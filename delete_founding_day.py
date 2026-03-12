"""
حذف صفوف 2026-02-22 الخاطئة من جدول prices
السوق السعودي كان مقفولاً في هذا اليوم (عيد تأسيس المملكة)
"""
import pandas as pd
from sqlalchemy import create_engine, text
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.core.config import settings

engine = create_engine(str(settings.DATABASE_URL))

# ========= Step 1: عدّ الصفوف قبل الحذف =========
with engine.connect() as conn:
    count_before = conn.execute(text("SELECT COUNT(*) FROM prices WHERE date = '2026-02-22'")).scalar()
    print(f"Rows to delete: {count_before}")

if count_before == 0:
    print("✅ No rows found for 2026-02-22. Nothing to delete.")
    exit()

# ========= Step 2: تأكيد الحذف =========
confirm = input(f"\nAre you sure you want to DELETE {count_before} rows for 2026-02-22? (yes/no): ")
if confirm.strip().lower() != 'yes':
    print("❌ Cancelled.")
    exit()

# ========= Step 3: حذف الصفوف =========
with engine.begin() as conn:
    result = conn.execute(text("DELETE FROM prices WHERE date = '2026-02-22'"))
    print(f"✅ Deleted {result.rowcount} rows for 2026-02-22 (Saudi Founding Day)")

# ========= Step 4: تأكيد أن السوق في الأيام المحيطة موجود =========
with engine.connect() as conn:
    surrounding = pd.read_sql(
        text("SELECT date, COUNT(*) as stocks FROM prices WHERE date BETWEEN '2026-02-19' AND '2026-02-25' GROUP BY date ORDER BY date"),
        conn
    )
    print("\n=== Remaining data around Feb 22 ===")
    print(surrounding.to_string(index=False))

print("\n✅ Done! Now run: .\\venv\\Scripts\\python.exe .\\scripts\\daily_calculations_only.py --date 2026-03-09")
