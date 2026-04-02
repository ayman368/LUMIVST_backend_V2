import sys, os
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import settings

def main():
    print("🚀 Adding SMA 20 and SMA 100 columns to 'stock_indicators' table...")
    
    engine = create_engine(str(settings.DATABASE_URL))
    
    # قائمة الأعمدة الجديدة لإضافتها
    columns_to_add = [
        "sma_20 NUMERIC(14, 4)",
        "sma_100 NUMERIC(14, 4)",
        "price_minus_sma_20 NUMERIC(14, 4)",
        "price_minus_sma_100 NUMERIC(14, 4)",
        "price_vs_sma_20_percent NUMERIC(14, 4)",
        "price_vs_sma_100_percent NUMERIC(14, 4)"
    ]
    
    with engine.begin() as conn:
        for col in columns_to_add:
            try:
                # محاولة إضافة العمود (ستفشل بهدوء لو كان موجود بالفعل إذا استخدمنا معالجة الاستثناء، 
                # لكننا سنضيف العمود IF NOT EXISTS لو كانت بقاعدة البيانات PostgreSQL)
                query = f"ALTER TABLE stock_indicators ADD COLUMN IF NOT EXISTS {col};"
                conn.execute(text(query))
                print(f"✅ Added column: {col.split(' ')[0]}")
            except Exception as e:
                print(f"⚠️ Error adding {col.split(' ')[0]}: {e}")

    print("🎉 All columns added successfully!")

if __name__ == "__main__":
    main()
