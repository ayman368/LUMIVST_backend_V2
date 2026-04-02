import sys, os
import pandas as pd
from sqlalchemy import create_engine, text
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import settings

def main():
    print("🚀 Starting Historical Market Breadth Calculation (20, 50, 100, 200 MA)")
    start_time = time.time()
    
    engine = create_engine(str(settings.DATABASE_URL))
    
    print("📡 Loading all price data from database (this might take a minute)...")
    # We only need close prices to calculate SMAs
    query = "SELECT symbol, date, close FROM prices ORDER BY symbol, date"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    if df.empty:
        print("❌ No price data found in the database.")
        return
        
    print(f"✅ Loaded {len(df):,} price records.")
    
    # تحويل التاريخ إلى datetime لضمان الترتيب السليم
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['symbol', 'date'])
    
    print("🧮 Calculating Moving Averages (20, 50, 100, 200) for all stocks...")
    # حساب المتوسطات المتحركة
    for ma in [20, 50, 100, 200]:
        df[f'sma_{ma}'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(ma).mean())
        
        # إنشاء أعمدة منطقية (هل السعر أعلى من المتوسط؟)
        df[f'above_{ma}'] = (df['close'] > df[f'sma_{ma}']).astype(int)
        
        # تحديد ما إذا كان المتوسط متوفراً في هذا اليوم (لضمان صحة النسبة المئوية)
        df[f'has_sma_{ma}'] = df[f'sma_{ma}'].notna().astype(int)

    print("📊 Aggregating Market Breadth globally by date...")
    # تجميع البيانات حسب التاريخ
    grouped = df.groupby('date').agg(
        above_20=('above_20', 'sum'),
        has_sma_20=('has_sma_20', 'sum'),
        
        above_50=('above_50', 'sum'),
        has_sma_50=('has_sma_50', 'sum'),
        
        above_100=('above_100', 'sum'),
        has_sma_100=('has_sma_100', 'sum'),
        
        above_200=('above_200', 'sum'),
        has_sma_200=('has_sma_200', 'sum'),
    ).reset_index()
    
    # حساب النسب المئوية مع تجنب القسمة على صفر
    for ma in [20, 50, 100, 200]:
        col_name = f'pct_above_{ma}'
        grouped[col_name] = (grouped[f'above_{ma}'] / grouped[f'has_sma_{ma}'] * 100).fillna(0).round(2)
        
    # تصفية الأعمدة النهائية للجدول
    final_df = grouped[['date', 'pct_above_20', 'pct_above_50', 'pct_above_100', 'pct_above_200']]
    
    # نتجاهل الأيام التي لم يكن فيها بيانات كافية للحساب
    # (مثلا أول 20 يوم في السوق لن يكون فيها sma_20)
    final_df = final_df[final_df['pct_above_20'] > 0]
    
    print(f"💾 Saving {len(final_df):,} daily breadth records to 'market_breadth' table...")
    
    # حفظ الجدول الجديد في الداتابيز
    with engine.begin() as conn:
        # إنشاء الجدول إذا لم يكن موجوداً
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS market_breadth (
                date DATE PRIMARY KEY,
                pct_above_20 NUMERIC(5, 2),
                pct_above_50 NUMERIC(5, 2),
                pct_above_100 NUMERIC(5, 2),
                pct_above_200 NUMERIC(5, 2)
            );
        """))
        
        # مسح البيانات القديمة لضمان نظافة السكريبت في حال إعادة التشغيل
        conn.execute(text("TRUNCATE TABLE market_breadth"))
        
    # حفظ الداتا باستخدام Pandas
    final_df.to_sql('market_breadth', con=engine, if_exists='append', index=False)
    
    elapsed = time.time() - start_time
    print(f"🎉 Successfully completed in {elapsed:.1f} seconds!")

if __name__ == "__main__":
    main()
