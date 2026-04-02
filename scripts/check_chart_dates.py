import sys, os
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import settings

def main():
    engine = create_engine(str(settings.DATABASE_URL))
    
    with engine.begin() as conn:
        q_start = "SELECT min(date), max(date), count(*) FROM market_breadth"
        r = conn.execute(text(q_start)).fetchone()
        
        print("====== 📊 إحصائيات جدول market_breadth ======")
        print(f"أقدم تاريخ كامل متوفر: {r[0]}")
        print(f"أحدث تاريخ متوفر: {r[1]}")
        print(f"إجمالي الأيام: {r[2]} يوم تداول")
        
        print("\n====== 🚨 مشكلة العرض (لماذا توقف عند 2020) ======")
        print("طريقة استعلام الـ API الحالية تستخدم:")
        print("ORDER BY date ASC LIMIT 5000")
        
        q_issue = "SELECT min(date), max(date) FROM (SELECT date FROM market_breadth ORDER BY date ASC LIMIT 5000) sub"
        r_issue = conn.execute(text(q_issue)).fetchone()
        
        print(f"استدعاء أول 5000 يوم من البداية يؤدي إلى جلب البيانات من: {r_issue[0]} وينتهي عند: {r_issue[1]}")
        print("ولهذا السبب تجد أن الشارت يتوقف عند نهايات 2021 وتختفي الـ 5 سنوات الأخيرة تماماً!")
        print("\n✅ تم تجهيز الحل في ملف market_breadth.py وسيقوم بجلب أحدث البيانات.")

if __name__ == "__main__":
    main()
