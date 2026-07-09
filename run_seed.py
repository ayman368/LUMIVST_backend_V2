import asyncio
from sqlalchemy import text
from app.core.database import SessionLocal

def seed_database():
    print("⏳ جاري إدخال البيانات الأولية إلى قاعدة البيانات...")
    
    with open("seed_valuation_data.sql", "r", encoding="utf-8") as file:
        sql_script = file.read()
        
    db = SessionLocal()
    try:
        # PostgreSQL multiple statements can be executed as text
        db.execute(text(sql_script))
        db.commit()
        print("✅ تم رفع البيانات الأولية بنجاح!")
    except Exception as e:
        db.rollback()
        print(f"❌ حدث خطأ أثناء إدخال البيانات: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    seed_database()
