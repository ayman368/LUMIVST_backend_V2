# app/core/database.py

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from app.core.config import settings

# ⭐ إعدادات pool محسّنة
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=15,           # ⬅️ تقليل للإنتاج
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,      # ⬅️ ساعة واحدة
    pool_pre_ping=True,     # ⬅️ مهم للإنتاج
    echo=False              # ⬅️ معطّل عشان مايملاش الـ terminal رسايل
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# استيراد الـ Models

from app.models.price import Price
from app.models.rs_daily import RSDaily
from app.models.official_filings import CompanyOfficialFiling
from app.models.economic_indicators import EconomicIndicator

def create_tables():
    """إنشاء الجداول"""
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ تم إنشاء الجداول في PostgreSQL بنجاح")
    except Exception as e:
        print(f"❌ خطأ في إنشاء الجداول: {e}")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()