import asyncio
from sqlalchemy import text
from app.core.database import engine, Base
# نستدعي جميع النماذج ليتعرف عليها الـ Base
from app.models.economic_indicators import EconomicIndicator, SP500History, TreasuryYieldCurve
from app.models.eps_estimates import EpsEstimate
from app.models.system_config import SystemConfig
from app.models.valuation_zones import ValuationZone
from app.models.tasi_components import TasiComponent

def create_tables():
    print("⏳ جاري إنشاء جداول التقييم الجديدة في قاعدة البيانات...")
    # هذه الخطوة ستنشئ الجداول الجديدة فقط ولن تمسح بياناتك القديمة
    Base.metadata.create_all(bind=engine)
    print("✅ تم إنشاء الجداول بنجاح!")

if __name__ == "__main__":
    create_tables()
