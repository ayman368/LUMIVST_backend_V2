import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.append(str(Path(__file__).resolve().parent))
from app.core.config import settings

# استيراد ملف التحديث الأساسي
import scripts.daily_market_update as dmu

def dummy_run_historical():
    print("⏭️ Skipping historical reports scraper...")
    return True

def dummy_update_market_pulse():
    print("⏭️ Skipping market pulse scraper...")
    return True

def dummy_scrape(headless=True):
    print("⏭️ Skipping daily details scraper (using existing DB prices)...")
    # إرجاع مصفوفة وهمية لكي لا يتوقف السكريبت عند شرط (if not scraped_data)
    # هكذا سيتخطى حفظ الأسعار ويكمل لباقي حسابات المؤشرات مباشرة
    return [{"Symbol": ""}]

def run(start_date_str=None):
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    else:
        # محاولة قراءة آخر تاريخ ناجح من الملف
        progress_file = Path("recalculate_progress.txt")
        if progress_file.exists():
            last_date_str = progress_file.read_text().strip()
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            start_date = last_date + timedelta(days=1)
            print(f"🔄 Resuming from saved progress... Starting at {start_date}")
        else:
            print("❌ Error: No start date provided and no progress file found.")
            print("Usage: python recalculate_history.py YYYY-MM-DD")
            sys.exit(1)

    end_date = date.today()
    
    engine = create_engine(str(settings.DATABASE_URL))
    Session = sessionmaker(bind=engine)
    db = Session()
    
    current_date = start_date
    
    # تطبيق الـ Monkey Patching لإلغاء الـ Scraping أثناء الحساب التاريخي
    dmu.run_historical_reports_scraper = dummy_run_historical
    dmu.update_market_pulse = dummy_update_market_pulse
    dmu.scrape_daily_details = dummy_scrape

    while current_date <= end_date:
        # التأكد من وجود بيانات تداول لهذا اليوم
        sql = text("SELECT COUNT(*) FROM prices WHERE date = :dt")
        res = db.execute(sql, {'dt': current_date}).scalar()
        
        if res > 0:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"\n=============================================")
            print(f"Recalculating for Trading Day: {date_str}")
            print(f"=============================================")
            
            try:
                dmu.update_daily(date_str)
                # حفظ التاريخ بعد نجاح اليوم بالكامل
                with open("recalculate_progress.txt", "w") as f:
                    f.write(date_str)
            except Exception as e:
                print(f"ERROR: Script failed for {date_str}: {e}")
                print("🛑 Stopping execution so you can resume later.")
                break
        else:
            print(f"Skipping {current_date} (No trading data found)")
            
        current_date += timedelta(days=1)
        
    print("\nAll historical recalculations completed!")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(sys.argv[1])
    else:
        run()
