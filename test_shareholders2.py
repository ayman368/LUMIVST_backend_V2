import sys
sys.path.append('.')
from selenium.webdriver.common.by import By
from app.services.daily_detailed_scraper import build_driver
import time

driver = build_driver(headless=True)
try:
    url = 'https://www.saudiexchange.sa/Resources/Reports-v2/MajorStakeHoldersPage_en.html'
    driver.get(url)
    time.sleep(10)
    
    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"Total tables: {len(tables)}")
    
    for tbl in tables:
        rows = tbl.find_element(By.TAG_NAME, "tbody").find_elements(By.TAG_NAME, "tr")
        if len(rows) > 5:
            print(f"Found table with {len(rows)} rows.")
            for i, row in enumerate(rows[:5]):
                cols = row.find_elements(By.TAG_NAME, "td")
                print(f"Row {i} has {len(cols)} columns")
                for j, col in enumerate(cols):
                    print(f"  Col {j}: '{col.text.strip().replace(chr(10), ' ')}'")
            break

finally:
    driver.quit()
