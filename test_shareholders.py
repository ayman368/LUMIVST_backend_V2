import sys
sys.path.append('.')
from selenium.webdriver.common.by import By
from app.services.daily_detailed_scraper import build_driver
import time
import os

os.system("taskkill /f /im chromedriver.exe")

driver = build_driver(headless=True)
try:
    url = 'https://www.saudiexchange.sa/Resources/Reports-v2/MajorStakeHoldersPage_en.html'
    driver.get(url)
    time.sleep(10)
    
    tables = driver.find_elements(By.TAG_NAME, "table")
    print(f"Total tables found (tagName='table'): {len(tables)}")
    
    for i, tbl in enumerate(tables):
        rows = tbl.find_element(By.TAG_NAME, "tbody").find_elements(By.TAG_NAME, "tr")
        print(f"Table {i} has {len(rows)} rows.")
        if len(rows) > 0:
            for j, row in enumerate(rows[:3]):
                cols = row.find_elements(By.TAG_NAME, "td")
                print(f"  Row {j} has {len(cols)} columns.")
                print(f"  Texts: {[c.text.strip().replace(chr(10), ' ') for c in cols]}")

finally:
    driver.quit()
