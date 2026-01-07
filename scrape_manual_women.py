import sqlite3
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from main import SwimcloudCrawler, BASE_URL, TEAM_ROSTER_URL

class WomenCrawler(SwimcloudCrawler):
    def crawl_women_only(self):
        print("Navigating to roster for WOMEN scan...")
        if not self.get_page(TEAM_ROSTER_URL):
            return

        # 1. Select "All Seasons"
        try:
            wait = WebDriverWait(self.driver, 10)
            select_elem = wait.until(EC.presence_of_element_located((By.ID, "id_season_id")))
            select = Select(select_elem)
            select.select_by_value("") 
            time.sleep(3)
            print("  > Selected 'All Seasons'")
        except Exception as e:
            print(f"  > Warning: Could not select 'All Seasons': {e}")

        # 2. Click Women
        try:
            # Try finding the label by text matching
            women_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//label[contains(., 'Women')]"))
            )
            # Click it
            self.driver.execute_script("arguments[0].click();", women_btn)
            time.sleep(3)
            print("  > Switched to 'Women' tab.")
        except Exception as e:
            print(f"  > ERROR: Could not switch to Women tab: {e}")
            return

        # 3. Scroll
        self.scroll_to_bottom()
        
        # 4. Parse
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        links = soup.select('a[href^="/swimmer/"]')
        
        unique_swimmers = {}
        for link in links:
            href = link.get('href')
            name = link.get_text(strip=True)
            if not name: continue
            
            swimmer_id = href.strip('/').split('/')[-1]
            if swimmer_id.isdigit():
                unique_swimmers[swimmer_id] = {
                    'name': name,
                    'url': f"{BASE_URL}{href}",
                    'id': swimmer_id
                }

        print(f"Found {len(unique_swimmers)} WOMEN swimmers.")
        
        # 5. Crawl
        count = 0
        total = len(unique_swimmers)
        for s_id, data in unique_swimmers.items():
            print(f"[{count+1}/{total}] Processing Woman: {data['name']}")
            
            self.cursor.execute('INSERT OR IGNORE INTO swimmers (id, name, url, team_id) VALUES (?, ?, ?, ?)',
                                (s_id, data['name'], data['url'], "10034725"))
            self.conn.commit()
            
            # Reuse the existing method
            self.crawl_swimmer_meets(s_id)
            count += 1

if __name__ == "__main__":
    crawler = WomenCrawler()
    try:
        crawler.crawl_women_only()
    finally:
        crawler.close()
