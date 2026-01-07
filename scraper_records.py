import os
import sys
import time
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# DB Setup
DB_PATH = 'data/natacion.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Check for Docker environment variables
    chrome_bin = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
    
    service = None
    if os.path.exists(chrome_bin) and os.path.exists(chromedriver_path):
        print("Using system Chromium (Docker environment detected).")
        chrome_options.binary_location = chrome_bin
        service = Service(chromedriver_path)
    else:
        print("Using WebDriverManager (Local environment detected).")
        try:
            service = Service(ChromeDriverManager().install())
        except Exception as e:
            print(f"Error setting up local driver: {e}")
            sys.exit(1)

    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def scrape_records_page(driver, pool_size, url):
    # IGNORE URL param, go to home and navigate
    print(f"Scraping {pool_size} records...")
    
    # 1. Home
    driver.get("https://estadisticas.fechida.org/")
    time.sleep(3)
    
    driver.get("https://estadisticas.fechida.org/")
    time.sleep(5)
    
    try:
        print("Clicking 'Records nacionales'...")
        # Use simple find_element
        links = driver.find_elements(By.TAG_NAME, "a")
        target = None
        for l in links:
            if "Records nacionales" in l.text:
                target = l
                break
        
        if target:
            target.click()
        else:
            print("Link text not found in any <a>.")
            # Try javascript click on hidden element if needed?
            return []
            
        time.sleep(5)
    except Exception as e:
        print(f"Error clicking menu: {e}")
        return []
             
    # 3. Choose Pool Size
             
    # 3. Choose Pool Size
    # "Piscina larga" = 50m, "Piscina corta" = 25m
    target_text = "Piscina larga" if pool_size == "50m" else "Piscina corta"
    try:
        print(f"Selecting {target_text}...")
        pool_link = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{target_text}')]"))
        )
        pool_link.click()
        time.sleep(3)
    except Exception as e:
        print(f"Could not click {target_text}: {e}")
        return []

    print(f"Current URL: {driver.current_url}")
    
    # Categories to scrape
    # Use simpler keys since "años" might have specific encoding
    categories_map = {
        "11-12": "11-12 años", 
        "13-14": "13-14 años", 
        "15-17": "15-17 años", 
        "ABSOLUTO": "ABSOLUTO"
    }
    
    records_found = []
    
    for key, full_name in categories_map.items():
        cat = full_name
        try:
            print(f"  Switching to Category: {full_name} (checking '{key}')")
            # Robust click
            # Try by text content relative
            # //a[contains(., '11-12')]
            try:
                link = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, f"//a[contains(text(), '{key}')]"))
                )
                link.click()
            except:
                print(f"    Link for {key} not found/clickable. Dumping links:")
                links = driver.find_elements(By.TAG_NAME, "a")
                for l in links: 
                    t = l.text.strip()
                    if t: print(f"LINK: '{t}'")
                pass
            
            time.sleep(2) # Wait for JS update
            
            # Parse Table
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            tables = soup.select("table")
            print(f"    Found {len(tables)} tables.")
            
            # Smart parsing logic
            if len(tables) == 8:
                # Assume all categories are present: 11-12, 13-14, 15-17, Absoluto (2 tables each)
                # But we are inside a loop that iterates categories.
                # If we process all 8 tables now, we should break the loop or ensure we don't duplicate.
                
                # Check if we are in the first iteration (checking '11-12')
                if key == "11-12":
                    print("    Detected 8 tables. Parsing ALL categories at once...")
                    
                    mapping = [
                        ("11-12", 0, 1),
                        ("13-14", 2, 3),
                        ("15-17", 4, 5),
                        ("ABSOLUTO", 6, 7)
                    ]
                    
                    for c_name, t_fem, t_mal in mapping:
                        # Use mapped names
                        cat_real = categories_map.get(c_name, c_name)
                        parse_table(tables[t_fem], pool_size, cat_real, "F", records_found)
                        parse_table(tables[t_mal], pool_size, cat_real, "M", records_found)
                    
                    # We scraped everything. We can stop the loop?
                    # return records_found? No, we need to let the loop finish or break.
                    # But the loop will run for other keys and scrape 11-12 again (as mislabeled).
                    # We should maintain a 'scraped_all' flag?
                    # Or just break?
                    break
                else:
                    # If we are in later iterations and still see 8 tables, we already scraped them in "11-12".
                    # So specific logic for duplicate avoidance?
                    # But the loop "continues" (was 'pass').
                    pass
            
            # Fallback for normal specific tab parsing (if len != 8 or not 11-12)
            active_pane = soup.select_one("div.tab-pane.active")
            if active_pane:
                tables_active = active_pane.select("table")
                if len(tables_active) >= 2:
                    parse_table(tables_active[0], pool_size, cat, "F", records_found)
                    parse_table(tables_active[1], pool_size, cat, "M", records_found)
            elif len(tables) >= 2 and key != "11-12" and len(tables) != 8:
                 # If we are somehow here and didn't trigger the 8-table logic?
                 pass


                
        except Exception as e:
            print(f"    Error scraping category {cat}: {e}")

    return records_found

from normalize_events import normalize_event_name_v2

def parse_table(table, pool, cat, gender, list_out):
    rows = table.select('tbody tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 3: continue
        
        raw_event = cols[0].get_text(strip=True)
        event = normalize_event_name_v2(raw_event)
        
        time_val = cols[1].get_text(strip=True)
        name = cols[2].get_text(strip=True)
        date_loc = cols[3].get_text(strip=True) if len(cols) > 3 else ""
        
        list_out.append((event, pool, gender, cat, time_val, name, date_loc))

def save_to_db(records):
    conn = get_db_connection()
    c = conn.cursor()
    # Clear old records? Or upsert?
    # Let's Clear for now to avoid dupes (it's a full scrape)
    # Create table if not exists
    c.execute("""
        CREATE TABLE IF NOT EXISTS national_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            pool_size TEXT,
            gender TEXT,
            category_code TEXT,
            time TEXT,
            swimmer_name TEXT,
            date TEXT
        )
    """)
    c.execute("DELETE FROM national_records")
    
    c.executemany("""
        INSERT INTO national_records (event_name, pool_size, gender, category_code, time, swimmer_name, date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records)
    
    conn.commit()
    conn.close()
    print(f"Saved {len(records)} records to DB.")

def main():
    driver = setup_driver()
    all_records = []
    
    try:
        # 50m
        url_50 = "https://estadisticas.fechida.org/records_marcas.php?id=115&tipo=1"
        all_records.extend(scrape_records_page(driver, "50m", url_50))
        
        # 25m
        url_25 = "https://estadisticas.fechida.org/records_marcas.php?id=115&tipo=2"
        all_records.extend(scrape_records_page(driver, "25m", url_25))
        
    finally:
        driver.quit()
    
    save_to_db(all_records)

if __name__ == "__main__":
    main()
