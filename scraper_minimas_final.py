
import os
import sys
import time
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from normalize_events import normalize_event_name_v2

DB_PATH = "data/natacion.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS minimum_standards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            time_text TEXT,
            time_seconds REAL,
            category_code TEXT,
            gender TEXT,
            pool_size TEXT,
            source_url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_name, category_code, gender, pool_size)
        )
    """)
    conn.commit()
    conn.close()

def parse_time_str(t_str):
    try:
        if not t_str or not t_str[0].isdigit(): return None
        # Format M:S.ms or S.ms
        parts = t_str.split(':')
        if len(parts) == 2:
            return float(parts[0])*60 + float(parts[1])
        return float(t_str)
    except:
        return None

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    chrome_bin = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
    
    service = None
    if os.path.exists(chrome_bin) and os.path.exists(chromedriver_path):
        chrome_options.binary_location = chrome_bin
        service = Service(chromedriver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def scrape_page_url(driver, url, pool_label):
    print(f"Scraping ({pool_label}) {url}...")
    driver.get(url)
    time.sleep(3)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    current_cat = None
    current_gender = None
    records = []
    
    elements = soup.find_all(['h5', 'table'])
    
    for el in elements:
        if el.name == 'h5':
            txt = el.get_text(strip=True)
            if "Categoria" in txt:
                current_cat = txt.replace("Categoria", "").strip()
            elif "Femenino" in txt:
                current_gender = "F"
            elif "Masculino" in txt:
                current_gender = "M"
        
        elif el.name == 'table':
            if current_cat and current_gender:
                rows = el.select('tbody tr')
                if not rows: rows = el.select('tr')[1:]
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        event_raw = cols[0].get_text(strip=True)
                        event = normalize_event_name_v2(event_raw)
                        time_val = cols[1].get_text(strip=True)
                        
                        if event and time_val:
                            records.append({
                                'event_name': event,
                                'time_text': time_val,
                                'time_seconds': parse_time_str(time_val),
                                'category_code': current_cat,
                                'gender': current_gender,
                                'pool_size': pool_label,
                                'source_url': url
                            })
                            
    return records

def save_to_db(records):
    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    for r in records:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO minimum_standards 
                (event_name, time_text, time_seconds, category_code, gender, pool_size, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r['event_name'], r['time_text'], r['time_seconds'], r['category_code'], r['gender'], r['pool_size'], r['source_url']))
            count += 1
        except Exception as e:
            # print(f"Error saving: {e}")
            pass
            
    conn.commit()
    conn.close()
    print(f"Saved {count} records.")

def main():
    init_db()
    driver = setup_driver()
    try:
        # 117 = Infantiles, 116 = Juveniles
        # tipo=1 (50m), tipo=2 (25m)
        targets = [
            (117, 1, '50m'),
            (117, 2, '25m'),
            (116, 1, '50m'),
            (116, 2, '25m')
        ]
        
        all_recs = []
        for pid, ptype, plabel in targets:
            url = f"https://estadisticas.fechida.org/records_marcas.php?id={pid}&tipo={ptype}"
            all_recs.extend(scrape_page_url(driver, url, plabel))
        
        save_to_db(all_recs)
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
