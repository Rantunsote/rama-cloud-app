import sqlite3
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import os
import sys
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

# Configuration
DB_PATH = "data/natacion.db"
START_EVENT_ID = 325 # Start high and go down
END_EVENT_ID = 300   # How far back to look

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Check for Docker environment variables (same as main.py)
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

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def normalize_name(name):
    """Normalize name for comparison: UPPERCASE, strip accents, standard spacing"""
    if not name: return ""
    name = name.upper().strip()
    # Simple accent removal
    replacements = (("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U"), ("Ñ", "N"))
    for a, b in replacements:
        name = name.replace(a, b)
    # Remove extra spaces
    return " ".join(name.split())

def similar(a, b):
    """Check similarity ratio"""
    return SequenceMatcher(None, a, b).ratio()

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Load target swimmers
    print("Loading swimmers from DB...")
    cursor.execute("SELECT id, name, birth_date FROM swimmers")
    swimmers = []
    for row in cursor.fetchall():
        # If birth_date is already present, maybe skip? For now, we update everything to be sure.
        swimmers.append({'id': row[0], 'name': row[1], 'norm_name': normalize_name(row[1])})
    
    print(f"Loaded {len(swimmers)} target swimmers from database.")
    
    driver = setup_driver()
    matched_count = 0
    
    try:
        # 2. Iterate through events (2025 List)
        # IDs gathered: 322, 269, 263, 257, 246, 236, 266, 225, 224, 220, 221, 222, 205
        # Also keep recent range just in case? Or specific list is better.
        target_events = [322, 269, 263, 257, 246, 236, 266, 225, 224, 220, 221, 222, 205]
    
        for event_id in target_events:
            url = f"https://estadisticas.fechida.org/estadisticas_atletas.php?evento={event_id}"
            print(f"\nChecking Event {event_id}: {url}")
            
            try:
                driver.get(url)
                time.sleep(2) # Wait for load using simple sleep for robustness vs wait conditions on simple pages
                
                # Check if we need to expand "Show N entries"
                # Some tables might be paginated or limited.
                # However, basic scraping often gets the DOM. Let's inspect the page source directly.
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Check if event has data (sometimes it's empty)
                rows = soup.select('table tbody tr')
                if not rows or "No data available" in rows[0].text:
                    print("  No data found or empty table.")
                    continue
                
                print(f"  Found {len(rows)} rows in table.")
                
                # 3. Parse and Match
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 5: continue
                    
                    # Columns usually: #, Nadador(a), Genero, Fec Nac, Club, ...
                    # Let's verify column index dynamically if headers exist? 
                    # Assuming standard layout from observation:
                    # Col 1 (index 1): Nadador Name "LASTNAME, Firstname"
                    # Col 3 (index 3): Fec Nac "YYYY-MM-DD"
                    
                    raw_name = cols[1].get_text(strip=True)
                    dob = cols[3].get_text(strip=True)
                    
                    # Fechida format is "LASTNAME, Firstname". DB is "Firstname Lastname" usually or mixed.
                    # Let's normalize both to handle "LASTNAME FIRSTNAME"
                    
                    # Convert Fechida "PEREZ, Juan" -> "JUAN PEREZ" for normalization
                    if ',' in raw_name:
                        parts = raw_name.split(',')
                        if len(parts) >= 2:
                            formatted_fechida_name = f"{parts[1]} {parts[0]}"
                        else:
                            formatted_fechida_name = raw_name
                    else:
                        formatted_fechida_name = raw_name
                        
                    norm_fechida = normalize_name(formatted_fechida_name)
                    
                    # --- MATCHING LOGIC ---
                    for swimmer in swimmers:
                        # Exact match on normalized
                        # Or partial match if very high confidence
                        
                        # Try exact match first
                        match = False
                        if swimmer['norm_name'] in norm_fechida or norm_fechida in swimmer['norm_name']:
                            # Check similarity to be safe
                            if similar(swimmer['norm_name'], norm_fechida) > 0.8:
                                match = True
                        
                        # Fallback: Check if names are swapped? (Already handled by comma split above)
                        
                        if match:
                            # Update DB
                            if not dob or dob == '0000-00-00':
                                continue
                            
                            print(f"    MATCH! {swimmer['name']} -> {dob} (Event {event_id})")
                            cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (dob, swimmer['id']))
                            conn.commit()
                            
                            # Remove from list to avoid re-checking? 
                            # No, keep checking in case a better date appears? 
                            # Actually remove to speed up
                            swimmers.remove(swimmer)
                            matched_count += 1
                            break # Stop checking other swimmers for this row
                            
                if len(swimmers) == 0:
                    print("All swimmers matched! Exiting.")
                    break
                    
            except Exception as e:
                print(f"  Error scraping event {event_id}: {e}")
                
    finally:
        driver.quit()
        conn.close()
        print(f"\nScraping finished. Total updates: {matched_count}")

if __name__ == "__main__":
    main()
