import os
import sys
import time
import sqlite3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

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

def scrape_minimas():
    driver = setup_driver()
    try:
        url = "https://estadisticas.fechida.org/"
        print(f"Navigating to {url}")
        driver.get(url)
        time.sleep(3)
        
        # Click "Marcas Mínimas 2025"
        try:
            print("Finding 'Marcas Mínimas 2025' link...")
            # Dump links to find exact text
            links = driver.find_elements(By.TAG_NAME, "a")
            target = None
            for l in links:
                if "Marcas Mínimas 2025" in l.text:
                    target = l
                    break
            
            if target:
                target.click()
                time.sleep(2)
                print("Clicked 'Marcas Mínimas 2025'. searching for sub-menus...")
                
                # Check for "Infantiles"
                sub_links = driver.find_elements(By.TAG_NAME, "a")
                infantiles = None
                juveniles = None
                
                for l in sub_links:
                    txt = l.text.strip()
                    if "Infantiles" in txt: infantiles = l
                    if "Juveniles" in txt: juveniles = l
                    
                if infantiles:
                    print("Found 'Infantiles'. Clicking...")
                    infantiles.click()
                    time.sleep(3)
                    print(f"Current URL: {driver.current_url}")
                    
                    # Parse Table
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    
                    # Inspect Tabs
                    tabs = [t.get_text(strip=True) for t in soup.select("ul.nav-tabs li a")]
                    print(f"Tabs Found: {tabs}")
                    
                    # Inspect Headers hierarchy
                    # Traverse all headers
                    all_headers = soup.find_all(['h1','h2','h3','h4','h5', 'strong'])
                    print("Page Headings sequence:")
                    for h in all_headers[:20]:
                        print(f"  {h.name}: {h.get_text(strip=True)}")
                        
                    tables = soup.select("table")
                    print(f"Found {len(tables)} tables in Infantiles.")

                    for i, table in enumerate(tables):
                        # Try to find a label
                        prev = table.find_previous(["h3", "h4", "h5", "strong", "p"])
                        label = prev.get_text(strip=True) if prev else "No Label"
                        print(f"Table {i} Label: {label}")
                        
                        # Headers
                        headers = [th.get_text(strip=True) for th in table.select("thead th")]
                        if not headers:
                            headers = [td.get_text(strip=True) for td in table.select("tr:first-child td")]
                        print(f"  Headers: {headers}")
                        
                else:
                    print("Infantiles link not found.")
                
                # ID Discovery
                for test_id in [116, 118, 119]:
                    test_url = f"https://estadisticas.fechida.org/records_marcas.php?id={test_id}&tipo=2"
                    print(f"Checking URL: {test_url}")
                    driver.get(test_url)
                    time.sleep(2)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    h5s = soup.find_all('h5')
                    if h5s:
                        print(f"  Headings for ID {test_id}:")
                        for h in h5s[:3]:
                            print(f"    {h.get_text(strip=True)}")
                    else:
                        print(f"  No headings for ID {test_id}.")
                        
                    tables = soup.select("table")
                    print(f"  Tables found: {len(tables)}")




                
        except Exception as e:
            print(f"Error: {e}")
            
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_minimas()
