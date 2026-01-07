from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

def debug():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    url = "https://www.swimcloud.com/results/366853/swimmer/3211584/"
    print(f"Navigating to {url}...")
    driver.get(url)
    time.sleep(5)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    rows = soup.select('table.c-table-clean tbody tr')
    if rows:
        print(f"Found {len(rows)} rows. First row:")
        print(rows[0].prettify())
        
        cols = rows[0].find_all('td')
        for i, td in enumerate(cols):
            print(f"Col {i}: '{td.get_text(strip=True)}'")
            print(td.prettify())
    else:
        print("No rows found")


    driver.quit()

if __name__ == "__main__":
    debug()
