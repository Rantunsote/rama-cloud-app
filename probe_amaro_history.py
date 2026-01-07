from main import SwimcloudCrawler
from bs4 import BeautifulSoup
import time

SWIMMER_ID = "2739286" # Amaro Gonzalez
URL = f"https://www.swimcloud.com/swimmer/{SWIMMER_ID}/meets/"

crawler = SwimcloudCrawler()
print(f"Checking Swimcloud history for Amaro ({SWIMMER_ID})...")
crawler.get_page(URL)
time.sleep(2)

soup = BeautifulSoup(crawler.driver.page_source, 'html.parser')
headers = soup.select('h3.c-title')

found_2024 = False
print("\n--- Meets Found ---")
for h3 in headers:
    meet_name = h3.get_text(strip=True)
    # Check date in sibling
    ul = h3.find_next_sibling('ul', class_='o-list-bare')
    date_text = ""
    if ul:
        date_text = ul.get_text(separator=" ", strip=True)
    
    print(f"Meet: {meet_name} | Info: {date_text}")
    if "2024" in date_text:
        found_2024 = True

print(f"\n2024 Data Available: {found_2024}")
crawler.close()
