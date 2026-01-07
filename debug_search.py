from main import SwimcloudCrawler
from bs4 import BeautifulSoup
import urllib.parse

SEARCH_TERM = "Copa Colina"
# Try standard search
SEARCH_URL = f"https://www.swimcloud.com/meets/?q={urllib.parse.quote(SEARCH_TERM)}"

crawler = SwimcloudCrawler()
crawler.get_page(SEARCH_URL)

soup = BeautifulSoup(crawler.driver.page_source, 'html.parser')
print("Title:", soup.title.string)

# Look for search results
# Usually a list of meets
links = soup.select('a[href*="/results/"]')
print(f"Result Links Found: {len(links)}")

for l in links[:5]:
    print(l.get_text(strip=True), "->", l['href'])

crawler.close()
