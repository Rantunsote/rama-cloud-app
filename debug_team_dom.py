from main import SwimcloudCrawler
from bs4 import BeautifulSoup

TEAM_ID = "10034725"
# Try meets endpoint
TEAM_URL = f"https://www.swimcloud.com/team/{TEAM_ID}/meets/"

crawler = SwimcloudCrawler()
crawler.get_page(TEAM_URL)

soup = BeautifulSoup(crawler.driver.page_source, 'html.parser')

print("Title:", soup.title.string)

# Check for tabs
tabs = soup.select('ul.c-tabs li a')
print("Tabs found:", [t.get_text(strip=True) for t in tabs])

# Check for table rows
rows = soup.select('table tbody tr')
print(f"Table Rows Found: {len(rows)}")

if len(rows) > 0:
    print("First Row Content:")
    print(rows[0].prettify())

crawler.close()
