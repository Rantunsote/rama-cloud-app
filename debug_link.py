from main import SwimcloudCrawler
from bs4 import BeautifulSoup
import re

URL = "https://www.swimcloud.com/swimmer/3236804/meets/"
crawler = SwimcloudCrawler()
crawler.get_page(URL)

soup = BeautifulSoup(crawler.driver.page_source, 'html.parser')

headers = soup.select('h3.c-title')
if headers:
    h3 = headers[0]
    print("Found H3:", h3.get_text(strip=True))
    
    curr = h3
    card_div = None
    # Traverse up to find the card
    for i in range(6):
        if 'c-swimmer-meets__card' in (curr.attrs.get('class') or []):
            card_div = curr
            print("Found Card Wrapper!")
            break
        curr = curr.parent
        
    if card_div:
        print("\n--- Card HTML ---")
        print(card_div.prettify())
        
        # Check for link inside card
        link = card_div.find('a', href=re.compile(r'/results/'))
        if link:
            print(f"\nFound Link inside card: {link['href']}")
        else:
            print("\nNO LINK FOUND IN CARD.")
else:
    print("No H3 found.")

crawler.close()
