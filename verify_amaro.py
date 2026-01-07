from main import SwimcloudCrawler

def scrape_amaro():
    crawler = SwimcloudCrawler()
    amaro_id = "2739286"
    print(f"Scraping Amaro Gonzalez ({amaro_id})...")
    
    # Ensure he is in the swimmers table first
    crawler.cursor.execute('INSERT OR IGNORE INTO swimmers (id, name, url, team_id) VALUES (?, ?, ?, ?)',
                           (amaro_id, "Amaro Gonzalez", "https://www.swimcloud.com/swimmer/2739286/", "10034725"))
    crawler.conn.commit()
    
    # Crawl Meets
    crawler.crawl_swimmer_meets(amaro_id)
    crawler.close()
    print("Amaro scraped.")

if __name__ == "__main__":
    scrape_amaro()
