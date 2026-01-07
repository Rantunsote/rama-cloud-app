import sqlite3
import time
from main import SwimcloudCrawler

DB_PATH = 'data/natacion.db'

def get_swimcloud_swimmers():
    """Fetch swimmers that have a Swimcloud ID (not MM_)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Select IDs that are numeric (Swimcloud style)
    # MM IDs start with "MM_"
    cur.execute("SELECT id, name FROM swimmers WHERE id NOT LIKE 'MM_%'")
    rows = cur.fetchall()
    conn.close()
    return rows

def backfill():
    swimmers = get_swimcloud_swimmers()
    print(f"Found {len(swimmers)} swimmers eligible for Swimcloud backfill.")
    
    crawler = SwimcloudCrawler()
    
    # We want to be careful not to crash, so we'll wrap in try/except block in loop
    count = 0
    for s_id, s_name in swimmers:
        count += 1
        print(f"\n[{count}/{len(swimmers)}] Processing: {s_name} ({s_id})")
        
        try:
            # crawl_swimmer_meets is the method in main.py that:
            # 1. Goes to profile
            # 2. Lists meets
            # 3. If meet missing in DB, scrapes it
            crawler.crawl_swimmer_meets(s_id)
        except Exception as e:
            print(f"Error processing {s_name}: {e}")
            # Try to recover driver if needed? 
            # Usually SwimcloudCrawler handles some errors, but let's be safe.
            time.sleep(2)
            
    crawler.close()
    print("\nBackfill Complete.")

if __name__ == "__main__":
    backfill()
