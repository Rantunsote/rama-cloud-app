import sqlite3
from main import SwimcloudCrawler

# User Provided Map: {Link -> Correct Name}
RESTORE_LIST = [
    {"url": "https://www.swimcloud.com/swimmer/2466453/", "name": "Juan Ignacio Polverelli Castro"},
    {"url": "https://www.swimcloud.com/swimmer/3233660/", "name": "Amanda Alquinta Espinoza"},
    {"url": "https://www.swimcloud.com/swimmer/3234652/", "name": "Victoria Pericana Molina"},
    {"url": "https://www.swimcloud.com/swimmer/2466400/", "name": "Baltazar Aguirre Pizarro"},
    {"url": "https://www.swimcloud.com/swimmer/3236839/", "name": "Florencia Varas Arriagada"},
    {"url": "https://www.swimcloud.com/swimmer/3211554/", "name": "Isidora Navarrete"}
]

def restore_swimmers():
    crawler = SwimcloudCrawler()
    conn = sqlite3.connect("natacion.db")
    cursor = conn.cursor()

    print("--- Restoring & Correcting Swimmers ---")
    
    for item in RESTORE_LIST:
        url = item['url']
        correct_name = item['name']
        swimmer_id = url.strip('/').split('/')[-1]
        
        print(f"Processing {correct_name} (ID: {swimmer_id})...")
        
        # 1. Ensure Swimmer Exists (Scrape if needed)
        cursor.execute("SELECT name FROM swimmers WHERE id = ?", (swimmer_id,))
        row = cursor.fetchone()
        
        if not row:
            print("  > Not in DB. Scraping...")
            # We insert a placeholder to allow crawl_swimmer_meets to work?
            # actually crawl_swimmer_meets assumes swimmer exists in DB? 
            # Look at main.py: process_meet_results inserts results with FK.
            # So we MUST insert swimmer first.
            
            # Insert with correct name
            cursor.execute("INSERT INTO swimmers (id, name, url, team_id) VALUES (?, ?, ?, ?)",
                           (swimmer_id, correct_name, url, "10034725"))
            conn.commit()
            
            # Scrape Meest
            crawler.crawl_swimmer_meets(swimmer_id)
        else:
            print(f"  > Found in DB as '{row[0]}'. Updating name...")
            cursor.execute("UPDATE swimmers SET name = ? WHERE id = ?", (correct_name, swimmer_id))
            conn.commit()
            
    conn.close()
    crawler.close()
    print("--- Done ---")

if __name__ == "__main__":
    restore_swimmers()
