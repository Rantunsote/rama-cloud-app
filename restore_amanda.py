import sqlite3
from main import SwimcloudCrawler

def restore_amanda():
    crawler = SwimcloudCrawler()
    conn = sqlite3.connect("natacion.db")
    cursor = conn.cursor()

    print("--- Restoring Amanda Alquinta Espinoza ---")
    
    url = "https://www.swimcloud.com/swimmer/2795994/"
    correct_name = "Amanda Alquinta Espinoza"
    swimmer_id = "2795994"
        
    print(f"Processing {correct_name} (ID: {swimmer_id})...")
        
    # 1. Ensure Swimmer Exists (Scrape if needed)
    cursor.execute("SELECT name FROM swimmers WHERE id = ?", (swimmer_id,))
    row = cursor.fetchone()
    
    if not row:
        print("  > Not in DB. Scraping...")
        cursor.execute("INSERT INTO swimmers (id, name, url, team_id) VALUES (?, ?, ?, ?)",
                       (swimmer_id, correct_name, url, "10034725"))
        conn.commit()
        crawler.crawl_swimmer_meets(swimmer_id)
    else:
        print(f"  > Found as '{row[0]}'. Updating name...")
        cursor.execute("UPDATE swimmers SET name = ? WHERE id = ?", (correct_name, swimmer_id))
        conn.commit()
            
    conn.close()
    crawler.close()
    print("--- Done ---")

if __name__ == "__main__":
    restore_amanda()
