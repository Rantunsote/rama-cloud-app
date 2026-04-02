import sqlite3
import time
import random
import sys
import re
import os
from curl_cffi import requests
from bs4 import BeautifulSoup

# Configuration
DB_NAME = "natacion.db"
# Ensure DB is in the data volume if running in Docker?
# Check where we are writing. If user maps /app/data, we should write there?
# The prompt said "main.py ... Insert each record into ... natacion.db".
# The Dockerfile VOLUME is /app/data.
# Let's write to current dir, and user maps $(pwd)/data to /app if they want.
# Or better: check if /app/data exists, else current.
DB_PATH = "data/natacion.db"
if os.path.exists("/app/data/natacion.db"):
    DB_PATH = "/app/data/natacion.db"

TEAM_ID = "10034725"
BASE_URL = "https://www.swimcloud.com"
TEAM_ROSTER_URL = f"{BASE_URL}/team/{TEAM_ID}/roster/"
EXTRA_SWIMMERS = [
    {"id": "3235418", "name": "Vicente Reyes", "url": "https://www.swimcloud.com/swimmer/3235418/"},
    {"id": "2739286", "name": "Amaro Gonzalez", "url": "https://www.swimcloud.com/swimmer/2739286/"}
]

class SwimcloudCrawler:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.setup_db()
        self.session = requests.Session(impersonate="chrome120")
        self.page_source = ""

    def setup_db(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS swimmers (
                id TEXT PRIMARY KEY,
                name TEXT,
                url TEXT,
                team_id TEXT,
                birth_date TEXT,
                gender TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS meets (
                id TEXT PRIMARY KEY,
                name TEXT,
                date TEXT,
                location TEXT,
                url TEXT,
                pool_size TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swimmer_id TEXT,
                meet_id TEXT,
                event_name TEXT,
                time TEXT,
                points TEXT,
                pool_size TEXT,
                time_url TEXT,
                place TEXT,
                FOREIGN KEY(swimmer_id) REFERENCES swimmers(id),
                FOREIGN KEY(meet_id) REFERENCES meets(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS splits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER,
                distance TEXT,
                split_time TEXT,
                cumulative_time TEXT,
                FOREIGN KEY(result_id) REFERENCES results(id)
            )
        ''')
        self.conn.commit()

    def get_page(self, url):
        print(f"Navigating to {url}...")
        try:
            response = self.session.get(url, timeout=15)
            self.page_source = response.text
            time.sleep(random.uniform(0.5, 1.0))
            return True
        except Exception as e:
            print(f"Error loading {url}: {e}")
            return False

    def scroll_to_bottom(self):
        """No longer needed since curl_cffi loads the page fully via html."""
        pass


    def crawl_roster(self, limit=None):
        unique_swimmers = {}

        def parse_links(soup_obj):
             links = soup_obj.select('a[href^="/swimmer/"]')
             for link in links:
                href = link.get('href')
                name = link.get_text(strip=True)
                swimmer_id = href.strip('/').split('/')[-1]
                if swimmer_id.isdigit():
                    unique_swimmers[swimmer_id] = {
                        'name': name.title(),
                        'url': f"{BASE_URL}{href}",
                        'id': swimmer_id
                    }

        # 1. Scrape Men
        print("  > Scraping Men's Roster...")
        if self.get_page(TEAM_ROSTER_URL + "?gender=M"):
            soup = BeautifulSoup(self.page_source, 'html.parser')
            parse_links(soup)
            print(f"  > Found {len(unique_swimmers)} so far.")

        # 2. Scrape Women
        print("  > Scraping Women's Roster...")
        if self.get_page(TEAM_ROSTER_URL + "?gender=F"):
            soup_women = BeautifulSoup(self.page_source, 'html.parser')
            parse_links(soup_women)

        print(f"Found {len(unique_swimmers)} unique swimmers in TOTAL roster.")

        # Add Extra Swimmers
        for extra in EXTRA_SWIMMERS:
            if extra['id'] not in unique_swimmers:
                 unique_swimmers[extra['id']] = extra
                 print(f"Added extra swimmer: {extra['name']}")
        
        count = 0
        for s_id, data in unique_swimmers.items():
            if limit and count >= limit:
                print(f"Limit of {limit} reached.")
                break
            
            print(f"\n[{count+1}/{len(unique_swimmers)}] Processing Swimmer: {data['name']}")
            
            self.cursor.execute('INSERT OR IGNORE INTO swimmers (id, name, url, team_id) VALUES (?, ?, ?, ?)',
                                (s_id, data['name'], data['url'], TEAM_ID))
            self.conn.commit()
            
            self.crawl_swimmer_meets(s_id)
            count += 1

    def crawl_swimmer_meets(self, swimmer_id):
        url = f"{BASE_URL}/swimmer/{swimmer_id}/meets/"
        if not self.get_page(url):
            return

        soup = BeautifulSoup(self.page_source, 'html.parser')
        
        # Debug
        meet_headers = soup.select('h3.c-title')
        print(f"  - Found {len(meet_headers)} meet headers.")
        
        processed_meets = set()

        for h3 in meet_headers:
            meet_name = h3.get_text(strip=True)
            if not meet_name: 
                continue

            # Metadata in next sibling UL
            ul = h3.find_next_sibling('ul', class_='o-list-bare')
            date_text = "Unknown"
            location_text = "Unknown"
            
            if ul:
                # Extract date and location
                text_parts = [li.get_text(strip=True) for li in ul.find_all('li')]
                # Prune 'Completed'
                clean_parts = [t for t in text_parts if "Completed" not in t and "Detailed" not in t]
                
                # Heuristic: Last is location, Second to last is date (if present).
                # The subagent showed:
                # LIST ITEM 1: UL (Completed, Date)
                # LIST ITEM 2: Location
                
                # text_parts from find_all('li') is recursive? Yes.
                # So we might get ["Completed", "Dec 5-6", "SANTIAGO"]
                if len(clean_parts) >= 1:
                    location_text = clean_parts[-1]
                if len(clean_parts) >= 2:
                    date_text = clean_parts[-2] # Assuming second to last is date
                    # Clean Swimcloud range "Dec 5-6, 2025" -> "2025-12-05"
                    match = re.search(r"([A-Za-z]+)\s+(\d+).*?,\s+(\d{4})", date_text)
                    if match:
                        month_str, day_str, year_str = match.groups()
                        try:
                            from datetime import datetime
                            dt = datetime.strptime(f"{month_str} {day_str} {year_str}", "%b %d %Y")
                            date_text = dt.strftime("%Y-%m-%d")
                        except:
                            pass
            # Result Link
            # It should be close by.
            # Look for links containing /results/ inside the same container?
            # Or just search ALL result links and match?
            # "VIII COPA ALEMANIA" might not be unique (e.g. 2024 vs 2025), so context matters.
            
            # The structure is loose. Let's try to find the link in the siblings following the H3.
            # We iterate siblings until the next H3.
            
            # Strategy: The link is likely in a wrapper or sibling of the parent.
            # Traverse up to 3 levels to find a container with a link.
            meet_link = None
            current_container = h3.parent
            for _ in range(3):
                if not current_container: break
                
                # Check if container IS the link
                if current_container.name == 'a' and '/results/' in (current_container.get('href') or ''):
                    meet_link = current_container
                    break
                
                # Check for link inside container
                found = current_container.find('a', href=re.compile(r'/results/'))
                if found:
                    meet_link = found
                    break
                    
                current_container = current_container.parent
            
            if meet_link:
                meet_url_suffix = meet_link['href']
                
                # Extract Meet ID
                parts = meet_url_suffix.strip('/').split('/')
                meet_id = "unknown"
                if 'results' in parts:
                    idx = parts.index('results')
                    if len(parts) > idx + 1:
                        meet_id = parts[idx + 1]

            # Skip if already processed for this swimmer
            # Check if we have results for this meet and swimmer
            # Efficient check: SELECT 1 FROM results WHERE meet_id = ? AND swimmer_id = ? LIMIT 1
            self.cursor.execute('SELECT 1 FROM results WHERE meet_id = ? AND swimmer_id = ? LIMIT 1', (meet_id, swimmer_id))
            if self.cursor.fetchone():
                print(f"    Skipping meet {meet_id} (Already exists for swimmer)")
                processed_meets.add(meet_id)
                continue

            if meet_id not in processed_meets:
                full_meet_url = f"{BASE_URL}{meet_url_suffix}"
            
                self.cursor.execute('INSERT OR IGNORE INTO meets (id, name, date, location, url) VALUES (?, ?, ?, ?, ?)',
                                    (meet_id, meet_name, date_text, location_text, full_meet_url))
                self.conn.commit()
                
                self.process_meet_results(meet_id, swimmer_id, full_meet_url)
                processed_meets.add(meet_id)
            else:
                 print(f"    Warning: No link found for meet '{meet_name}' (Checked 3 levels up)")


    def process_meet_results(self, meet_id, swimmer_id, url):
        self.get_page(url)
        soup = BeautifulSoup(self.page_source, 'html.parser')
        
        rows = soup.select('table.c-table-clean tbody tr')
        print(f"  > Processing Meet {meet_id}: {len(rows)} events found.")
        
        for row in rows:
            cols = row.find_all('td')
            if not cols: 
                continue
            
            # Extract basic data
            # Extract Event Name cleaner (exclude span)
            event_link = cols[0].find('a')
            if event_link:
                event_name = event_link.get_text(strip=True)
            else:
                event_name = cols[0].get_text(strip=True)
            
            # Clean up: remove "Timed Finals" suffix if stuck
            event_name = event_name.replace("Timed Finals", "").strip()
            
            final_time = "Unknown"
            time_url = ""
            points = "0"
            
            # Find time link
            for c in cols:
                a_tag = c.find('a', href=re.compile(r'/times/'))
                if a_tag:
                    final_time = a_tag.get_text(strip=True)
                    time_url = f"{BASE_URL}{a_tag['href']}"
                    break
            
            if not time_url:
                # Try finding time without link
                # (Maybe looking for points column)
                continue
            
            # Determine Pool Size
            # Priority 1: Event Name (e.g. "50 S Free", "100 L Free")
            # Priority 2: Location String
            
            pool_size = "Unknown"
            
            # Check Event Name
            if ' S ' in event_name or event_name.endswith(' S'):
                pool_size = "25m"
            elif ' L ' in event_name or event_name.endswith(' L'):
                pool_size = "50m"
            elif ' Y ' in event_name or event_name.endswith(' Y'):
                pool_size = "25y"
            
            if pool_size == "Unknown":
                # Retrieve header info from current page
                header_ul = soup.select_one('ul.o-list-inline')
                if header_ul:
                    header_text = header_ul.get_text()
                    if " SC" in header_text or "Short Course" in header_text:
                         pool_size = "25m"
                    elif " LC" in header_text or "Long Course" in header_text:
                         pool_size = "50m"
                    elif "SCM" in header_text:
                         pool_size = "25m"
                    elif "LCM" in header_text:
                         pool_size = "50m"
            
            # Update Meet Pool Size if valid
            if pool_size in ["25m", "50m"]:
                self.cursor.execute("UPDATE meets SET pool_size = ? WHERE id = ?", (pool_size, meet_id))
                self.conn.commit()

            # Extract Place (Lugar)
            # Based on DOM inspection: Last TD or Index 4.
            place = "—"
            if len(cols) >= 5:
                # Check last column
                last_col_text = cols[-1].get_text(strip=True)
                if last_col_text:
                    place = last_col_text

            # Check for duplicates
            self.cursor.execute("SELECT id FROM results WHERE swimmer_id=? AND meet_id=? AND event_name=?", (swimmer_id, meet_id, event_name))
            if self.cursor.fetchone():
                # print(f"    Skipping existing: {event_name}")
                continue

            # Save Result
            self.cursor.execute('''
                INSERT INTO results (swimmer_id, meet_id, event_name, time, points, pool_size, time_url, place)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (swimmer_id, meet_id, event_name, final_time, points, pool_size, time_url, place))
            result_id = self.cursor.lastrowid
            self.conn.commit()
            
            # Get Splits
            self.get_splits(result_id, time_url)


    def get_splits(self, result_id, url):
        # Optimistic check: Assume splits page loads fast
        self.get_page(url)
        soup = BeautifulSoup(self.page_source, 'html.parser')
        
        # Look for the splits table
        # There might be multiple tables. Look for one with numeric distance headers (50, 100)
        # or just "Split" text.
        
        # Strategy: Iterate tables
        tables = soup.select('table')
        splits_found = 0
        
        for table in tables:
            rows = table.find_all('tr')
            valid_splits = []
            
            # Must have rows
            if not rows: continue
            
            for row in rows:
                cols = row.find_all('td')
                # Needs at least distance and time
                if len(cols) >= 2:
                    dist = cols[0].get_text(strip=True)
                    split_val = cols[1].get_text(strip=True)
                    
                    # Validation: Dist starts with digit
                    if dist and (dist[0].isdigit()):
                         valid_splits.append((dist, split_val))
            
            if len(valid_splits) > 0:
                # Looks like a split table
                for dist, val in valid_splits:
                    self.cursor.execute('INSERT INTO splits (result_id, distance, split_time) VALUES (?, ?, ?)',
                                        (result_id, dist, val))
                splits_found = len(valid_splits)
                self.conn.commit()
                break # Stop after finding one valid table

        if splits_found:
            print(f"    + Saved {splits_found} splits.")

    def close(self):
        self.session.close()
        self.conn.close()

def main(limit=None):
    crawler = SwimcloudCrawler()
    try:
        crawler.crawl_roster(limit=limit)
    except KeyboardInterrupt:
        print("\\nStopping crawler...")
    finally:
        crawler.close()

if __name__ == "__main__":
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except:
            pass
    main(limit=limit)
