import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz
from datetime import datetime
import sys
import os
from bs4 import BeautifulSoup
import re
import time

# Import crawler 
from main import SwimcloudCrawler

DB_PATH = 'data/natacion.db'
BASE_URL = "https://www.swimcloud.com"

def get_connection():
    return sqlite3.connect(DB_PATH)

def parse_date(date_str):
    if not date_str or date_str == "Unknown": return None
    
    # Try Direct ISO first (YYYY-MM-DD)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except:
        pass
    
    # Handle ranges: "Dec 5-6, 2025" -> "Dec 5, 2025"
    import re
    
    # Check for en-dash or other dashes
    # Normalize dashes first to simple hyphen
    date_str = date_str.replace('–', '-').replace('—', '-')
    date_str = re.sub(r'(\d+)-\d+', r'\1', date_str)

    # Clean month names (Manual)
    months = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12 
    }
    
    # Parse "Dec 5, 2025" manually
    # Regex: (Month) (Day), (Year)
    match = re.search(r'([A-Z][a-z]{2})\s(\d{1,2}),\s(\d{4})', date_str)
    if match:
        mon_str, day_str, year_str = match.groups()
        if mon_str in months:
            res = datetime(int(year_str), months[mon_str], int(day_str)).date()
            # print(f"  -> Parsed {original} as {res}")
            return res
            
    # print(f"  -> Failed to parse {original}")
    return None

def normalize_date_match(db_date, sc_date_text):
    d1 = parse_date(db_date)
    d2 = parse_date(sc_date_text)
    print(f"    [DateCheck] D1: {d1} (from {db_date}) | D2: {d2} (from {sc_date_text})")
    
    if not d1 or not d2: return False
    return abs((d1 - d2).days) <= 1

def enrich_pools():
    crawler = SwimcloudCrawler()
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Get MM Meets that need checking
    cursor.execute("SELECT id, name, date, pool_size FROM meets WHERE id LIKE 'MM_%'")
    mm_meets = cursor.fetchall()
    
    if not mm_meets:
        print("No MM meets.")
        return

    # 2. Get Proxy Swimmers (Active ones)
    proxies = [
        "Emilio Reyes", 
        "Martin Quijada", 
        "Borja Studer",
        "Amalia Alquinta",
        "Vicente Silva"
    ] 
    
    sc_meets = []
    
    unique_urls = set()
    
    for proxy_name in proxies:
        print(f"\n--- Checking Proxy: {proxy_name} ---")
        cursor.execute("SELECT id, name, url FROM swimmers WHERE name LIKE ?", (f"%{proxy_name}%",))
        res = cursor.fetchone()
        
        if not res:
            print(f"Swimmer {proxy_name} not found in local DB.")
            continue
            
        s_id, s_name, s_url = res
        url = f"{BASE_URL}/swimmer/{s_id}/meets/"
        print(f"Scraping: {url}")
        
        crawler.get_page(url)
        time.sleep(2) # Wait for load
        soup = BeautifulSoup(crawler.driver.page_source, 'html.parser')
        
        headers = soup.select('h3.c-title')
        print(f"Found {len(headers)} meets (h3) on profile.")

        for h3 in headers:
            m_name = h3.get_text(strip=True)
            if not m_name: continue
            
            # Find Wrapper Card (traverse up)
            card = None
            curr = h3
            for _ in range(5):
                if 'c-swimmer-meets__card' in (curr.attrs.get('class') or []):
                    card = curr
                    break
                curr = curr.parent
            
            if not card: continue
            
            # Find Link inside Card
            m_link = None
            link_tag = card.find('a', href=re.compile(r'/results/'))
            if link_tag:
                raw_link = link_tag['href']
                # Clean URL to get meet base: /results/12345/
                parts = raw_link.strip('/').split('/')
                if 'results' in parts:
                    idx = parts.index('results')
                    if len(parts) > idx + 1:
                        meet_id = parts[idx+1]
                        m_link = f"/results/{meet_id}/"
            
            # Date
            m_date = "Unknown"
            ul = card.select_one('ul.o-list-bare')
            if ul:
                all_text = ul.get_text(separator=" ", strip=True)
                # Regex logic...
                
                # Check for year 2023, 2024, 2025
                match = re.search(r'([A-Z][a-z]{2}\s\d{1,2}(?:[–-]\d{1,2})?,\s\d{4})', all_text)
                if match:
                    m_date = match.group(1)
            
            if m_link and m_link not in unique_urls:
                full_link = f"{BASE_URL}{m_link}" if m_link.startswith('/') else m_link
                sc_meets.append({
                    "name": m_name,
                    "date": m_date,
                    "url": full_link
                })
                unique_urls.add(m_link)

    print(f"\nCollected {len(sc_meets)} potential matches from proxies.")
    for m in sc_meets:
        print(f"  SC: {m['name']} | Date: {m['date']} | URL: {m['url']}")
    
    # 3. Match and Enrich
    for mm in mm_meets:
        mm_id, mm_name, mm_date, mm_pool = mm
        if not mm_pool: mm_pool = "25m"
        
        print(f"\nChecking MM Meet: {mm_name} ({mm_date}) [{mm_pool}]")
        
        match = None
        for sc in sc_meets:
            is_date_ok = normalize_date_match(mm_date, sc['date'])
            ratio = fuzz.token_sort_ratio(mm_name, sc['name'])
            
            print(f"    Comparing with {sc['name']} ({sc['date']}) -> DateMatch: {is_date_ok}, Fuzz: {ratio}")
            
            if is_date_ok:
                if ratio > 50:
                    match = sc
                    print(f"  -> Match Found: {sc['name']} ({sc['date']}) [{ratio}%]")
                    break
        
        if match:
            # Visit Logic
            print(f"  -> Visiting Swimcloud: {match['url']}")
            crawler.get_page(match['url'])
            msoup = BeautifulSoup(crawler.driver.page_source, 'html.parser')
            
            pool_detected = None
            # Check Header
            header_ul = msoup.select_one('ul.o-list-inline')
            if header_ul:
                ht = header_ul.get_text()
                if "LCM" in ht or "Long Course" in ht: pool_detected = "50m"
                elif "SCM" in ht or "Short Course" in ht: pool_detected = "25m"
            
            if not pool_detected:
                # Check Event Names in Table
                event_row = msoup.select_one('table tbody tr td a')
                if event_row:
                    txt = event_row.get_text()
                    if " L " in txt or " LCM" in txt: pool_detected = "50m"
                    elif " S " in txt or " SC" in txt: pool_detected = "25m"

            if pool_detected:
                print(f"    * Detected Pool: {pool_detected}")
                if pool_detected != mm_pool:
                     print(f"    ! UPDATING {mm_pool} -> {pool_detected}")
                     cursor.execute("UPDATE meets SET pool_size = ? WHERE id = ?", (pool_detected, mm_id))
                     cursor.execute("UPDATE results SET pool_size = ? WHERE meet_id = ?", (pool_detected, mm_id))
                     conn.commit()
            else:
                print("    ? Could not detect pool size.")
        
    conn.close()
    crawler.close()

if __name__ == "__main__":
    enrich_pools()
