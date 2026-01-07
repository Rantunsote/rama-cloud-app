
import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta

DB_PATH = "data/natacion.db"

def parse_date(date_str):
    # Try different formats
    # DB has: "Sep 8–11, 2022" (Swimcloud), "2025-12-05" (Meet Mobile)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        pass
        
    return None

def detect():
    conn = sqlite3.connect(DB_PATH)
    meets = pd.read_sql("SELECT id, name, date FROM meets", conn)
    conn.close()
    
    # Preprocess dates
    # We add a 'parsed_date' column
    # For Swimcloud "Dec 10–13, 2025", we just want "2025-12-10"
    
    parsed_dates = []
    for d in meets['date']:
        try:
            # Try ISO first
            dt = datetime.strptime(d, "%Y-%m-%d")
            parsed_dates.append(dt)
            continue
        except:
            pass
            
        try:
            # Try "Dec 5–6, 2025" or "Dec 5, 2025"
            # Split by comma
            parts = d.split(',')
            if len(parts) == 2:
                year = int(parts[1].strip())
                # "Dec 5–6" -> "Dec 5"
                month_day = parts[0].split('–')[0].split('-')[0].strip() # Handle en-dash or hyphen
                dt = datetime.strptime(f"{month_day} {year}", "%b %d %Y")
                parsed_dates.append(dt)
            else:
                 parsed_dates.append(datetime(1900, 1, 1))
        except Exception as e:
            # print(f"Failed to parse: {d}")
            parsed_dates.append(datetime(1900, 1, 1))
            
    meets['dt'] = parsed_dates
    
    # Sort
    meets = meets.sort_values('dt')
    
    candidates = []
    
    # Window comparison
    rows = meets.to_dict('records')
    for i in range(len(rows)):
        for j in range(i + 1, min(i + 5, len(rows))): # Look ahead 5 meets
            m1 = rows[i]
            m2 = rows[j]
            
            # Date Check (within 14 days)
            delta = abs((m1['dt'] - m2['dt']).days)
            if delta > 14:
                continue
                
            # Name Check
            ratio = fuzz.token_set_ratio(m1['name'], m2['name'])
            
            if ratio > 65: # Threshold
                candidates.append({
                    "Meet 1": f"{m1['name']} ({m1['id']})",
                    "Date 1": m1['date'],
                    "Meet 2": f"{m2['name']} ({m2['id']})",
                    "Date 2": m2['date'],
                    "Score": ratio
                })
                
    # Print Report
    print(f"Found {len(candidates)} potential duplicates:\n")
    for c in candidates:
        print(f"[{c['Score']}% Match]")
        print(f"  A: {c['Meet 1']} [{c['Date 1']}]")
        print(f"  B: {c['Meet 2']} [{c['Date 2']}]")
        print("-" * 40)

if __name__ == "__main__":
    detect()
