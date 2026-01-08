import sqlite3
import csv
import re
import os

# --- Configuration ---
DB_PATH = "data/natacion.db"
CSV_PATH = "isidora_check.csv"

# --- Normalization Helpers (Adapted from app.py) ---
def normalize_scraped_event_name(raw_name):
    """
    Normalizes 'Mujeres 9-10 400 Metro Libre' -> '400 Free'
    """
    if not isinstance(raw_name, str):
        return raw_name
        
    name = raw_name.lower()
    is_relay = "relevo" in name or "relay" in name
    
    # Extract Distance
    dist_match = re.search(r'(\b|^)(25|50|100|200|400|800|1500)(\b|$)', name)
    if not dist_match:
        if "4 x 50" in name or "4x50" in name: distance = "200"
        elif "4 x 100" in name or "4x100" in name: distance = "400"
        else: return raw_name
    else:
        distance = dist_match.group(2)
    
    # Extract Style
    style = None
    if "libre" in name or "free" in name: style = "Free"
    elif "espalda" in name or "back" in name: style = "Back"
    elif "pecho" in name or "breast" in name: style = "Breast"
    elif "mariposa" in name or "fly" in name: style = "Fly"
    elif "combinado" in name or "medley" in name or "ci" in name or "im" in name:
        style = "IM"
        if is_relay: style = "Medley"
        
    if not style: return raw_name
        
    final_name = f"{distance} {style}"
    if is_relay: final_name += " Relay"
    return final_name

def parse_time(time_str):
    """Parses '1:21,60' or '34,20' into total seconds."""
    if not time_str: return 0.0
    t = time_str.replace(',', '.').strip()
    try:
        if ':' in t:
            parts = t.split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(t)
    except:
        return 0.0

def normalize_name(name):
    return name.strip().lower()

# --- Main Script ---

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Load all DB results into memory for fast lookup
# Key: (swimmer_name_norm, event_norm, time_seconds) -> Count
print("Loading DB results...")
db_results = []
cursor.execute("""
    SELECT s.name, r.event_name, r.time 
    FROM results r 
    JOIN swimmers s ON r.swimmer_id = s.id
""")
rows = cursor.fetchall()
for r in rows:
    s_name = normalize_name(r[0])
    # DB event names are usually "400 Free" etc (standardized) OR original scraped names?
    # Actually current DB has mixed. Some are "400 Free", some "400 metros Libre".
    # Let's normalize both sides to be safe.
    evt_norm = normalize_scraped_event_name(r[1])
    t_seconds = parse_time(r[2])
    db_results.append((s_name, evt_norm, f"{t_seconds:.2f}"))

# Create a set for O(1) lookups
db_set = set(db_results)
print(f"Loaded {len(db_set)} unique results from DB.")

print(f"Reading CSV: {CSV_PATH}...")
missing_count = 0
mismatch_count = 0
total_checked = 0

with open(CSV_PATH, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    print(f"CSV Headers: {reader.fieldnames}")
    
    for row in reader:
        # CSV Cols: Nombre, Apellido, Edad, Equipo, Competencia, Prueba, Tiempo
        first = row.get('Nombre', '')
        last = row.get('Apellido', '')
        full_name = f"{first} {last}".strip()
        
        # Check if swimmer exists in DB first? 
        # Actually user wants to know if RESULTS are missing.
        
        raw_event = row.get('Prueba', '')
        raw_time = row.get('Tiempo', '')
        
        # Normalize
        s_name_norm = normalize_name(full_name)
        evt_norm = normalize_scraped_event_name(raw_event)
        t_val = parse_time(raw_time)
        t_str = f"{t_val:.2f}"
        
        total_checked += 1
        
        # Check Existance
        key = (s_name_norm, evt_norm, t_str)
        
        # Fuzzy check for name? (e.g. 'Isidora Navarrete' vs 'Isidora Navarrete Bustamante')
        # Simple containment check
        found = False
        if key in db_set:
            found = True
        else:
            # Fuzzy check: relaxed name match
            s_name_parts = set(s_name_norm.split())
            
            for db_key in db_set:
                db_name, db_evt, db_time = db_key
                # exact time and event match required
                if db_evt == evt_norm and db_time == t_str:
                    # Token Subset Match
                    # "baltazar aguirre" (tokens) subset of "baltazar leon aguirre" (tokens)?
                    # OR match if ANY significant overlap?
                    # Let's say: if ALL parts of the shorter name appear in the longer name
                    db_name_parts = set(db_name.split())
                    
                    if s_name_parts.issubset(db_name_parts) or db_name_parts.issubset(s_name_parts):
                        found = True
                        break
        
        if not found:
            # Check if we have the event but different time
            event_found = False
            found_times = []
            
            s_name_parts = set(s_name_norm.split())

            for db_key in db_set:
                db_name, db_evt, db_time = db_key
                if db_evt == evt_norm:
                     # Check name
                     db_name_parts = set(db_name.split())
                     if s_name_parts.issubset(db_name_parts) or db_name_parts.issubset(s_name_parts):
                         event_found = True
                         found_times.append(db_time)
            
            if event_found:
                 print(f"[TIME MISMATCH] {full_name} | {evt_norm} | CSV: {t_str}s | DB Has: {found_times}")
                 mismatch_count += 1
            else:
                 missing_count += 1
                 print(f"[MISSING EVENT] {full_name} | {evt_norm} ({raw_event}) | {t_str}s")

print(f"\n--- Summary ---")
print(f"Total CSV Rows Checked: {total_checked}")
print(f"Time Mismatches (Prelim/Final?): {mismatch_count}")
print(f"Fully Missing Events: {missing_count}")
