import sqlite3
import pandas as pd
import shutil
import os
import unicodedata
from fuzzywuzzy import process, fuzz
import re
import csv
from normalize_events import normalize_event_name_v2

# --- CONFIG ---
LIVE_DB_PATH = '/Users/jrb/Library/Containers/7F2BC93B-8FAC-48B0-BF83-D128B1ADF11C/Data/Documents/MeetMobile.db'
MM_DB_PATH = '/Users/jrb/Documents/RAMA/swim_scraper/meet_mobile_dump.db'
LOCAL_DB_PATH = '/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db'
WHITELIST_PATH = '/Users/jrb/Documents/RAMA/swim_scraper/data/swimmers_whitelist.csv'
# JAN_1_2020_TIMESTAMP = 1577836800 
# JAN_1_2010_TIMESTAMP = 1262304000
JAN_1_2000_TIMESTAMP = 946684800 # Capture ALL history

def normalize_text(text):
    """
    Normalize text: strip, lowercase, remove accents.
    """
    if not isinstance(text, str): return ""
    text = text.strip().lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return text

def load_whitelist():
    """
    Loads whitelist and returns a list of sets of name tokens.
    [ {'josefa', 'acuna', 'rojas'}, {'baltazar', 'aguirre'} ]
    """
    whitelist_tokens = []
    if not os.path.exists(WHITELIST_PATH):
        print(f"WARNING: Whitelist not found at {WHITELIST_PATH}")
        return []
    
    with open(WHITELIST_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Construct full name from parts
            # Nombre, Apellido Paterno, Apellido Materno
            parts = [row.get('Nombre', ''), row.get('Apellido Paterno', ''), row.get('Apellido Materno', '')]
            full_str = " ".join([p for p in parts if p]).strip()
            
            # Normalize and tokenize
            norm = normalize_text(full_str)
            tokens = set(norm.split())
            if tokens:
                whitelist_tokens.append(tokens)
                
    print(f"Loaded {len(whitelist_tokens)} swimmers from whitelist.")
    return whitelist_tokens

def is_whitelisted(name_str, whitelist_tokens_list):
    """
    Full Name Token Subset Match with Fuzzy Fallback.
    Returns True if name_str matches any entry in whitelist.
    """
    norm_name = normalize_text(name_str)
    name_tokens = set(norm_name.split())
    
    if not name_tokens: return False
    
    # 1. Strict Token Subset
    for w_tokens in whitelist_tokens_list:
        if w_tokens.issubset(name_tokens) or name_tokens.issubset(w_tokens):
            return True

    # 2. Fuzzy Fallback (for 'Gonzales' vs 'Gonzalez')
    # Use fuzzy string comparison against reconstructed whitelist strings
    # This might be slow if list is huge, but for 40 swimmers it's instant.
    for w_tokens in whitelist_tokens_list:
        w_str = " ".join(w_tokens)
        score = fuzz.token_set_ratio(norm_name, w_str)
        if score >= 90:
             # Double check to prevent "Juan Perez" matching "Juan Gonzalez" with high score? 
             # token_set_ratio handles subset logic well.
             # e.g. "Lourdes Gonzales" vs "Lourdes Gonzalez Rodriguez" -> High score
             return True
            
    return False

def normalize_event_name(raw_name):
    """Normalizes event names like 'Hombres 11&O 200 Metro Pecho' to '200 Breast'."""
    clean = raw_name.strip()
    
    # 1. Remove units
    clean = re.sub(r'\b(Metro|Meter|Metros|Meters)\b', '', clean, flags=re.IGNORECASE)
    
    # 2. Remove Gender prefixes
    prefixes = ['Hombres', 'Mujeres', 'Men', 'Women', 'Mixto', 'Niños', 'Niñas', 'Boys', 'Girls', 'Mixed', 'Ninas', 'Ninos']
    for p in prefixes:
        clean = re.sub(r'^' + p + r'\s*', '', clean, flags=re.IGNORECASE)
        
    clean = clean.strip()
    
    # 3. Remove "Open", "Todo Competidor"
    clean = re.sub(r'^(Open|Todo Competidor|Absoluto)\s*', '', clean, flags=re.IGNORECASE)

    # 4. Remove valid Age Group patterns explicitly
    clean = re.sub(r'\b\d{1,2}-\d{1,2}\s*(años|years)?\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b\d{1,2}[&][OUou]\b', '', clean)
    clean = re.sub(r'\b\d{1,2}\s+(años|years)\b', '', clean, flags=re.IGNORECASE)
    
    clean = clean.strip()
    
    # 5. Handle "Age Distance" pattern (e.g. "10 50 Free")
    # Identify if starts with two numbers. First is Age.
    match_two = re.match(r'^(\d+)\s+(\d+)\s+(.*)', clean)
    if match_two:
        clean = f"{match_two.group(2)} {match_two.group(3)}"
    else:
        # Check if starts with number not in valid distances?
        valid_dists = [25, 50, 100, 200, 400, 800, 1500]
        match_one = re.match(r'^(\d+)\s+(.*)', clean)
        if match_one:
            val = int(match_one.group(1))
            if val not in valid_dists:
                # Likely an age (e.g. "9 Free" -> 9 year old free?)
                # Ambiguous but safer to strip single digit ages if they aren't standard distances
                if val < 25 and val != 4: # 4 for Relay?
                    clean = match_one.group(2)

    # Translation Map
    translations = {
        'Libre': 'Free', 'Pecho': 'Breast', 'Espalda': 'Back',
        'Mariposa': 'Fly', 'Combinado': 'IM', 'CI': 'IM',
        'Relevo': 'Relay', 'Medley': 'Medley'
    }
    
    words = clean.split()
    new_words = []
    for w in words:
        cap = w.capitalize()
        # Direct Match
        if cap in translations:
            new_words.append(translations[cap])
        # Case insensitive check
        elif w.upper() == "CI":
             new_words.append("IM")
        else:
            new_words.append(w)
            
    clean = " ".join(new_words)
    if "IM Relay" in clean: clean = clean.replace("IM Relay", "Medley Relay")
    
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def refresh_mm_db():
    pass 
    # if os.path.exists(LIVE_DB_PATH):
    #     try:
    #         shutil.copy2(LIVE_DB_PATH, MM_DB_PATH)
    #         print("Synced Meet Mobile DB.")
    #     except:
    #         print("Could not sync live DB, using cache.")

def normalize_name(name):
    """Normalize specific names to match our DB."""
    # Custom fixes based on known aliases
    name = name.strip().title()
    return name

def get_mm_connection():
    refresh_mm_db()
    return sqlite3.connect(MM_DB_PATH)

def get_local_connection():
    return sqlite3.connect(LOCAL_DB_PATH)

def fetch_local_swimmers(conn):
    """Returns dict {Name: ID}"""
    df = pd.read_sql("SELECT id, name FROM swimmers", conn)
    return dict(zip(df['name'], df['id']))

def sync_data():
    mm_conn = get_mm_connection()
    local_conn = get_local_connection()
    local_cursor = local_conn.cursor()
    
    # 0. Load Whitelist
    whitelist_tokens = load_whitelist()
    if not whitelist_tokens:
        print("ABORTING: No whitelist loaded from CSV.")
        return

    # 1. Get Target Meets (ALL HISTORY + Team Filter)
    # Re-using logic from extract_full_report but focused on Meets
    print("Finding Meets (History)...")
    # Find team IDs first
    teams = pd.read_sql("SELECT id FROM Team WHERE name LIKE '%Penalolen%' OR name LIKE '%CRNP%' OR name LIKE '%Rama%'", mm_conn)
    team_ids = tuple(teams['id'].tolist())
    if len(team_ids) == 1: team_ids = f"({team_ids[0]})"
    
    meet_query = f"""
    SELECT DISTINCT m.id, m.name, m.startDateUtc, m.city, m.country, m.facilityName
    FROM Meet m
    JOIN Swimmer s ON s.meetId = m.id
    WHERE s.teamId IN {team_ids}
      AND m.startDateUtc >= {JAN_1_2000_TIMESTAMP}
    """
    meets_df = pd.read_sql(meet_query, mm_conn)
    print(f"Found {len(meets_df)} meets to sync.")
    
    # 2. Load Local Swimmers
    local_swimmers = fetch_local_swimmers(local_conn)
    local_swimmer_names = list(local_swimmers.keys())
    
    # 3. Iterate Meets
    for _, meet in meets_df.iterrows():
        mm_meet_id = meet['id']
        meet_name = meet['name']
        meet_date = pd.to_datetime(meet['startDateUtc'], unit='s').strftime('%Y-%m-%d')
        meet_loc = f"{meet['city']}, {meet['country']}"
        
        # Build Address
        parts = []
        if meet.get('facilityName'): parts.append(str(meet['facilityName']).title())
        if meet.get('city'): parts.append(str(meet['city']).title())
        if meet.get('country'): parts.append(str(meet['country']).upper())
        address_str = ", ".join(parts)
        
        # Check if Meet Exists
        new_meet_id = f"MM_{mm_meet_id}"
        
        # Check existence
        res = local_cursor.execute("SELECT id, pool_size FROM meets WHERE id = ?", (new_meet_id,)).fetchone()
        
        target_pool = "25m" # Default
        
        if not res:
            local_cursor.execute(
                "INSERT INTO meets (id, name, date, location, url, pool_size, address) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (new_meet_id, meet_name, meet_date, meet_loc, "MeetMobile", target_pool, address_str)
            )
        else:
            # Upsert Address (if was missing)
            local_cursor.execute("UPDATE meets SET address = ? WHERE id = ?", (address_str, new_meet_id))
            
            # Use existing pool size from DB (which allows manual override persistence)
            if res[1]:
                target_pool = res[1]
            else:
                local_cursor.execute("UPDATE meets SET pool_size = '25m' WHERE id = ?", (new_meet_id,))
            pass
            
        # 4. Get Results for this Meet + Team
        r_query = f"""
        SELECT 
            s.id as MMSwimmerId,
            s.firstName || ' ' || s.lastName as SwimmerName,
            he.id as HeatEntryId,
            e.name as EventName,
            he.timeInSecs as Time,
            he.pointsEarned as Points,
            he.overallPlace as Place
        FROM Swimmer s
        JOIN SwimmerHeatEntry she ON s.id = she.swimmerId
        JOIN HeatEntry he ON she.heatEntryId = he.id
        JOIN Heat h ON he.heatId = h.id
        JOIN Round r ON h.roundId = r.id
        JOIN Event e ON r.eventId = e.id
        WHERE s.meetId = {mm_meet_id}
          AND s.teamId IN {team_ids}
        """
        results_df = pd.read_sql(r_query, mm_conn)
        
        for _, row in results_df.iterrows():
            s_name = normalize_name(row['SwimmerName'])
            
            # --- WHITELIST CHECK ---
            if not is_whitelisted(s_name, whitelist_tokens):
                # print(f"  [SKIP] {s_name} not in whitelist.")
                continue
            
            swimmer_id = local_swimmers.get(s_name)
            
            if not swimmer_id:
                # Fuzzy Match Fallback (Still useful for small variations even with whitelist)
                best_match, score = process.extractOne(s_name, local_swimmer_names, scorer=fuzz.token_set_ratio)
                if score >= 90:
                    swimmer_id = local_swimmers[best_match]
            
            if not swimmer_id:
                # 2. BLACKLIST CHECK (Redundant with whitelist but safe)
                if s_name in ['[Relay] [Swimmer]', 'Unknown Swimmer 4']:
                    continue

                # CREATE NEW SWIMMER
                new_sw_id = f"MM_{row['MMSwimmerId']}"
                
                # Double check existence by ID
                chk = local_cursor.execute("SELECT id FROM swimmers WHERE id = ?", (new_sw_id,)).fetchone()
                if not chk:
                    # print(f"  [NEW] Creating Swimmer: {s_name} (ID: {new_sw_id})")
                    local_cursor.execute(
                        "INSERT INTO swimmers (id, name, url, team_id, birth_date, gender) VALUES (?, ?, ?, ?, ?, ?)",
                        (new_sw_id, s_name, "MeetMobile", "MM_TEAM", None, None)
                    )
                    local_conn.commit()
                
                swimmer_id = new_sw_id
                # Update cache
                local_swimmers[s_name] = swimmer_id
                local_swimmer_names.append(s_name)
            
            # Check if result exists (avoid dupes)
            # Use NORMALIZED event name
            norm_event = normalize_event_name_v2(row['EventName'])
            
            check_sql = "SELECT id FROM results WHERE swimmer_id = ? AND meet_id = ? AND event_name = ?"
            existing = local_cursor.execute(check_sql, (swimmer_id, new_meet_id, norm_event)).fetchone()
            
            if existing:
                result_id = existing[0]
            else:
                # Replace comma with dot
                raw_time = row['Time']
                if not raw_time: continue
                raw_time = raw_time.replace(',', '.')
                
                local_cursor.execute("""
                    INSERT INTO results (swimmer_id, meet_id, event_name, time, points, place, pool_size, time_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (swimmer_id, new_meet_id, norm_event, raw_time, row['Points'], row['Place'], target_pool, "MeetMobile"))
                result_id = local_cursor.lastrowid
            
            # 5. Sync Splits
            # Get Splits for this HeatEntryId
            s_query = f"""
            SELECT sequence, distance, time, cumulativeTime, stroke
            FROM SplitTime
            WHERE heatEntryId = {row['HeatEntryId']}
            ORDER BY sequence ASC
            """
            splits_df = pd.read_sql(s_query, mm_conn)
            
            if splits_df.empty: continue
            
            # Clear old splits for this result?
            local_cursor.execute("DELETE FROM splits WHERE result_id = ?", (result_id,))
            
            for _, split in splits_df.iterrows():
                s_time = split['time'].replace(',', '.') if split['time'] else None
                s_cum = split['cumulativeTime'].replace(',', '.') if split['cumulativeTime'] else None
                
                local_cursor.execute("""
                    INSERT INTO splits (result_id, distance, split_time, cumulative_time)
                    VALUES (?, ?, ?, ?)
                """, (result_id, split['distance'], s_time, s_cum))
                
    local_conn.commit()
    local_conn.close()
    mm_conn.close()
    print("Sync Complete!")

if __name__ == "__main__":
    sync_data()
