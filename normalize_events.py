
import sqlite3
import re

DB_PATH = "data/natacion.db"

def normalize_event_name_v2(raw_name):
    clean = raw_name.strip()
    
    # 1. Remove units
    clean = re.sub(r'\b(Metro|Meter|Metros|Meters)\b', '', clean, flags=re.IGNORECASE)
    
    # 2. Remove Gender prefixes
    prefixes = ['Hombres', 'Mujeres', 'Men', 'Women', 'Mixto', 'Niños', 'Niñas', 'Boys', 'Girls', 'Mixed', 'Ninas', 'Ninos']
    for p in prefixes:
        clean = re.sub(r'^' + p + r'\s*', '', clean, flags=re.IGNORECASE)
        
    clean = clean.strip()
    
    # 3. Remove "Open", "Todo Competidor", etc
    clean = re.sub(r'^(Open|Todo Competidor|Absoluto)\s*', '', clean, flags=re.IGNORECASE)

    # 4. Remove Meet Mobile Suffixes (Junk)
    suffixes = [
        'Timed Finals', 'Prelims', 'Finals', 'Semifinals', 
        'Extracted', 'Leadoff', 'Multidisability', 'Split', 
        'Swim-off', 'Advancement'
    ]
    # Remove parens like (Leadoff) or just words at end
    for s in suffixes:
        # 1. Remove parenthesized version "(Leadoff)"
        clean = re.sub(r'\s*\(' + s + r'\)', '', clean, flags=re.IGNORECASE)
        # 2. Remove standard version "Timed Finals" at end of string
        clean = re.sub(r'\s*' + s + r'$', '', clean, flags=re.IGNORECASE)

    # 4. Remove Age Groups
    clean = re.sub(r'\b\d{1,2}-\d{1,2}\s*(años|years)?\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b\d{1,2}[&][OUou]\b', '', clean)
    clean = re.sub(r'\b\d{1,2}\s+(años|years)\b', '', clean, flags=re.IGNORECASE)
    
    clean = clean.strip()
    
    # 5. Handle "Age Distance"
    match_two = re.match(r'^(\d+)\s+(\d+)\s+(.*)', clean)
    if match_two:
        clean = f"{match_two.group(2)} {match_two.group(3)}"
    else:
        valid_dists = [25, 50, 100, 200, 400, 800, 1500]
        match_one = re.match(r'^(\d+)\s+(.*)', clean)
        if match_one:
            val = int(match_one.group(1))
            if val not in valid_dists:
                if val < 25 and val != 4: 
                    clean = match_one.group(2)

    # Translation
    translations = {
        'Libre': 'Free', 'Pecho': 'Breast', 'Espalda': 'Back',
        'Mariposa': 'Fly', 'Combinado': 'IM', 'CI': 'IM',
        'Relevo': 'Relay', 'Medley': 'Medley'
    }
    
    words = clean.split()
    new_words = []
    for w in words:
        cap = w.capitalize()
        if cap in translations:
            new_words.append(translations[cap])
        elif w.upper() == "CI":
             new_words.append("IM")
        else:
            new_words.append(w)
            
    clean = " ".join(new_words)
    if "IM Relay" in clean: clean = clean.replace("IM Relay", "Medley Relay")
    
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def run():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tables to Normalize
    # Note: 'results' already handled by sync/main. Focused here on references.
    targets = ['minimum_standards', 'national_records']
    
    for table in targets:
        print(f"Normalizing {table}...")
        cursor.execute(f"SELECT id, event_name FROM {table}")
        rows = cursor.fetchall()
        
        count = 0
        skipped = 0
        for r_id, raw in rows:
            new_name = normalize_event_name_v2(raw)
            if new_name != raw and new_name:
                try:
                    cursor.execute(f"UPDATE {table} SET event_name = ? WHERE id = ?", (new_name, r_id))
                    count += 1
                except sqlite3.IntegrityError:
                   # If duplicate exists (UNIQUE constraint), maybe delete this one?
                   # Or ignore?
                   # For references, we might have duplicate rows if normalization collapses them?
                   # Let's Skip.
                   skipped += 1
                   
        print(f"Updated {count} rows in {table}. Skipped/Dupes: {skipped}")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    run()
