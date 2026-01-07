import sqlite3
import pandas as pd
import os
from difflib import SequenceMatcher

DB_PATH = "/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db"
CSV_PATH = "/Users/jrb/Documents/RAMA/swim_scraper/meet_mobile_swimmers.csv"
VICENTE_ID = '3235418'

def simplify_name(name):
    return name.lower().strip().replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u').replace('ñ','n')

def cleanup():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Load Authorized List
    try:
        csv_df = pd.read_csv(CSV_PATH)
        csv_df['full_name'] = csv_df['firstName'].str.strip() + ' ' + csv_df['lastName'].str.strip()
        csv_df['simple_name'] = csv_df['full_name'].apply(simplify_name)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # 2. Map of Clean Name -> details
    authorized_map = {} 
    for _, row in csv_df.iterrows():
        authorized_map[row['simple_name']] = {'id': str(row['swimmer_id']), 'name': row['full_name']}
    
    # Add Vicente and Tomas
    vicente_name = "Vicente Reyes"
    authorized_map[simplify_name(vicente_name)] = {'id': VICENTE_ID, 'name': vicente_name}
    
    tomas_name = "Tomas Oyarzun"
    # We don't have a fixed ID for him in CSV, so we let the script find him or keep his MM_ID
    # We use a placeholder ID to ensure he is 'authorized'
    authorized_map[simplify_name(tomas_name)] = {'id': 'KEEP_TOMAS', 'name': tomas_name}

    # 3. Load All DB Swimmers
    db_swimmers = pd.read_sql("SELECT * FROM swimmers", conn)
    db_swimmers['id'] = db_swimmers['id'].astype(str)
    
    kept_ids = set()
    merged_count = 0
    deleted_count = 0
    
    found_authorized = set()

    print(f"Total entries in DB BEFORE: {len(db_swimmers)}")

    # 4. First Pass: Exact ID Matches
    for _, row in db_swimmers.iterrows():
        s_id = row['id']
        matched_auth = None
        for k, v in authorized_map.items():
            if v['id'] == s_id:
                matched_auth = k
                break
        
        if matched_auth:
            kept_ids.add(s_id)
            found_authorized.add(matched_auth)

    # 5. Second Pass: Name Matches
    for auth_simple, auth_data in authorized_map.items():
        target_name = auth_data['name']
        
        candidates = []
        for _, row in db_swimmers.iterrows():
            db_simple = simplify_name(row['name'])
            
            score = 0
            if auth_simple == db_simple:
                score = 1.0
            elif auth_simple in db_simple or db_simple in auth_simple:
                score = 0.95
            else:
                 score = SequenceMatcher(None, auth_simple, db_simple).ratio()
            
            if score > 0.85: # Stricter threshold
                # Sibling Guard: If first names are different but surnames match, score might be high.
                # Explicitly check first token difference
                cand_first = row['name'].split()[0].lower()
                auth_first = target_name.split()[0].lower()
                
                # Verify first name similarity or containment
                if cand_first != auth_first and SequenceMatcher(None, cand_first, auth_first).ratio() < 0.8:
                     pass # Different people (e.g. Matias vs Tomas)
                else:
                     candidates.append((score, row['id'], row['name']))
        
        if not candidates:
            continue
            
        candidates.sort(key=lambda x: x[0], reverse=True)
        
        survivor_id = None
        for _, cid, _ in candidates:
            if cid in kept_ids:
                survivor_id = cid
                break
        
        if not survivor_id:
            survivor_id = candidates[0][1]
            kept_ids.add(survivor_id)
            found_authorized.add(auth_simple)
            
        for _, cand_id, cand_name in candidates:
            if cand_id != survivor_id:
                try:
                    cursor.execute("UPDATE results SET swimmer_id = ? WHERE swimmer_id = ?", (survivor_id, cand_id))
                    cursor.execute("DELETE FROM swimmers WHERE id = ?", (cand_id,))
                    merged_count += 1
                except Exception as e:
                    print(f"Merge error: {e}")

    conn.commit()
    
    # 6. Delete Unwanted
    if not kept_ids:
        print("Error: No kept IDs?")
        return
        
    formatted_kept = tuple(kept_ids)
    if len(formatted_kept) == 1: formatted_kept = f"('{list(kept_ids)[0]}')"
    
    doomed = pd.read_sql(f"SELECT * FROM swimmers WHERE id NOT IN {formatted_kept}", conn)
    
    if not doomed.empty:
        print(f"\n--- DELETING {len(doomed)} UNAUTHORIZED ---")
        cursor.execute(f"DELETE FROM results WHERE swimmer_id NOT IN {formatted_kept}")
        cursor.execute(f"DELETE FROM swimmers WHERE id NOT IN {formatted_kept}")
        deleted_count = len(doomed)
    
    conn.commit()
    conn.close()
    
    print(f"\nResults: Merged {merged_count}, Deleted {deleted_count}. Final Count: {len(kept_ids)}")

if __name__ == "__main__":
    cleanup()
