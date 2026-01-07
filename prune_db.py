import pandas as pd
import sqlite3
import unicodedata

# File Paths
EXCEL_PATH = "/Users/jrb/Downloads/Nadadores.xlsx"
DB_PATH = "natacion.db"

def normalize_str(s):
    if not s: return ""
    s = unicodedata.normalize('NFD', str(s))
    return "".join(c for c in s if unicodedata.category(c) != 'Mn').lower().strip()

def prune_database():
    print("--- Pruning Database to match Excel Roster ---")
    
    # 1. Load Trusted Roster (Excel)
    try:
        df = pd.read_excel(EXCEL_PATH)
        # Assuming column 'Nombre y Apellidos' exists
        name_col = next((c for c in df.columns if "nombre" in c.lower()), None)
        if not name_col:
            print("Error: Could not find 'Name' column in Excel.")
            return
        
        excel_names = df[name_col].dropna().tolist()
        print(f"Loaded {len(excel_names)} valid swimmers from Excel.")
    except Exception as e:
        print(f"Error reading Excel: {e}")
        return

    # 2. Load Current DB Swimmers
    conn = sqlite3.connect(DB_PATH)
    # Enable FK support to cascade deletions if schema supports (or manual delete)
    conn.execute("PRAGMA foreign_keys = ON") 
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name FROM swimmers")
    db_rows = cursor.fetchall()
    
    # Pre-process DB candidates for matching
    db_candidates = []
    for sid, name in db_rows:
        if name:
            norm = normalize_str(name)
            tokens = set(norm.split())
            db_candidates.append({
                'id': sid,
                'name': name,
                'norm': norm,
                'tokens': tokens
            })
            
    # 3. Identify Keep List
    keep_ids = set()
    matched_names = []
    
    for raw_name in excel_names:
        excel_norm = normalize_str(raw_name)
        excel_tokens = set(excel_norm.split())
        
        # Reuse logic: Find best match in DB
        best_match = None
        
        for candidate in db_candidates:
            # Skip if already kept? (One-to-many check)
            # Actually one excel person maps to one DB person.
            
            common = candidate['tokens'].intersection(excel_tokens)
            is_subset_db_in_excel = candidate['tokens'].issubset(excel_tokens)
            is_subset_excel_in_db = excel_tokens.issubset(candidate['tokens'])
            
            if len(common) >= 2 or (len(candidate['tokens']) == 1 and len(common) == 1):
                if is_subset_db_in_excel or is_subset_excel_in_db:
                    best_match = candidate
                    break
        
        if best_match:
            keep_ids.add(best_match['id'])
            matched_names.append(f"{raw_name} -> {best_match['name']}")
        else:
            print(f"WARNING: Swimmer in Excel NOT found in DB: {raw_name}")

    print(f"Identified {len(keep_ids)} unique IDs to KEEP.")
    
    # 4. Prune
    if not keep_ids:
        print("Safety Abort: No matched IDs found to keep. Not deleting anything.")
        conn.close()
        return

    # Delete results first (if no cascade)
    placeholders = ','.join('?' for _ in keep_ids)
    
    # Count before
    cursor.execute("SELECT COUNT(*) FROM swimmers")
    count_before = cursor.fetchone()[0]
    
    # Delete Swimmers NOT IN keep_ids
    print("Deleting obsolete records...")
    
    # 1. Delete Splits for obsolete results
    # We need to find results that belong to obsolete swimmers first
    cursor.execute(f"DELETE FROM splits WHERE result_id IN (SELECT id FROM results WHERE swimmer_id NOT IN ({placeholders}))", list(keep_ids))
    deleted_splits = cursor.rowcount

    # 2. Delete Results for obsolete swimmers
    cursor.execute(f"DELETE FROM results WHERE swimmer_id NOT IN ({placeholders})", list(keep_ids))
    deleted_results = cursor.rowcount
    
    # 3. Delete Swimmers
    cursor.execute(f"DELETE FROM swimmers WHERE id NOT IN ({placeholders})", list(keep_ids))
    deleted_swimmers = cursor.rowcount
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM swimmers")
    count_after = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"--- Pruning Complete ---")
    print(f"Swimmers Before: {count_before}")
    print(f"Swimmers After:  {count_after}")
    print(f"Deleted: {deleted_count} swimmers, {deleted_results} results.")

if __name__ == "__main__":
    prune_database()
