import pandas as pd
import sqlite3
import sys

# File Path
EXCEL_PATH = "/Users/jrb/Downloads/Nadadores.xlsx"
DB_PATH = "natacion.db"

def sync_dob():
    # 1. Load Excel
    try:
        df = pd.read_excel(EXCEL_PATH)
        print(f"Loaded Excel: {len(df)} rows")
    except Exception as e:
        print(f"Error loading Excel: {e}")
        return

    # Normalize Columns
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Identify key columns (heuristics)
    name_col = next((c for c in df.columns if "nombre" in c or "name" in c), None)
    dob_col = next((c for c in df.columns if "fecha" in c or "birth" in c or "nacim" in c), None)
    
    if not name_col or not dob_col:
        print(f"Could not identify columns. Found: {df.columns}")
        return

    print(f"Using columns: Name='{name_col}', DOB='{dob_col}'")

    # 2. Connect DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get current swimmers
    cursor.execute("SELECT id, name FROM swimmers")
    db_swimmers = cursor.fetchall()
    
    # Normanization helper
    import unicodedata
    def normalize_str(s):
        if not s: return ""
        # NFD decomposition to separate accents
        s = unicodedata.normalize('NFD', str(s))
        # Filter non-spacing marks and lower case
        return "".join(c for c in s if unicodedata.category(c) != 'Mn').lower().strip()

    # Pre-process DB swimmers
    # List of (id, original_name, set_of_tokens)
    db_candidates = []
    for sid, name in db_swimmers:
        if name:
            norm = normalize_str(name)
            tokens = set(norm.split())
            db_candidates.append({
                'id': sid,
                'name': name,
                'norm': norm,
                'tokens': tokens
            })
            
    updates = 0
    matches_found = 0
    
    for idx, row in df.iterrows():
        raw_name = row[name_col]
        raw_dob = row[dob_col]
        
        if pd.isna(raw_name) or pd.isna(raw_dob):
            continue
            
        # Parse Date
        try:
            date_val = pd.to_datetime(raw_dob).date() # YYYY-MM-DD
        except:
            continue
            
        # Normalize Excel Name
        excel_norm = normalize_str(raw_name)
        excel_tokens = set(excel_norm.split())
        
        # Find Match
        # Strategy: DB Name tokens must be a SUBSET of Excel Name tokens
        # e.g. DB="Amaro Gonzalez" ({amaro, gonzalez}) <= Excel="Amaro Gonzalez Aldana" ({amaro, gonzalez, aldana})
        # OR vice versa? Usually Excel has full legal name, DB has "Common" name.
        # But sometimes DB might be "Juan Pablo Perez" and Excel "Juan Perez".
        # Let's try Intersection Score.
        
        best_match = None
        
        for candidate in db_candidates:
            # Check 1: Subset (Strong Match)
            # If all tokens of the SHORTER name are in the LONGER name
            
            common = candidate['tokens'].intersection(excel_tokens)
            
            # Match condition: 
            # 1. Exact match of tokens
            # 2. Candidate (DB) is subset of Excel (e.g. DB: Amaro Gonzalez, Excel: Amaro Gonzalez Aldana)
            # 3. Excel is subset of Candidate (e.g. DB: Amaro Gonzalez Aldana, Excel: Amaro Gonzalez) - less likely but possible
            
            is_subset_db_in_excel = candidate['tokens'].issubset(excel_tokens)
            is_subset_excel_in_db = excel_tokens.issubset(candidate['tokens'])
            
            # Threshold: Must share at least 2 tokens (First Name + Last Name) to avoid "Juan" matching "Juan Perez"
            # Unless the name only has 1 token?
            
            if len(common) >= 2 or (len(candidate['tokens']) == 1 and len(common) == 1):
                if is_subset_db_in_excel or is_subset_excel_in_db:
                    # Found a potential match
                    # Disambiguation? If multiple match?
                    # For now take the first strong match or refine logic.
                    best_match = candidate
                    break
        
        if best_match:
            matches_found += 1
            # Update
            try:
                # print(f"Match: '{raw_name}' <-> '{best_match['name']}'")
                cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (str(date_val), best_match['id']))
                if cursor.rowcount > 0:
                    updates += 1
            except Exception as e:
                print(f"Error updating {best_match['name']}: {e}")

    conn.commit()
    conn.close()
    print(f"Total entries in Excel: {len(df)}")
    print(f"Successfully matched and updated: {updates}")

if __name__ == "__main__":
    sync_dob()
