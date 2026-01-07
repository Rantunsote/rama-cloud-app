
import sqlite3
import re

DB_PATH = "data/natacion.db"

# Exceptions / Explicit mappings for ambiguous or non-standard ending names
GENDER_MAP = {
    # Female Exceptions (Don't end in A)
    "CONSUELO": "F", "ROCIO": "F", "PAZ": "F", "SOL": "F", 
    "BELEN": "F", "MONTSERRAT": "F", "MONSERRAT": "F", "ABRIL": "F",
    "RAQUEL": "F", "ESTER": "F", "PILAR": "F", "CARMEN": "F", "BEATRIZ": "F",
    "JAZMIN": "F", "MAITE": "F", "DANIELA": "F", "SOFIA": "F", # End in A actually
    
    # Male Exceptions (End in A or ambiguous)
    "LUCA": "M", "NICOLA": "M", "ANDREA": "M", # In Italian, but in Chile usually F? Safe to assume F in Chile mostly, check context.
    "JOSUE": "M", "NOAH": "M", "RENE": "M", "FELIPE": "M", "VICENTE": "M",
    "DANTE": "M", "CLEMENTE": "M", "JOSE": "M",
}

def get_gender(name):
    first = name.split()[0].upper()
    
    # 1. Check Explicit Map
    if first in GENDER_MAP:
        return GENDER_MAP[first]
        
    # 2. Ends in 'A' -> Female
    if first.endswith('A'):
        return 'F'
        
    # 3. Default -> Male (Spanish convention: o, e, con, etc.)
    return 'M'

def run():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Select missing
    cursor.execute("SELECT id, name FROM swimmers WHERE gender IS NULL OR gender = '' OR gender = '?'")
    rows = cursor.fetchall()
    
    updated = 0
    for s_id, name in rows:
        inferred = get_gender(name)
        print(f"Inferring {name} -> {inferred}")
        cursor.execute("UPDATE swimmers SET gender = ? WHERE id = ?", (inferred, s_id))
        updated += 1
        
    conn.commit()
    conn.close()
    print(f"Updated {updated} swimmers.")

if __name__ == "__main__":
    run()
