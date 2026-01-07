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

def report_roster():
    # 1. Get DB Swimmers
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM swimmers ORDER BY name ASC")
    db_names = [r[0] for r in cursor.fetchall()]
    conn.close()
    
    # 2. Get Excel Swimmers
    try:
        df = pd.read_excel(EXCEL_PATH)
        name_col = next((c for c in df.columns if "nombre" in c.lower()), None)
        excel_names = df[name_col].dropna().tolist()
    except:
        excel_names = []

    print(f"### ✅ Nadadores Activos en Base de Datos ({len(db_names)})")
    for name in db_names:
        print(f"- {name}")
        
    print("\n" + "="*40 + "\n")
    
    print(f"### ⚠️ No encontrados del Excel ({len(excel_names) - len(db_names)} aprox)")
    # Simple check: which excel names have no match in DB?
    # Re-using the logic from prune_db roughly
    
    db_tokens = []
    for dbn in db_names:
        db_tokens.append(set(normalize_str(dbn).split()))
        
    missing = []
    for raw_name in excel_names:
        norm = normalize_str(raw_name)
        tokens = set(norm.split())
        
        found = False
        for cand_tokens in db_tokens:
             common = cand_tokens.intersection(tokens)
             # Same fuzzy logic as before
             if len(common) >= 2 or (len(cand_tokens) == 1 and len(common) == 1):
                 if cand_tokens.issubset(tokens) or tokens.issubset(cand_tokens):
                     found = True
                     break
        if not found:
            missing.append(raw_name)
            
    for m in missing:
        print(f"- {m}")

if __name__ == "__main__":
    report_roster()
