
import sqlite3
import pandas as pd
from io import StringIO
import datetime
import unicodedata
from fuzzywuzzy import process, fuzz
import uuid

# FULL 93 SWIMMERS DATA
csv_data = """Nombre Completo,Fecha de Nacimiento
Josefa Acuña Rojas,16-ene-10
Javiera Aguirre Polverelli,6-mar-07
Amalia Alquinta Almarza,5-sep-12
Noelia Aranda Muñoz,11-oct-12
Alonso Arce Marabolí,13-oct-11
Josefa Arellano Valdebenito,31-oct-08
Amaro Arias Vasquez,3-jul-08
Rafaela Astudillo Gonzalez,15-feb-10
Sebastian Barria Diaz,25-abr-06
Rafael Belandria Molina,8-mar-09
Gaspar Belandria Molina,27-abr-16
Gabriel Belandria Molina,27-abr-16
Maria Jesus Beltran Muñoz,21-feb-02
Camila Benavente Albarracin,28-oct-09
Pedro Briones Gonzalez,21-feb-08
Josefa Cabello Perez,7-dic-06
Gabriel Cabrera Muñoz,29-sep-05
Amanda Caligari Pinilla,18-nov-07
Bruno Calizaya Rojas,7-dic-01
Isidora Campos Muñoz,18-jun-08
Florencia Campos Muñoz,5-jun-04
Rafaela Canales Vasquez,13-feb-14
Alonso Canales Vasquez,25-jun-11
Emilia Cardenas Soro,19-jul-10
Martina Castillo Donoso,6-oct-10
Catalina Castro Perez,21-ago-07
Santiago Cañas Jimenez,3-jun-11
Matias Contreras Pavez,15-ene-13
Isidora Faundez Beltran,8-sep-12
Camila Figueroa Gomez,19-abr-11
Tomas Figueroa Gomez,19-abr-11
Facundo Gomez Huerta,23-sept-11
Magdalena Gomez Huerta,4-nov-14
Amaro González Aldana,11-feb-10
Angelo González Gajardo,14-abr-01
Lourdes González Rodriguez,29-jun-12
Blanca González Aldana,2-jul-14
Ema González Aldana,2-jul-14
Adriano Guarama,16-dic-09
Benjamin Heredia Jara,9-ago-17
Martin Heredia Jara,17-nov-11
Roberta Hidalgo Valencia,23-dic-10
Tomas Iglesias Diez de Medina,23-jun-06
Amanda Mac Ivar Jara,21-abr-14
Antonella Maldonado Gajardo,31-dic-15
Ashly de los Angeles Marquez Rojas,27-nov-14
Wesley Medina Medina,10-jul-14
Gabriela Medina Cáceres,21-dic-04
Cristobal Melendez Landa,1-dic-16
Diego Adan Meneses Cruces,6-ago-10
Leon Montenegro Espinoza,9-nov-16
Amelia Montoya Ponce,5-jun-15
Alonso Muñoz,1-abr-09
Isidora Navarrete Bustamante,2-jul-15
Renata Navas Carvajal,9-dic-13
Florencia Nietzschmann Bayas,2-nov-13
Matías Oyarzún Vergara,3-jul-15
Nicolás Oyarzún Vergara,24-ago-12
Tomás Oyarzún Vergara,13-oct-10
Agustín Pavez,
Isis Pérez Urrieta,2-jun-14
Francisco Pérez,
Victoria Pericana Molina,16-mar-11
Viviana Pericana Molina,2-may-14
Maria Paz Pinzon Torrent,28-feb-03
Claudio Poblete Olguin,8-jun-09
Javiera Polverelli Castro,17-feb-15
Juan Ignacio Polverelli Castro,8-jun-10
Agustín Pueller Valenzuela,30-mar-15
Santiago Pueller Valenzuela,26-dic-17
Samantha Ramos Silva,25-oct-11
Micaella Ramos Silva,21-dic-06
Nelson Reis,18-nov-07
Tomas Reyes Morales,13-ene-10
Emilio Reyes Morales,8-jun-15
Ignacio Rocamora Galaz,29-ene-01
Martin Rocamora Vega,22-abr-06
Sebastian Rojas Silva,14-dic-12
Lorenzo Salgado Heredia,6-oct-09
Ana Gloria Salvador Avila,17-dic-12
Franco San Martin Saavedra,16-mar-12
Victoria Santibanez Sotelo,26-abr-10
Leonardo Santibanez Sotelo,24-jun-11
Antonia Suarez Aguilar,11-jun-14
Diego Tamayo Contreras,14-abr-15
Laura Tapia Lobos,1-jul-16
Florencia Varas Arriagada,10-dic-12
Alonso Vargas Figueroa,11-jul-16
Antonella Vasquez Arias,3-jun-09
Gustavo Vega Zapata,31-ago-15
Salvador Vega Zapata,25-nov-09
Catalina Villegas Carrasco,18-mar-15
Mateo Villegas Carrasco,4-ago-16"""

# CONFIG
DB_PATH = "data/natacion.db"
DEFAULT_TEAM_ID = "10034725" # CRNP
MONTHS_ES = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12}

def normalize_match_name(n):
    n = n.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

def parse_spanish_date(d_str):
    if not isinstance(d_str, str) or not d_str.strip(): return None
    try:
        parts = d_str.split('-')
        if len(parts) != 3: return None
        day = int(parts[0])
        mon_str = parts[1].lower().strip()
        year_short = int(parts[2])
        year = 2000 + year_short if year_short < 50 else 1900 + year_short
        month = MONTHS_ES.get(mon_str)
        if not month: return None
        return datetime.date(year, month, day).isoformat()
    except:
        return None

def import_swimmers():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Load Existing Swimmers
    cursor.execute("SELECT id, name, birth_date FROM swimmers")
    db_rows = cursor.fetchall()
    
    db_swimmers_norm = {}
    db_names_norm = []
    
    for row in db_rows:
        n_norm = normalize_match_name(row[1])
        db_swimmers_norm[n_norm] = row
        db_names_norm.append(n_norm)
        
    # Process CSV
    df = pd.read_csv(StringIO(csv_data))
    
    updated = 0
    inserted = 0
    
    print("\n--- Processing 93 Swimmers ---")
    
    for _, row in df.iterrows():
        raw_name = row['Nombre Completo'].strip()
        raw_dob = row['Fecha de Nacimiento']
        iso_dob = parse_spanish_date(raw_dob)
        
        csv_norm = normalize_match_name(raw_name)
        
        match_id = None
        match_name = None
        is_fuzzy = False
        
        # 1. Exact Match
        if csv_norm in db_swimmers_norm:
            match_id, match_name, curr_dob = db_swimmers_norm[csv_norm]
        else:
            # 2. Fuzzy Match STRICTER
            # token_sort_ratio handles "First Last" vs "First Middle Last" better than set_ratio for mismatched first names
            best_match_norm, score = process.extractOne(csv_norm, db_names_norm, scorer=fuzz.token_sort_ratio)
            
            # High threshold to avoid "Juan Perez" matching "Diego Perez"
            if score >= 85:
                # Double check: First token match? 
                # (Prevents "Gonzalez" matching all Gonzalezes)
                # Also prevents "Victoria" matching "Viviana" (Siblings)
                
                # Get the matched name from DB
                match_id, match_name, curr_dob = db_swimmers_norm[best_match_norm]
                
                # Compare first tokens
                token_csv = csv_norm.split()[0]
                token_db = best_match_norm.split()[0]
                
                # Allow for very minor typos in first name? No, risky. 
                # Require identical first token (normalized) OR extremely high ratio on first token
                if token_csv == token_db or fuzz.ratio(token_csv, token_db) > 90:
                    is_fuzzy = True
                else:
                    # print(f"Skipping potential sibling match: {raw_name} vs {match_name} ({score})")
                    match_id = None
            
        # ACTION
        if match_id:
            # UPDATE
            if iso_dob and curr_dob != iso_dob:
                cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (iso_dob, match_id))
                updated += 1
                print(f"✅ UPDATED: {raw_name} ({match_name}) -> {iso_dob}")
        else:
            # INSERT
            new_id = f"MANUAL_{uuid.uuid4().hex[:8]}"
            # Try to infer gender? No, safer NULL.
            cursor.execute(
                "INSERT INTO swimmers (id, name, url, team_id, birth_date, gender) VALUES (?, ?, ?, ?, ?, ?)",
                (new_id, raw_name, "Manual Import", DEFAULT_TEAM_ID, iso_dob, None)
            )
            inserted += 1
            print(f"🆕 INSERTED: {raw_name} -> {iso_dob}")
            
    conn.commit()
    conn.close()
    
    print(f"\nSummary:")
    print(f" Updated DOBs: {updated}")
    print(f" New Swimmers Inserted: {inserted}")

if __name__ == "__main__":
    import_swimmers()
