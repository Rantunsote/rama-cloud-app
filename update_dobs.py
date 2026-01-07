
import sqlite3
import pandas as pd
from io import StringIO
import datetime

# DATA FROM GOOGLE SHEET
csv_data = """Nombre Completo,Fecha de Nacimiento
Josefa Acuña Rojas,16-ene-10
Baltazar Aguirre Pizarro,11-ene-12
Javiera Aguirre Lipari,21-abr-15
Clemente Aguirre Gonzalez,8-may-14
Octavio Aguirre Gonzalez,19-dic-09
Amanda Alquinta Espinoza,9-ene-10
Amalia Alquinta Espinoza,23-mar-15
Amaro Antilen Inostroza,2-ago-11
Maite Antilen Inostroza,10-ene-10
Cristobal Antilen Inostroza,4-jul-13
Dominga Ayala Rivera,17-jun-13
Diego Barraza Tapia,23-ene-11
Josefina Barraza Tapia,3-jul-13
Julian Barrera Galarce,10-abr-12
Baltazar Beltran Cerpa,21-may-09
Dominga Bozo Gacitua,6-mar-12
Clemente Bozo Gacitua,25-jun-15
Agustin Bugueño Miranda,19-sep-12
Florencia Bugueño Miranda,1-sep-12
Leonor Cardenas Meza,15-ene-13
Amapola Cardenas Meza,16-ago-14
Martin Carrasco Viveros,21-dic-10
Colomba Carrasco Viveros,18-nov-14
Maria Ignacia Carvacho Flores,18-mar-07
Jose Manuel Carvacho Flores,7-dic-10
Santiago Carvacho Flores,13-feb-13
Tomas Cedamanos Lopez,10-feb-16
Valentina Cedamanos Lopez,21-sep-12
Matias Contreras Miranda,16-feb-17
Sofia Corvalan Soto,24-may-12
Baltazar Duarte Reyes,4-ago-11
Isidora Faundez Olguín,12-dic-15
Isidora Fernandez Fernandez,27-sep-07
Julieta Fernandez Fernandez,6-ene-15
Gaspar Galindo Fuentes,24-oct-12
Trinidad Galvez Perez,11-jun-14
Amanda Garcia Villarroel,26-may-11
Nicolas Gonzalez Gonzalez,16-jun-11
Valentina Gonzalez Saavedra,13-oct-10
Benjamin Heredia Miranda,26-abr-16
Camila Hernandez Gomez,19-oct-04
Catalina Hernandez Gomez,26-nov-07
Simon Herrera Rodriguez,5-may-11
Mateo Jimenez Ortiz,21-dic-11
Pedro Jimenez Ortiz,25-abr-14
Martin Lagos Hernandez,24-nov-10
Agustin Lira Villaroel,20-dic-08
Trinidad Lira Villaroel,22-dic-11
Antonella Maldonado Diaz,22-dic-16
Emma Martinez Gana,4-abr-16
Sofia Medina Saavedra,17-jun-11
Cristobal Melendez Reyes,29-ene-16
Alfonso Mendez Mendez,2-abr-09
Isidora Navarrete Bustamante,2-jul-15
Renata Navas Carvajal,9-dic-13
Florencia Nietzschmann Bayas,2-nov-13
Matias Oyarzun Vergara,3-jul-15
Nicolás Oyarzún Vergara,24-ago-12
Tomás Oyarzún Vergara,13-oct-10
Agustín Pavez,N/A
Isis Pérez Urrieta,2-jun-14
Francisco Pérez,N/A
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

# SPANISH MONTH MAPPING
MONTHS_ES = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
}

def parse_spanish_date(d_str):
    if not d_str or d_str == "N/A": return None
    try:
        # Expected: d-mm-yy (16-ene-10)
        parts = d_str.split('-')
        if len(parts) != 3: return None
        
        day = int(parts[0])
        mon_str = parts[1].lower()
        year_short = int(parts[2])
        
        # Assumption: 2000s vs 1900s
        # If year < 50 => 20xx, else 19xx (heuristic)
        year = 2000 + year_short if year_short < 50 else 1900 + year_short
        
        month = MONTHS_ES.get(mon_str)
        if not month: return None
        
        return datetime.date(year, month, day).isoformat()
    except Exception as e:
        # print(f"Error parsing {d_str}: {e}")
        return None

import unicodedata
from fuzzywuzzy import process, fuzz

DB_PATH = "data/natacion.db"

def normalize_match_name(n):
    n = n.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

def update_db():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Load DB Swimmers
    cursor.execute("SELECT id, name, birth_date FROM swimmers")
    db_rows = cursor.fetchall()
    
    # Map Normalized Name -> Row
    # Also keep raw map
    db_swimmers_norm = {}
    db_names_norm = []
    
    for row in db_rows:
        n_norm = normalize_match_name(row[1])
        db_swimmers_norm[n_norm] = row
        db_names_norm.append(n_norm)
    
    # 2. Process CSV
    df = pd.read_csv(StringIO(csv_data))
    
    updated_count = 0
    not_found = []
    
    print("\n--- Processing Updates ---")
    
    for _, row in df.iterrows():
        raw_name = row['Nombre Completo'].strip()
        raw_dob = row['Fecha de Nacimiento']
        
        # Parse Date
        iso_dob = parse_spanish_date(raw_dob)
        if not iso_dob:
            continue
            
        csv_norm = normalize_match_name(raw_name)
        
        match_id = None
        match_name = None
        
        # 1. Exact Normalized Match
        if csv_norm in db_swimmers_norm:
            val = db_swimmers_norm[csv_norm]
            match_id = val[0]
            match_name = val[1]
        else:
            # 2. Fuzzy Match on NORMALIZED strings
            best_match_norm, score = process.extractOne(csv_norm, db_names_norm, scorer=fuzz.token_set_ratio)
            
            # Mapping back is tricky if multiple names normalize to same, but unlikely for swimmers
            # Find original row
            if score >= 80:
                val = db_swimmers_norm[best_match_norm]
                match_id = val[0]
                match_name = val[1]
                # print(f"Fuzzy: '{raw_name}' -> '{match_name}' ({score})")
            else:
                not_found.append(f"{raw_name}")
                continue
                
        # UPDATE
        if match_id:
            current_dob = db_swimmers_norm[normalize_match_name(match_name)][2]
            if current_dob != iso_dob:
                cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (iso_dob, match_id))
                updated_count += 1
                # print(f"Updated: {match_name} -> {iso_dob}")
    
    conn.commit()
    conn.close()
    
    with open("missing_swimmers.txt", "w") as f:
        for n in not_found:
            f.write(f"{n}\n")
            
    print(f"Saved {len(not_found)} missing swimmers to missing_swimmers.txt")
    
    print(f"\nTotal Updated: {updated_count}")
    print(f"Not Found in DB ({len(not_found)}):")
    for n in not_found:
        print(f" - {n}")

if __name__ == "__main__":
    update_db()
