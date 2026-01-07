
import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = "/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db"

raw_data = """Josefa 	Acuña	Rojas	16-ene-10
Baltazar	Aguirre	Pizarro	11-ene-12
Javiera	Aguirre	Lipari	21-abr-15
Clemente	Aguirre	Gonzalez	8-may-14
Octavio 	Aguirre	Gonzalez	19-dic-09
Amanda 	Alquinta	Espinoza	9-ene-10
Amalia	Alquinta	Espinoza	23-mar-15
Amaya	Barahona	Reyes	18-mar-11
Leon	Barahona	Reyes	12-jun-09
Esteban	Barahona	Chacón	24-jun-10
Marcela 	Bermudez	Mayz	19-dic-08
Nicolas	Briones	Concha	11-jul-13
Vicente	Briones	Concha	11-jul-13
Isidora	Brito	Guerra	10-dic-06
Ignacio 	Carranza	Astudillo	6-ene-11
Amanda 	Carranza	Astudillo	24-abr-14
Maximiliano	Cereceda	Herrera	15-feb-04
Bastián	Cereceda	Herrera	1-abr-11
Profe César	Cereceda		23-abr-00
Emanuel	Cereceda	Muñoz	23-jul-08
Profe Oscar 	Cifuentes		07-ene-00
Profe Valeria 	Contalba		20-nov-99
Constanza	Contreras	Cid	8-feb-14
Matias	Contreras 	Moreno	11-mar-16
Francisco	Correa	Diaz	31-ene-09
Bryan	Davila	Cordova	11-may-10
Juan Martín	Duarte		26-nov-09
Belén	Galvez	Carrasco	8-mar-04
Martin 	Garces	Quiroz	1-abr-10
Antonio 	Garcia	Palma					
Blanca 	García	Galaz	1-jul-13
Facundo 	Gomez	Huerta	25-sept-11
Magdalena 	Gomez	Huerta	4-nov-14
Amaro	González	Aldana	11-feb-10
Angelo	González	Gajardo	14-abr-01
Lourdes	González	Rodriguez	29-jun-12
Blanca	González	Aldana	2-jul-14
Ema 	González	Aldana	2-jul-14
Adriano	Guarama		16-dic-09
Benjamin	Heredia	Jara	9-ago-17
Martin 	Heredia	Jara	17-nov-11
Roberta	Hidalgo	Valencia	23-dic-10
Tomas 	Iglesias	Diez de Medina	23-jun-06
Amanda 	Mac Ivar	Jara	21-abr-14
Antonella	Maldonado	Gajardo	31-dic-15
Ashly de los Angeles	Marquez 	Rojas	27-nov-14
Wesley	Medina	Medina	10-jul-14
Gabriela	Medina	Cáceres	21-dic-04
Cristobal 	Melendez	Landa	1-dic-16
Diego Adan	Meneses	Cruces	6-ago-10
Leon 	Montenegro	Espinoza	9-nov-16
Amelia	Montoya	Ponce	5-jun-15
Alonso	Muñoz		1-abr-09
Isidora	Navarrete	Bustamante	2-jul-15
Renata	Navas	Carvajal	9-dic-13
Florencia	Nietzschmann	Bayas	2-nov-13
Matías	Oyarzún	Vergara	3-jul-15
Nicolás	Oyarzún	Vergara	24-ago-12
Tomás	Oyarzún	Vergara	13-oct-10
Agustín	Pavez						
Isis	Pérez	Urrieta	2-jun-14
Francisco	Pérez						
Victoria 	Pericana	Molina	16-mar-11
Viviana	Pericana	Molina	2-may-14
Maria Paz	Pinzon 	Torrent	28-feb-03
Claudio	Poblete	Olguin	8-jun-09
Javiera 	Polverelli	Castro	17-feb-15
Juan Ignacio	Polverelli	Castro	8-jun-10
Agustín	Pueller	Valenzuela	30-mar-15
Santiago	Pueller	Valenzuela	26-dic-17
Samantha	Ramos	Silva	25-oct-11
Micaella	Ramos	Silva	21-dic-06
Nelson 	Reis		18-nov-07
Tomas	Reyes	Morales	13-ene-10
Emilio	Reyes	Morales	8-jun-15
Ignacio	Rocamora	Galaz	29-ene-01
Martin 	Rocamora	Vega	22-abr-06
Sebastian	Rojas	Silva	14-dic-12
Lorenzo	Salgado	Heredia	6-oct-09
Ana Gloria	Salvador	Avila	17-dic-12
Franco	San Martin 	Saavedra	16-mar-12
Victoria 	Santibanez	Sotelo	26-abr-10
Leonardo	Santibanez	Sotelo	24-jun-11
Antonia 	Suarez	Aguilar	11-jun-14
Diego 	Tamayo	Contreras	14-abr-15
Laura 	Tapia	Lobos	1-jul-16
Florencia	Varas	Arriagada	10-dic-12
Alonso 	Vargas	Figueroa	11-jul-16
Antonella	Vasquez	Arias	3-jun-09
Gustavo	Vega	Zapata	31-ago-15
Salvador	Vega	Zapata	25-nov-09
Catalina 	Villegas 	Carrasco	18-mar-15
Mateo 	Villegas 	Carrasco	4-ago-16"""

month_map = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sep': '09', 'sept': '09', 'oct': '10', 'nov': '11', 'dic': '12'
}

def parse_date(d_str):
    if not d_str or not d_str.strip(): return None
    try:
        parts = d_str.strip().split('-')
        if len(parts) != 3: return None
        day, month_txt, year_short = parts
        
        month = month_map.get(month_txt.lower())
        if not month: return None
        
        # Handle Year
        if len(year_short) == 2:
            y = int(year_short)
            # Pivot year: assuming DOBs, 00-26 is 2000-2026, 90-99 is 1990-1999
            if y < 30: year = 2000 + y
            else: year = 1900 + y
        else:
            year = int(year_short)
            
        return f"{year}-{month}-{str(day).zfill(2)}"
    except Exception as e:
        print(f"Failed to parse {d_str}: {e}")
        return None

def update_dobs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    lines = raw_data.strip().split('\n')
    updates = 0
    
    for line in lines:
        parts = line.split('\t')
        # Format varies, usually: Name, Surname1, Surname2, DOB
        # Let's clean and filter empty parts
        clean_parts = [p.strip() for p in parts if p.strip()]
        
        # Heuristic: Find the date part
        dob_candidate = None
        name_parts = []
        
        for p in clean_parts:
            # Check if looks like a date (contains - and month text?)
            if '-' in p and any(m in p.lower() for m in month_map.keys()):
                dob_candidate = p
                break
            # Skip instagram handles
            if '@' in p: continue
            # Skip numbers like Age
            if p.isdigit(): continue
            if p.upper() in ['OK', 'SI', 'NO TIENE']: continue
            
            name_parts.append(p)
            
        if not dob_candidate:
            continue
            
        full_name = " ".join(name_parts)
        parsed_dob = parse_date(dob_candidate)
        
        if not parsed_dob: continue
        
        # Update DB using fuzzy Name match logic or First+Last
        # Try finding swimmer by partial match
        # print(f"Processing: {full_name} -> {parsed_dob}")
        
        # 1. Try fuzzy match in DB
        # Simplistic approach: First Name + Last Name
        
        # Build search pattern
        # First word is FirstName, others are Surnames
        # Try `firstName Like %` AND `name LIKE %LastName%`
        tokens = full_name.split()
        if not tokens: continue
        first = tokens[0]
        last = tokens[1] if len(tokens) > 1 else ""
        
        # Try simple UPDATE
        # We assume database names might be "Josefa Acuna" vs "Josefa Acuña Rojas"
        # We match on First Name strict + Last Name partial
        
        query = f"""
            UPDATE swimmers 
            SET birth_date = ? 
            WHERE name LIKE '{first}%' AND name LIKE '%{last}%'
        """
        cursor.execute(query, (parsed_dob,))
        if cursor.rowcount > 0:
            updates += cursor.rowcount
            # print(f"  Updated: {full_name}")
        else:
            # Try removing accents from search?
            pass
            
    conn.commit()
    conn.close()
    print(f"Updated {updates} birth dates.")

if __name__ == "__main__":
    update_dobs()
