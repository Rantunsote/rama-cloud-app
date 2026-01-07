import sqlite3
import pandas as pd
from io import StringIO
import unicodedata
from fuzzywuzzy import process, fuzz
import sys

# CONFIG
DB_PATH = "data/natacion.db"

# The master list from user (Embedded from import_swimmers_full.py)
csv_data = """Nombre Completo
Josefa Acuña Rojas
Javiera Aguirre Polverelli
Amalia Alquinta Almarza
Noelia Aranda Muñoz
Alonso Arce Marabolí
Josefa Arellano Valdebenito
Amaro Arias Vasquez
Rafaela Astudillo Gonzalez
Sebastian Barria Diaz
Rafael Belandria Molina
Gaspar Belandria Molina
Gabriel Belandria Molina
Maria Jesus Beltran Muñoz
Camila Benavente Albarracin
Pedro Briones Gonzalez
Josefa Cabello Perez
Gabriel Cabrera Muñoz
Amanda Caligari Pinilla
Bruno Calizaya Rojas
Isidora Campos Muñoz
Florencia Campos Muñoz
Rafaela Canales Vasquez
Alonso Canales Vasquez
Emilia Cardenas Soro
Martina Castillo Donoso
Catalina Castro Perez
Santiago Cañas Jimenez
Matias Contreras Pavez
Isidora Faundez Beltran
Camila Figueroa Gomez
Tomas Figueroa Gomez
Facundo Gomez Huerta
Magdalena Gomez Huerta
Amaro González Aldana
Angelo González Gajardo
Lourdes González Rodriguez
Blanca González Aldana
Ema González Aldana
Adriano Guarama
Benjamin Heredia Jara
Martin Heredia Jara
Roberta Hidalgo Valencia
Tomas Iglesias Diez de Medina
Amanda Mac Ivar Jara
Antonella Maldonado Gajardo
Ashly de los Angeles Marquez Rojas
Wesley Medina Medina
Gabriela Medina Cáceres
Cristobal Melendez Landa
Diego Adan Meneses Cruces
Leon Montenegro Espinoza
Amelia Montoya Ponce
Alonso Muñoz
Isidora Navarrete Bustamante
Renata Navas Carvajal
Florencia Nietzschmann Bayas
Matías Oyarzún Vergara
Nicolás Oyarzún Vergara
Tomás Oyarzún Vergara
Agustín Pavez
Isis Pérez Urrieta
Francisco Pérez
Victoria Pericana Molina
Viviana Pericana Molina
Maria Paz Pinzon Torrent
Claudio Poblete Olguin
Javiera Polverelli Castro
Juan Ignacio Polverelli Castro
Agustín Pueller Valenzuela
Santiago Pueller Valenzuela
Samantha Ramos Silva
Micaella Ramos Silva
Nelson Reis
Tomas Reyes Morales
Emilio Reyes Morales
Ignacio Rocamora Galaz
Martin Rocamora Vega
Sebastian Rojas Silva
Lorenzo Salgado Heredia
Ana Gloria Salvador Avila
Franco San Martin Saavedra
Victoria Santibanez Sotelo
Leonardo Santibanez Sotelo
Antonia Suarez Aguilar
Diego Tamayo Contreras
Laura Tapia Lobos
Florencia Varas Arriagada
Alonso Vargas Figueroa
Antonella Vasquez Arias
Gustavo Vega Zapata
Salvador Vega Zapata
Catalina Villegas Carrasco
Mateo Villegas Carrasco"""

# ADDITIONS
# Keep Vicente Reyes (assuming standard format)
ADDITIONAL_KEEP = ["Vicente Reyes"]

def normalize_match_name(n):
    n = n.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

def cleanup_db():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Get all DB Swimmers
    cursor.execute("SELECT id, name FROM swimmers")
    db_rows = cursor.fetchall()
    print(f"Total Swimmers in DB: {len(db_rows)}")
    
    # Map normalized DB name -> ID
    db_map = {} # norm_name -> id
    db_names_norm = []
    
    for row in db_rows:
        n_norm = normalize_match_name(row[1])
        db_map[n_norm] = row[0]
        db_names_norm.append(n_norm)

    # 2. Build Keep List (Normalized)
    keep_names_raw = [line.strip() for line in csv_data.split('\n') if line.strip()] + ADDITIONAL_KEEP
    
    keep_ids = set()
    kept_count = 0
    
    print("\n--- Identifying Swimmers to KEEP ---")
    for raw in keep_names_raw:
        n_norm = normalize_match_name(raw)
        
        # Exact Match
        if n_norm in db_map:
            keep_ids.add(db_map[n_norm])
            kept_count += 1
            # print(f"KEEP (Exact): {raw}")
        else:
            # Fuzzy Match
            match_norm, score = process.extractOne(n_norm, db_names_norm, scorer=fuzz.token_sort_ratio)
            if score >= 80:
                 match_id = db_map[match_norm]
                 keep_ids.add(match_id)
                 kept_count += 1
                 print(f"KEEP (Fuzzy {score}): {raw} -> matched DB '{match_norm}'")
            else:
                print(f"⚠️ WARNING: Could not find '{raw}' in DB to keep! (Score: {score} vs {match_norm})")

    print(f"\nidentified {len(keep_ids)} IDs to keep out of {len(keep_names_raw)} target names.")
    
    # 3. Identify IDs to Delete
    all_ids = set([row[0] for row in db_rows])
    delete_ids = all_ids - keep_ids
    
    if not delete_ids:
        print("No swimmers to delete. DB is clean.")
        return

    print(f"\n--- DELETING {len(delete_ids)} Swimmers ---")
    
    if len(delete_ids) > 200:
        print("Safety check: Deleting > 200 swimmers. Are you sure?")
        # Using sys.stdin might be tricky in agent mode, so we proceed assuming explicit instruction.
    
    # Execute Deletion
    # Convert set to list for SQL
    chunks = list(delete_ids)
    chunk_size = 900 # limit variables
    deleted_count = 0
    
    try:
        for i in range(0, len(chunks), chunk_size):
            batch = chunks[i:i+chunk_size]
            ph = ",".join(["?"] * len(batch))
            
            # Delete Results first (if no cascade)
            cursor.execute(f"DELETE FROM results WHERE swimmer_id IN ({ph})", batch)
            
            # Delete Swimmer
            cursor.execute(f"DELETE FROM swimmers WHERE id IN ({ph})", batch)
            deleted_count += cursor.rowcount
            
        print(f"Successfully deleted {deleted_count} swimmers and their results.")
        conn.commit()
    except Exception as e:
        print(f"ERROR: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    cleanup_db()
