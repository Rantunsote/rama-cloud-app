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
ADDITIONAL_KEEP = ["Vicente Reyes"]

def normalize_match_name(n):
    n = n.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')

def smart_cleanup_db():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Get all DB Swimmers with their RowID to handle robustly
    cursor.execute("SELECT id, name FROM swimmers")
    # Store as dictionary for easy access: id -> name
    all_db_swimmers = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"Total Swimmers in DB: {len(all_db_swimmers)}")
    
    # 2. Build Target List (The ones we WANT to keep/merge into)
    target_names_raw = [line.strip() for line in csv_data.split('\n') if line.strip()] + ADDITIONAL_KEEP
    
    # Identify which IDs in DB *are* the target names
    # We normalized names for matching
    
    # ID of the "Official" target profile in DB (if exists)
    # target_norm -> [id1, id2...] 
    # Ideally should be unique but might have duplicates. 
    # If duplicates exist in "Official" list in DB, we pick one as master.
    
    # Let's map Normalize(DB_Name) -> List of IDs
    db_lookup = {}
    for sid, sname in all_db_swimmers.items():
        norm = normalize_match_name(sname)
        if norm not in db_lookup: db_lookup[norm] = []
        db_lookup[norm].append(sid)
        
    # Find Official IDs
    official_ids = set()
    # map official_norm -> official_id (The one we will keep)
    official_map_norm_to_id = {} 
    
    print("\n--- Identifying OFFICIAL Targets in DB ---")
    for target in target_names_raw:
        tnorm = normalize_match_name(target)
        
        # 1. Exact Match in DB?
        if tnorm in db_lookup:
            # Pick the "best" one? Or just the first one?
            # Prefer MANUAL_ IDs if available? Or those with most metadata?
            # For now, just pick the first one found as "Master"
            candidates = db_lookup[tnorm]
            master_id = candidates[0] 
            
            official_ids.add(master_id)
            official_map_norm_to_id[tnorm] = master_id
            
            # If there are duplicate EXACT matches, mark others for merge later?
            # No, complicated. Let's stick to simple logic first.
        else:
            # Check Fuzzy Match against DB?
            # Theoretically the IMPORT script already inserted them. 
            # So they SHOULD be there exactly.
            # If not, try fuzzy match to find the "Official" one in DB?
            # (e.g. if import script failed or did something else)
             pass

    print(f"identified {len(official_ids)} Official Profile IDs.")
    
    # 3. Process Non-Official Swimmers
    # Identify candidates for DELETE or MERGE
    
    merged_count = 0
    deleted_count = 0
    
    db_names_list = list(db_lookup.keys()) # For searching
    
    print("\n--- Processing Candidates for Cleanup ---")
    
    for sid, sname in all_db_swimmers.items():
        if sid in official_ids:
            continue # Skip, this is a keeper
            
        snorm = normalize_match_name(sname)
        
        # Is this an exact alias of an official name? (e.g. Duplicate exact name)
        # If so, merge into the official one.
        if snorm in official_map_norm_to_id:
            target_id = official_map_norm_to_id[snorm]
            if target_id != sid:
                print(f"🔄 MERGE (Exact Duplicate): {sname} -> {all_db_swimmers[target_id]}")
                cursor.execute("UPDATE results SET swimmer_id = ? WHERE swimmer_id = ?", (target_id, sid))
                cursor.execute("DELETE FROM swimmers WHERE id = ?", (sid,))
                merged_count += 1
                continue

        # Not exact match. Try FUZZY match against OFFICAL TARGETS.
        # We need to match 'sname' against 'target_names_raw'
        # If match -> Find the corresponding Official ID -> Merge.
        
        # Map target_norm back to target_raw for display? No need.
        # We need to match snorm against keys of official_map_norm_to_id
        
        # Use token_set_ratio to handle "Emilio Reyes" -> "Emilio Reyes Morales" (Score 100)
        # But still check first name to avoid "Juan Perez" matching "Diego Perez"
        best_match_norm, score = process.extractOne(snorm, list(official_map_norm_to_id.keys()), scorer=fuzz.token_set_ratio)
        
        if score >= 90:
            # Verify first token again (Crucial)
            token_db = snorm.split()[0]
            token_target = best_match_norm.split()[0]
            
            if token_db == token_target or fuzz.ratio(token_db, token_target) > 90:
                # MERGE!
                target_id = official_map_norm_to_id[best_match_norm]
                target_name = all_db_swimmers[target_id]
                
                print(f"🔄 MERGE (Fuzzy {score}): {sname} -> {target_name}")
                
                # Transfer Results
                cursor.execute("UPDATE results SET swimmer_id = ? WHERE swimmer_id = ?", (target_id, sid))
                
                # Delete old swimmer
                cursor.execute("DELETE FROM swimmers WHERE id = ?", (sid,))
                merged_count += 1
                continue
        
        # If we got here, no match found. DELETE.
        # print(f"❌ DELETE (Ghost): {sname} (No match found)")
        cursor.execute("DELETE FROM results WHERE swimmer_id = ?", (sid,))
        cursor.execute("DELETE FROM swimmers WHERE id = ?", (sid,))
        deleted_count += 1

    conn.commit()
    conn.close()
    
    print(f"\nSummary:")
    print(f" Merged Swimmers: {merged_count}")
    print(f" Deleted Ghosts: {deleted_count}")
    print(f" Total Remaining: {len(official_ids)}")

if __name__ == "__main__":
    smart_cleanup_db()
