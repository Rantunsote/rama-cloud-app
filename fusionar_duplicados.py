import sqlite3
import pandas as pd
from difflib import SequenceMatcher
import re
import os

# Ajusta la ruta base donde se encuentre el script
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT_DIR, "data", "natacion.db")

def normalize(name):
    n = str(name).lower()
    n = re.sub(r'(?i)resultados completos', '', n).strip()
    n = re.sub(r'seme\b', 'semestre', n)
    return n

def similar(a, b):
    na = normalize(a)
    nb = normalize(b)
    
    # Penalizar si los años son distintos explícitamente
    ya = re.search(r'202\d', na)
    yb = re.search(r'202\d', nb)
    if ya and yb and ya.group() != yb.group():
        return 0.0
        
    return SequenceMatcher(None, na, nb).ratio()

def clean_duplicates():
    print(f"Conectando a la base de datos en: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("ERROR: Base de datos no encontrada.")
        return

    conn = sqlite3.connect(DB_PATH)
    meets = pd.read_sql("SELECT id, name, date FROM meets", conn)
    c = conn.cursor()

    f_meets = meets[meets['id'].str.startswith('F_')]
    mm_meets = meets[~meets['id'].str.startswith('F_')]

    if f_meets.empty:
        print("No hay competencias nuevas de Fechida (prefijo F_) por procesar.")
        return

    print(f"Evaluando {len(f_meets)} torneos recientes de Fechida...")

    for _, fm in f_meets.iterrows():
        best_score = 0
        best_match = None
        
        for _, mm in mm_meets.iterrows():
            sc = similar(fm['name'], mm['name'])
            if sc > best_score:
                best_score = sc
                best_match = mm
                
        # Umbral alto para evitar mezclar torneos distintos
        if best_score >= 0.85:
            # Es un duplicado. Pasamos los resultados al original y borramos el nuevo
            c.execute("UPDATE results SET meet_id = ? WHERE meet_id = ?", (best_match['id'], fm['id']))
            c.execute("DELETE FROM meets WHERE id = ?", (fm['id'],))
            print(f"[FUSIONADO] '{fm['name']}' -> '{best_match['name']}' (Similitud: {best_score:.2f})")
        else:
            # Es un torneo realmente nuevo, sólo le limpiamos el nombre
            clean = re.sub(r'(?i)resultados completos', '', str(fm['name'])).strip()
            c.execute("UPDATE meets SET name = ? WHERE id = ?", (clean, fm['id']))
            print(f"[NUEVO TORNEO] Renombrado '{fm['name']}' -> '{clean}'")

    conn.commit()
    conn.close()
    print("\nProceso de fusión y limpieza completado con éxito.")

if __name__ == "__main__":
    clean_duplicates()
