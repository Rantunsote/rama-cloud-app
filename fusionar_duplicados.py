import sqlite3
import pandas as pd
import re
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "natacion.db")

# Hardcoded true mapping to completely bypass any algorithmic failure
MERGE_MAP = {
    # Nombres EXACTOS de Fechida -> IDs EXACTOS de MeetMobile en la base
    "Resultados Completos Copa Chile Etapa 3": "MM_5175102",  # Esto es Etapa 3 2025
    "Resultados Completos Copa Chile 2S 2025 Etapa 2": "MM_5319108",
    "Resultados Completos Copa Chile 2026 Etapa 1 Primer Semestre": "MANUAL_COPA_CHILE_2026_1",
    "Resultados Completos Copa Chile 2025 Etapa 1": "MM_5300206",
    
    "Resultados Completos Campeonato Nacional Infantil De Verano 2026": "MM_5612109",
    "Resultados Completos Campeonato Nacional De Desarrollo Verano 2026": "MM_5593509",
    "Resultados Completos Festival De Menores Invierno 2025": "MM_5202805",
}

def clean_duplicates():
    print(f"Abriendo DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    meets = pd.read_sql("SELECT id, name FROM meets", conn)
    f_meets = meets[meets['id'].str.startswith('F_')]
    
    if f_meets.empty:
        print("No hay meets pendientes.")
        return

    for _, fm in f_meets.iterrows():
        fechida_name = str(fm['name']).strip()
        f_id = fm['id']
        
        if fechida_name in MERGE_MAP:
            target_id = MERGE_MAP[fechida_name]
            
            # Verify target exists
            c.execute("SELECT name FROM meets WHERE id=?", (target_id,))
            res = c.fetchone()
            if res:
                print(f"[FUSIONADO] '{fechida_name}' -> '{res[0]}'")
                c.execute("UPDATE results SET meet_id = ? WHERE meet_id = ?", (target_id, f_id))
                c.execute("DELETE FROM meets WHERE id = ?", (f_id,))
            else:
                print(f"Error: Target {target_id} no existe en la base. Dejando como nuevo torneo.")
        else:
            # Nuevo torneo (no tiene equivalente previo en MeetMobile)
            clean = re.sub(r'(?i)resultados completos', '', fechida_name).strip()
            c.execute("UPDATE meets SET name = ? WHERE id = ?", (clean, f_id))
            print(f"[NUEVO TORNEO] Renombrado '{fechida_name}' -> '{clean}'")

    conn.commit()
    conn.close()
    print("\n¡Fusionado con precisión!")

if __name__ == "__main__":
    clean_duplicates()
