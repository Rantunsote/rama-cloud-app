
import sqlite3

DB_PATH = "data/natacion.db"

MERGE_MAP = {
    # [Verified Duplicates]
    "350912": "MM_4815304", # COPA KIDS UC
    "350921": "MM_5175102", # COPA CHILE ETAPA 3
    "340712": "MM_5129208", # CAMPEONATO NACIONAL
    "350753": "MM_5202805", # Festival Menores Invierno
    "350757": "MM_5221901", # Nacional Infantil Invierno
    "350977": "MM_5300206", # Copa Chile Etapa 1
    "353189": "MM_5319108", # Circuito Copa Chile 2
    "356108": "MM_5348908", # Circuito Copa Chile 3
}

def merge_meets():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        for old_id, new_id in MERGE_MAP.items():
            print(f"Merging {old_id} -> {new_id}...")
            
            # 1. Verify both exist
            cursor.execute("SELECT name FROM meets WHERE id = ?", (old_id,))
            src = cursor.fetchone()
            cursor.execute("SELECT name FROM meets WHERE id = ?", (new_id,))
            dst = cursor.fetchone()
            
            if not src or not dst:
                print(f"  Skipping: One of them missing. Src: {src}, Dst: {dst}")
                continue
                
            print(f"  Source: {src[0]}")
            print(f"  Target: {dst[0]}")
            
            # 2. Update Results
            cursor.execute("UPDATE results SET meet_id = ? WHERE meet_id = ?", (new_id, old_id))
            print(f"  Moved {cursor.rowcount} results.")
            
            # 3. Delete Old Meet
            cursor.execute("DELETE FROM meets WHERE id = ?", (old_id,))
            print(f"  Deleted meet {old_id}.")
            
        conn.commit()
        print("Merge complete.")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    merge_meets()
