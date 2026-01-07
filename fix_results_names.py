
import sqlite3
from normalize_events import normalize_event_name_v2

DB_PATH = "data/natacion.db"

def run():
    print("Connecting to DB...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Get all unique event names from results to minimize processing
    cursor.execute("SELECT DISTINCT event_name FROM results")
    unique_names = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {len(unique_names)} unique event names.")
    
    mapping = {}
    for raw in unique_names:
        norm = normalize_event_name_v2(raw)
        if norm != raw and norm:
            mapping[raw] = norm
            
    print(f"Identified {len(mapping)} names to normalize.")
    
    # 2. Bulk Update
    count = 0
    for raw, norm in mapping.items():
        # Update all rows with this raw name
        # Use parameterized query
        cursor.execute("UPDATE results SET event_name = ? WHERE event_name = ?", (norm, raw))
        count += cursor.rowcount
        print(f"Updated '{raw}' -> '{norm}' ({cursor.rowcount} rows)")
        
    conn.commit()
    conn.close()
    print(f"Total rows updated: {count}")

if __name__ == "__main__":
    run()
