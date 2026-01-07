import sqlite3

from normalize_events import normalize_event_name_v2

DB_PATH = "data/natacion.db"

def run_migration():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Fetching distinct event names from results...")
    cursor.execute("SELECT DISTINCT event_name FROM results")
    rows = cursor.fetchall()
    
    count = 0
    updated_count = 0
    
    print(f"Found {len(rows)} distinct event names. Checking for normalization...")
    
    for row in rows:
        original = row[0]
        if not original: continue
        
        normalized = normalize_event_name_v2(original)
        
        if original != normalized:
            # print(f"  Mapping: '{original}' -> '{normalized}'")
            # Update all instances
            cursor.execute("UPDATE results SET event_name = ? WHERE event_name = ?", (normalized, original))
            updated_count += cursor.rowcount
            count += 1
            
    print(f"Migration Complete.")
    print(f"Standardized {count} unique event names.")
    print(f"Updated {updated_count} total rows in 'results'.")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    run_migration()
