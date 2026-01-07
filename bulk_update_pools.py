
import sqlite3

DB_PATH = "data/natacion.db"

def run_updates():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Rule 1: Nacional / Copa Chile -> Kristel Köbrich (50m)
    # Using 'Kk' as user requested, expanded to full name for quality
    addr_kk = "Centro Acuático Kristel Köbrich, Santiago, CL"
    
    print("Applying Rule 1: Nacional / Copa Chile...")
    cursor.execute("""
        SELECT id, name FROM meets 
        WHERE name LIKE '%Nacional%' OR name LIKE '%Copa Chile%'
    """)
    meets_1 = cursor.fetchall()
    
    ids_1 = [m[0] for m in meets_1]
    if ids_1:
        # Update Meets
        placeholders = ','.join('?' for _ in ids_1)
        cursor.execute(f"UPDATE meets SET pool_size='50m', address=? WHERE id IN ({placeholders})", [addr_kk] + ids_1)
        # Update Results
        cursor.execute(f"UPDATE results SET pool_size='50m' WHERE meet_id IN ({placeholders})", ids_1)
        print(f"  Updated {len(ids_1)} meets to 50m / Kristel Köbrich.")

    # Rule 2: UC -> Club Deportivo UC (50m)
    addr_uc = "Club Deportivo Universidad Católica, Santiago, CL"
    
    print("Applying Rule 2: UC...")
    cursor.execute("""
        SELECT id, name FROM meets 
        WHERE name LIKE '%UC%' OR name LIKE '%Cduc%'
    """)
    meets_2 = cursor.fetchall()
    
    ids_2 = [m[0] for m in meets_2]
    if ids_2:
        # Update Meets
        placeholders = ','.join('?' for _ in ids_2)
        cursor.execute(f"UPDATE meets SET pool_size='50m', address=? WHERE id IN ({placeholders})", [addr_uc] + ids_2)
        # Update Results
        cursor.execute(f"UPDATE results SET pool_size='50m' WHERE meet_id IN ({placeholders})", ids_2)
        print(f"  Updated {len(ids_2)} meets to 50m / Universidad Católica.")

    conn.commit()
    conn.close()
    print("Bulk update complete.")

if __name__ == "__main__":
    run_updates()
