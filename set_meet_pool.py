import sqlite3
import sys

DB_PATH = 'data/natacion.db'

def list_meets():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, date, pool_size FROM meets ORDER BY date DESC")
    print("\nExisting Meets:")
    print(f"{'ID':<15} {'Date':<12} {'Size':<6} {'Name'}")
    print("-" * 60)
    for row in cursor.fetchall():
        pid = row[0]
        pdate = row[2]
        psize = row[3] if row[3] else "N/A"
        pname = row[1]
        print(f"{pid:<15} {pdate:<12} {psize:<6} {pname}")
    conn.close()

def set_pool_size(meet_id, new_size):
    if new_size not in ['25m', '50m']:
        print("Error: Size must be '25m' or '50m'")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Update Meet
    cursor.execute("UPDATE meets SET pool_size = ? WHERE id = ?", (new_size, meet_id))
    if cursor.rowcount == 0:
        print(f"Meet {meet_id} not found.")
        conn.close()
        return

    # 2. Update Results
    cursor.execute("UPDATE results SET pool_size = ? WHERE meet_id = ?", (new_size, meet_id))
    
    conn.commit()
    conn.close()
    print(f"Success: Updated Meet {meet_id} and its results to {new_size}.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        list_meets()
        print("\nUsage: python3 set_meet_pool.py <MEET_ID> <25m|50m>")
    else:
        set_pool_size(sys.argv[1], sys.argv[2])
