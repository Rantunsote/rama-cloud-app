import sqlite3
import os

DB_PATH = "/app/data/natacion.db"

def verify():
    print(f"Checking DB at {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("DB does not exist!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Read current
    # Martin Garces ID: 2466442
    target_id = 2466442
    
    row = cursor.execute("SELECT birth_date FROM swimmers WHERE id = ?", (target_id,)).fetchone()
    print(f"Current Value: {row}")
    
    # 2. Update to "2010-01-04"
    new_val = "2010-01-04"
    print(f"Updating to {new_val}...")
    cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (new_val, target_id))
    print(f"Rowcount: {cursor.rowcount}")
    conn.commit()
    conn.close()
    
    # 3. Read back in NEW connection
    print("Reconnecting...")
    conn2 = sqlite3.connect(DB_PATH)
    cursor2 = conn2.cursor()
    row2 = cursor2.execute("SELECT birth_date FROM swimmers WHERE id = ?", (target_id,)).fetchone()
    print(f"Readback Value: {row2}")
    conn2.close()
    
    if row2 and row2[0] == new_val:
        print("SUCCESS: Persistence confirmed.")
    else:
        print("FAILURE: Value did not persist.")

if __name__ == "__main__":
    verify()
