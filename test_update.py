import sqlite3
import pandas as pd

DB_PATH = "data/natacion.db"

def test_update():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Read current
    sid = 2466442
    print(f"Checking ID {sid}...")
    
    cursor.execute("SELECT id, name, birth_date, gender FROM swimmers WHERE id=?", (sid,))
    row = cursor.fetchone()
    if not row:
        print("Swimmer not found!")
        return
        
    print(f"Current: {row}")
    
    # 2. Update
    new_dob = "2010-04-01"
    print(f"Updating to {new_dob}...")
    try:
        cursor.execute("UPDATE swimmers SET birth_date=? WHERE id=?", (new_dob, sid))
        conn.commit()
        print("Commit executed.")
    except Exception as e:
        print(f"Update failed: {e}")
        
    # 3. Read back
    cursor.execute("SELECT id, name, birth_date FROM swimmers WHERE id=?", (sid,))
    row_new = cursor.fetchone()
    print(f"New Value: {row_new}")
    
    if row_new[2] == new_dob:
        print("SUCCESS: DB update works.")
        
        # 4. Revert (optional, or keep if useful)
        # cursor.execute("UPDATE swimmers SET birth_date=NULL WHERE id=?", (sid,))
        # conn.commit()
        # print("Reverted.")
    else:
        print("FAILURE: Value did not persist.")
        
    conn.close()

if __name__ == "__main__":
    test_update()
