import sqlite3

conn = sqlite3.connect('/app/data/natacion.db')
cursor = conn.cursor()

# 1. Find the meet (just ID and Name)
print("Searching for meet...")
cursor.execute("SELECT id, name FROM meets WHERE name LIKE '%UT SC Senior Championship%'")
meets = cursor.fetchall()

if not meets:
    print("Meet not found.")
else:
    for m in meets:
        meet_id, name = m
        print(f"Found Meet: ID={meet_id}, Name='{name}'")
        
        # 2. Count results to be deleted
        cursor.execute("SELECT COUNT(*) FROM results WHERE meet_id = ?", (meet_id,))
        count = cursor.fetchone()[0]
        print(f"  - Deleting {count} results associated with this meet...")
        
        # 3. Delete results
        cursor.execute("DELETE FROM results WHERE meet_id = ?", (meet_id,))
        
        # 4. Delete meet
        cursor.execute("DELETE FROM meets WHERE id = ?", (meet_id,))
        print("  - Meet and results deleted.")

conn.commit()
conn.close()
