import sqlite3

def normalize_names():
    conn = sqlite3.connect("natacion.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name FROM swimmers")
    rows = cursor.fetchall()
    
    count = 0
    for s_id, name in rows:
        if not name: continue
        
        # Title case (e.g. "amaro gonzalez" -> "Amaro Gonzalez")
        # Handling edge cases like "Mc" or "De La" is complex, but .title() is a good 90% solution.
        # "Rama DE Natacion" -> "Rama De Natacion"
        
        # Simple Title Case
        new_name = name.title()
        
        if new_name != name:
            print(f"Updating: '{name}' -> '{new_name}'")
            cursor.execute("UPDATE swimmers SET name = ? WHERE id = ?", (new_name, s_id))
            count += 1
            
    conn.commit()
    conn.close()
    print(f"Total normalized: {count}")

if __name__ == "__main__":
    normalize_names()
