
import sqlite3

DB_PATH = "data/natacion.db"

def run():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE meets ADD COLUMN address TEXT")
        print("Added 'address' column to meets table.")
    except sqlite3.OperationalError as e:
        print(f"Column might already exist: {e}")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    run()
