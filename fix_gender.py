
import sqlite3
import os

# Extensive Gender Map based on user's DB content + common names
GENDER_MAP = {
    # MALE
    'Clemente': 'M', 'Baltazar': 'M', 'Octavio': 'M', 'Esteban': 'M', 
    'Leon': 'M', 'Nicolas': 'M', 'Vicente': 'M', 'Ignacio': 'M', 
    'Bastian': 'M', 'Emanuel': 'M', 'Maximiliano': 'M', 'Matias': 'M',
    'Francisco': 'M', 'Bryan': 'M', 'Juan': 'M', 'Maximo': 'M',
    'Antonio': 'M', 'Theo': 'M', 'Facundo': 'M', 'Amaro': 'M',
    'Angelo': 'M', 'Adriano': 'M', 'Benjamin': 'M', 'Martin': 'M',
    'Tomas': 'M', 'Wesley': 'M', 'Cristobal': 'M', 'Diego': 'M',
    'Alonso': 'M', 'Claudio': 'M', 'Agustin': 'M', 'Nelson': 'M',
    'Sebastian': 'M', 'Lorenzo': 'M', 'Franco': 'M', 'Leonardo': 'M',
    'Gustavo': 'M', 'Salvador': 'M', 'Mateo': 'M', 'Joaquin': 'M',
    'Pedro': 'M', 'Pablo': 'M', 'Felipe': 'M', 'Ricardo': 'M',
    'Eduardo': 'M', 'Javier': 'M', 'Carlos': 'M', 'Jose': 'M',
    'Lucas': 'M', 'Hugo': 'M', 'Daniel': 'M', 'Alejandro': 'M',
    'David': 'M', 'Simon': 'M', 'Julian': 'M', 'Gabriel': 'M',
    
    # FEMALE (Just in case)
    'Maria': 'F', 'Sofia': 'F', 'Valentina': 'F', 'Isabella': 'F',
    'Camila': 'F', 'Martina': 'F', 'Fernanda': 'F', 'Josefa': 'F',
    'Antonia': 'F', 'Emilia': 'F', 'Florencia': 'F', 'Isidora': 'F',
    'Catalina': 'F', 'Maite': 'F', 'Amanda': 'F', 'Trinidad': 'F',
    'Javiera': 'F', 'Constanza': 'F', 'Francisca': 'F', 'Rocio': 'F'
}

DB_PATH = '/app/data/natacion.db'

def fix_genders():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("--- Starting Gender Auto-Assignment ---")
    
    # Get all swimmers with missing gender
    cursor.execute("SELECT id, name FROM swimmers WHERE gender IS NULL OR gender = '' OR gender = 'None'")
    rows = cursor.fetchall()
    
    updates = 0
    unknowns = []
    
    for rid, name in rows:
        first_name = name.split(' ')[0].strip()
        # Clean special chars if needed, but dictionary handles accents roughly or we can normalize
        # Simple lookup
        gender = GENDER_MAP.get(first_name)
        
        # Try case insensitive
        if not gender:
            for k, v in GENDER_MAP.items():
                if k.lower() == first_name.lower():
                    gender = v
                    break
        
        if gender:
            cursor.execute("UPDATE swimmers SET gender = ? WHERE id = ?", (gender, rid))
            print(f"✅ Assigned {gender} to {name} (ID: {rid})")
            updates += 1
        else:
            unknowns.append(first_name)
            
    conn.commit()
    conn.close()
    
    print("---------------------------------------")
    print(f"Total Updates: {updates}")
    if unknowns:
        print(f"⚠️ Could not identify gender for: {set(unknowns)}")
    print("Done.")

if __name__ == "__main__":
    fix_genders()
