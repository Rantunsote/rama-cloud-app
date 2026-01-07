import sqlite3
import unicodedata

TARGET_DB = "/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db"
SOURCE_DB = "/Users/jrb/Documents/RAMA/swim_scraper/meet_mobile_dump.db"

# Backup Map (Copied from existing fix_gender.py)
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
    'Luciano': 'M', 'Santiago': 'M', 'Agustín': 'M', 'Tomás': 'M',
    'Matías': 'M', 'Sebastián': 'M', 'Nicolás': 'M', 'León': 'M',
    'Renato': 'M', 'Emilio': 'M', 'Gaspar': 'M', 'Rafael': 'M',
    'Bruno': 'M', 'Alonso': 'M', 'Joaquín': 'M', 'Vicente': 'M',
    'Dante': 'M', 'Santino': 'M', 'Benjamín': 'M', 'Cristóbal': 'M',
    'Fabián': 'M', 'Andrés': 'M', 'Fernando': 'M', 'Héctor': 'M',
    'Sergio': 'M', 'Rodrigo': 'M', 'Luis': 'M', 'Jorge': 'M',
    
    # FEMALE
    'Maria': 'F', 'Sofia': 'F', 'Valentina': 'F', 'Isabella': 'F',
    'Camila': 'F', 'Martina': 'F', 'Fernanda': 'F', 'Josefa': 'F',
    'Antonia': 'F', 'Emilia': 'F', 'Florencia': 'F', 'Isidora': 'F',
    'Catalina': 'F', 'Maite': 'F', 'Amanda': 'F', 'Trinidad': 'F',
    'Javiera': 'F', 'Constanza': 'F', 'Francisca': 'F', 'Rocio': 'F',
    'Pia': 'F', 'Magdalena': 'F', 'Lourdes': 'F', 'Amaya': 'F',
    'Samantha': 'F', 'Micaella': 'F', 'Victoria': 'F', 'Antonella': 'F',
    'Blanc': 'F', 'Blanca': 'F', 'Laura': 'F', 'Ema': 'F', 'Renata': 'F',
    'Roberta': 'F', 'Ana': 'F', 'Amalia': 'F', 'Ashley': 'F',
    'Giuliana': 'F', 'Milenka': 'F', 'Layla': 'F', 'Isis': 'F',
    'Julieta': 'F', 'Agustina': 'F', 'Monserrat': 'F', 'Colomba': 'F',
    'Dominga': 'F', 'Leonor': 'F', 'Elena': 'F', 'Violeta': 'F',
    'Josefina': 'F', 'Rafaela': 'F', 'Luciana': 'F', 'Elisa': 'F'
}

def normalize(n):
    return ''.join(c for c in unicodedata.normalize('NFD', n.lower().strip()) if unicodedata.category(c) != 'Mn')

def restore_gender():
    print("Connecting DBs...")
    conn_target = sqlite3.connect(TARGET_DB)
    # Ensure tables exist
    
    conn_source = sqlite3.connect(SOURCE_DB)
    
    # 1. Get Target Swimmers missing gender
    cursor_target = conn_target.cursor()
    cursor_target.execute("SELECT id, name FROM swimmers WHERE gender IS NULL OR gender = '' OR gender = 'None'")
    targets = cursor_target.fetchall()
    
    print(f"Found {len(targets)} missing gender.")
    
    # 2. Get All Source Swimmers for lookup
    cursor_source = conn_source.cursor()
    try:
        cursor_source.execute("SELECT firstName, lastName, gender FROM Swimmer")
        source_rows = cursor_source.fetchall()
    except Exception as e:
        print(f"Error reading source DB: {e}")
        source_rows = []
    
    # Build Lookup Map: normalized_full_name -> gender
    source_map = {}
    for fn, ln, g in source_rows:
        if not g: continue
        full = f"{fn} {ln}"
        source_map[normalize(full)] = g
        
    count = 0
    
    for rid, name in targets:
        nname = normalize(name)
        gender = source_map.get(nname)
        
        # Method 2: Dictionary
        if not gender:
            first = name.split()[0].title()
            if first in GENDER_MAP:
                gender = GENDER_MAP[first]
            else:
                 nfirst = normalize(first) 
                 for k, v in GENDER_MAP.items():
                     if normalize(k) == nfirst:
                         gender = v
                         break
        
        if gender:
            cursor_target.execute("UPDATE swimmers SET gender = ? WHERE id = ?", (gender, rid))
            count += 1
        else:
            print(f"WARN: Could not determine gender for {name}")

    conn_target.commit()
    conn_target.close()
    conn_source.close()
    print(f"Updated {count} records.")

if __name__ == "__main__":
    restore_gender()
