import sqlite3
from datetime import datetime

conn = sqlite3.connect("/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db")
cursor = conn.cursor()
cursor.execute("SELECT name, birth_date FROM swimmers WHERE birth_date IS NOT NULL")
rows = cursor.fetchall()
today = datetime.now() # 2026-01-07
count = 0
names = []

print(f"Calculating ages for date: {today.date()}")

for name, dob_str in rows:
    try:
        bd = datetime.strptime(dob_str, "%Y-%m-%d")
        age = today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
        
        if age == 11:
            count += 1
            names.append(f"{name} ({dob_str})")
            
    except:
        pass

print(f"Total 11 year olds: {count}")
for n in names:
    print(n)
