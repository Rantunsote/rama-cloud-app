import sqlite3
import pandas as pd
from datetime import datetime
import os

DB_PATH = "/Users/jrb/Documents/RAMA/swim_scraper/data/natacion.db"
TEAM_ID = "10034725"

def get_connection():
    return sqlite3.connect(DB_PATH)

def load_swimmers():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM swimmers WHERE team_id = ? ORDER BY name", conn, params=(TEAM_ID,))
    conn.close()
    
    # Same logic as app.py
    d_iso = pd.to_datetime(df['birth_date'], format='%Y-%m-%d', errors='coerce')
    d_dmy = pd.to_datetime(df['birth_date'], dayfirst=True, errors='coerce')
    df['birth_date'] = d_iso.fillna(d_dmy)
    return df

def get_age(dob):
    if pd.isna(dob): return "NaT"
    try:
        today = datetime.now()
        val = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return val
    except Exception as e:
        return f"Error: {e}"

df = load_swimmers()
targets = ['Amanda Carranza', 'Wesley Medina', 'Blanca Gonzalez', 'Constanza Contreras']
print(f"Total Swimmers: {len(df)}")

for name in targets:
    row = df[df['name'].str.contains(name)]
    if not row.empty:
        dob = row.iloc[0]['birth_date']
        print(f"\nName: {name}")
        print(f"Raw DOB: {dob} (Type: {type(dob)})")
        print(f"Age: {get_age(dob)}")
    else:
        print(f"\n{name} NOT FOUND in DF")
