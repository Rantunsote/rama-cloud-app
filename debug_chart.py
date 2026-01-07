
import sqlite3
import pandas as pd
from datetime import datetime
import sys

# Mocking app.py functions
def get_connection():
    return sqlite3.connect('data/natacion.db')

def parse_time(time_str):
    try:
        if not time_str or not time_str[0].isdigit():
            return None
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        return float(time_str)
    except:
        return None

def find_match_val(df_source, p_size, s_gen, s_age, val_col):
    if df_source.empty: 
        print("  DEBUG: Source DF is empty")
        return None
    
    # Filter
    print(f"  DEBUG: Filtering for Event=... Pool={p_size} Gen={s_gen} Age={s_age}")
    
    f = df_source[
        (df_source['pool_size'] == p_size) & 
        (df_source['gender'] == s_gen)
    ]
    
    if f.empty:
        print(f"  DEBUG: No matches for Pool/Gender. Available Genders: {df_source['gender'].unique()}")
        return None
        
    print(f"  DEBUG: Found {len(f)} potential rows. Checking categories...")
    
    for _, r in f.iterrows():
        raw_code = r['category_code']
        code = str(raw_code).replace(' años', '').replace(' ', '')
        
        # print(f"    Checking row: {raw_code} -> {code}")
        
        # 1. Open/Absoluto (Fixed Logic)
        if 'open' in code.lower() or 'todo' in code.lower() or 'absoluto' in code.lower():
            print(f"    MATCH: Absoluto/Open found! Val: {r[val_col]}")
            return r[val_col]
            
        # 2. Range 11-12
        if '-' in code:
            try:
                low, high = map(int, code.split('-'))
                if low <= s_age <= high: 
                    print(f"    MATCH: Range {low}-{high} for age {s_age}. Val: {r[val_col]}")
                    return r[val_col]
            except: pass
            
        # 3. Single Digit
        elif code.isdigit():
            if int(code) == s_age: 
                print(f"    MATCH: Exact age {code}. Val: {r[val_col]}")
                return r[val_col]
                
    print("  DEBUG: No category match found.")
    return None

def debug(swimmer_id, event_name, input_age=None):
    conn = get_connection()
    
    # 1. Get Swimmer
    swimmers = pd.read_sql("SELECT * FROM swimmers WHERE id=?", conn, params=(swimmer_id,))
    if swimmers.empty:
        print("Swimmer not found.")
        return
        
    s = swimmers.iloc[0]
    s_gen = s['gender']
    s_dob = s['birth_date']
    
    # Calc Age
    if input_age:
        age = input_age
    else:
        # Simple Age Calc
        try:
            bd = pd.to_datetime(s_dob)
            age = datetime.now().year - bd.year
        except:
            age = 15 # Default
            
    print(f"Swimmer: {s['name']} | Gender: {s_gen} | Age: {age}")
    
    # 2. Load Records
    records_df = pd.read_sql(f"SELECT * FROM national_records WHERE event_name='{event_name}'", conn)
    print(f"Loaded {len(records_df)} records for {event_name}")
    
    # 3. Test Match
    # Try 50m
    print("\n--- Testing 50m Match ---")
    val = find_match_val(records_df, "50m", s_gen, age, 'time')
    print(f"Result: {val}")
    
    conn.close()

if __name__ == "__main__":
    # Param: SwimmerID, Event
    # Look for a swimmer in DB first
    conn = get_connection()
    s_id = conn.execute("SELECT id FROM swimmers LIMIT 1").fetchone()[0]
    conn.close()
    
    # Try "100 Breast"
    debug(s_id, "100 Breast")
