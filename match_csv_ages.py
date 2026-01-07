
import pandas as pd
import sqlite3
from datetime import datetime
from rapidfuzz import process, fuzz

DB_PATH = "data/natacion.db"
CSV_PATH = "Reporte_CRNP_2025.csv"

def run():
    # 1. Load DB Swimmers with missing DOB
    conn = sqlite3.connect(DB_PATH)
    # Get ID, Name
    db_df = pd.read_sql("SELECT id, name FROM swimmers WHERE birth_date IS NULL OR birth_date = ''", conn)
    
    if db_df.empty:
        print("No swimmers with missing DOB found in DB.")
        return

    # 2. Load CSV
    try:
        csv_df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
        
    # Clean CSV names
    # Assuming column 'Swimmer' contains names
    if 'Swimmer' not in csv_df.columns or 'Age' not in csv_df.columns:
        print("CSV missing 'Swimmer' or 'Age' columns.")
        return

    # Unique swimmers in CSV with Age
    csv_swimmers = csv_df[['Swimmer', 'Age']].drop_duplicates().dropna()
    
    updates = []
    
    db_names = db_df['name'].tolist()
    
    print(f"Checking {len(db_df)} DB swimmers against {len(csv_swimmers)} CSV entries...")
    
    for _, row in csv_swimmers.iterrows():
        csv_name = row['Swimmer']
        age = int(row['Age'])
        if age <= 0: continue
        
        # Approximate Year
        # Report is 2025, so DOB Year = 2025 - Age
        birth_year = 2025 - age
        est_dob = f"{birth_year}-01-01"
        
        # Match with DB
        # 1. Exact Match
        exact = db_df[db_df['name'].str.lower() == csv_name.lower()]
        if not exact.empty:
            s_id = exact.iloc[0]['id']
            updates.append((s_id, csv_name, age, est_dob))
        else:
            # 2. Fuzzy Match
            # Only match if high confidence, as names should be somewhat consistent
            match, score, idx = process.extractOne(csv_name, db_names, scorer=fuzz.token_sort_ratio)
            if score >= 85:
                # Find ID
                s_id = db_df[db_df['name'] == match].iloc[0]['id']
                updates.append((s_id, match, age, est_dob))
                
    # Dedup updates (csv might have dupes)
    unique_updates = {}
    for uid, name, age, dob in updates:
        unique_updates[uid] = (name, age, dob)
        
    print(f"Found {len(unique_updates)} matches to update.")
    for uid, (name, age, dob) in unique_updates.items():
        print(f"  - {name} (Ag: {age}) -> Est. DOB: {dob}")
        
    # Perform Update
    if unique_updates:
        # q = input("Proceed with update? (y/n): ")
        # Auto-run for this task
        print("Updating database...")
        cursor = conn.cursor()
        count = 0
        for uid, (name, age, dob) in unique_updates.items():
            cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (dob, uid))
            count += 1
        conn.commit()
        print(f"Updated {count} records in DB.")
        
    conn.close()

if __name__ == "__main__":
    run()
