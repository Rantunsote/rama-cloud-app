import sqlite3
import pandas as pd
import argparse
import sys
import os

import shutil

# Path to the live iOS App Container (varies by system, but this is the one we found)
LIVE_DB_PATH = '/Users/jrb/Library/Containers/7F2BC93B-8FAC-48B0-BF83-D128B1ADF11C/Data/Documents/MeetMobile.db'
DB_PATH = 'meet_mobile_dump.db'

def refresh_db():
    """Copies the latest DB from the app container to our working directory."""
    try:
        if os.path.exists(LIVE_DB_PATH):
            shutil.copy2(LIVE_DB_PATH, DB_PATH)
            # print("Synced with latest Meet Mobile data.")
        else:
            print(f"Warning: Live DB not found at {LIVE_DB_PATH}. Using cached copy.")
    except Exception as e:
        print(f"Warning: Could not sync live DB ({e}). Using cached copy.")

def get_connection():
    refresh_db() # Sync before connecting
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return None
    return sqlite3.connect(DB_PATH)

def search_swimmer(name_part):
    conn = get_connection()
    if not conn: return
    
    query = """
    SELECT id, uniqueId, firstName, lastName, age, teamId 
    FROM Swimmer 
    WHERE firstName LIKE ? OR lastName LIKE ?
    """
    
    term = f"%{name_part}%"
    df = pd.read_sql(query, conn, params=(term, term))
    conn.close()
    
    if df.empty:
        print("No swimmers found.")
    else:
        print(df.to_string(index=False))
        
def get_swimmer_details(swimmer_id):
    conn = get_connection()
    if not conn: return
    
    # 1. Swimmer Info
    s_query = "SELECT * FROM Swimmer WHERE id = ?"
    swimmer = pd.read_sql(s_query, conn, params=(swimmer_id,))
    
    if swimmer.empty:
        print(f"No swimmer found with ID {swimmer_id}")
        conn.close()
        return

    print("\n=== Swimmer Profile ===")
    print(swimmer[['id', 'uniqueId', 'firstName', 'lastName', 'age', 'teamName']].to_string(index=False))
    
    # 2. Results
    r_query = """
    SELECT 
        e.name as Event,
        he.timeInSecs as Time,
        he.overallPlace as Place,
        m.name as Meet
    FROM SwimmerHeatEntry she
    JOIN HeatEntry he ON she.heatEntryId = he.id
    JOIN Heat h ON he.heatId = h.id
    JOIN Round r ON h.roundId = r.id
    JOIN Event e ON r.eventId = e.id
    JOIN Meet m ON e.meetId = m.id
    WHERE she.swimmerId = ?
    ORDER BY m.startDate DESC
    """
    
    results = pd.read_sql(r_query, conn, params=(swimmer_id,))
    conn.close()
    
    print("\n=== Results ===")
    if results.empty:
        print("No results found.")
    else:
        print(results.to_string(index=False))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("  Search name: python3 meet_mobile_query.py search <name>")
        print("  Get ID info: python3 meet_mobile_query.py get <id>")
        sys.exit(1)
        
    cmd = sys.argv[1]
    arg = sys.argv[2]
    
    if cmd == "search":
        search_swimmer(arg)
    elif cmd == "get":
        get_swimmer_details(arg)
