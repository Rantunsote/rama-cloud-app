import sqlite3
import pandas as pd
import os

DB_PATH = 'meet_mobile_dump.db'

def get_connection():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        return None
    return sqlite3.connect(DB_PATH)

def extract_swimmers():
    conn = get_connection()
    if not conn: return
    
    # Target Team ID: 61919108 (Found from exploration)
    query = """
    SELECT 
        s.id as swimmer_id,
        s.firstName,
        s.lastName,
        s.age,
        s.gender,
        t.name as team_name,
        m.name as meet_name
    FROM Swimmer s
    JOIN Team t ON s.teamId = t.id
    JOIN Meet m ON s.meetId = m.id
    WHERE s.teamId = 61919108
    """
    
    df = pd.read_sql(query, conn)
    print(f"Extracted {len(df)} swimmers.")
    df.to_csv("meet_mobile_swimmers.csv", index=False)
    conn.close()

def extract_results():
    conn = get_connection()
    if not conn: return
    
    query = """
    SELECT 
        s.firstName || ' ' || s.lastName as swimmer_name,
        s.age,
        s.gender,
        e.name as event_name,
        e.ageGroup,
        he.timeInSecs as result_time,
        he.seedTimeInSecs as seed_time,
        he.overallPlace as place,
        m.name as meet_name
    FROM Swimmer s
    JOIN SwimmerHeatEntry she ON s.id = she.swimmerId
    JOIN HeatEntry he ON she.heatEntryId = he.id
    JOIN Heat h ON he.heatId = h.id
    JOIN Round r ON h.roundId = r.id
    JOIN Event e ON r.eventId = e.id
    JOIN Meet m ON s.meetId = m.id
    WHERE s.teamId = 61919108
    """
    
    df = pd.read_sql(query, conn)
    print(f"Extracted {len(df)} results.")
    df.to_csv("meet_mobile_results.csv", index=False)
    conn.close()

if __name__ == "__main__":
    extract_swimmers()
    extract_results()
