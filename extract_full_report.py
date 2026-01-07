import sqlite3
import pandas as pd
import os
import shutil
import time

# Constants
LIVE_DB_PATH = '/Users/jrb/Library/Containers/7F2BC93B-8FAC-48B0-BF83-D128B1ADF11C/Data/Documents/MeetMobile.db'
DB_PATH = 'meet_mobile_dump.db'
JAN_1_2025_TIMESTAMP = 1735689600

def refresh_db():
    try:
        if os.path.exists(LIVE_DB_PATH):
            shutil.copy2(LIVE_DB_PATH, DB_PATH)
            print("Synced with live Meet Mobile DB.")
        else:
            print("Live DB not found, using cached.")
    except Exception as e:
        print(f"Sync error: {e}")

def get_connection():
    refresh_db()
    return sqlite3.connect(DB_PATH)

def extract_report():
    conn = get_connection()
    if not conn: return
    
    # 1. Find relevant Team IDs
    print("Finding Teams...")
    team_query = """
    SELECT id, name FROM Team 
    WHERE name LIKE '%Penalolen%' 
       OR name LIKE '%Peñalolen%' 
       OR name LIKE '%CRNP%'
       OR name LIKE '%Rama%'
    """
    teams = pd.read_sql(team_query, conn)
    if teams.empty:
        print("No matching teams found.")
        return
    
    team_ids = tuple(teams['id'].tolist())
    print(f"Found {len(teams)} matching teams.")
    # print(teams[['id', 'name']])

    # 2. Main Query
    print("Extracting 2025 Results...")
    
    # Timestamp filter for Meets
    # Join path: Swimmer -> SwimmerHeatEntry -> HeatEntry -> Heat -> Round -> Event -> Meet
    # OR simpler: Swimmer -> Meet (But Swimmer.meetId might be inconsistent with Event.meetId?)
    # Usually Swimmer.meetId is reliable for the meet they are registered in.
    # Results come from HeatEntry.
    
    query = f"""
    SELECT 
        s.firstName || ' ' || s.lastName as Swimmer,
        s.age as Age,
        s.gender as Gender,
        c.name as Category,
        t.name as Team,
        e.name as Event,
        he.timeInSecs as Time,
        he.overallPlace as Place,
        m.name as Meet,
        date(m.startDateUtc, 'unixepoch') as Date
    FROM Swimmer s
    JOIN Team t ON s.teamId = t.id
    JOIN Meet m ON s.meetId = m.id
    JOIN SwimmerHeatEntry she ON s.id = she.swimmerId
    JOIN HeatEntry he ON she.heatEntryId = he.id
    LEFT JOIN Category c ON he.categoryId = c.id
    JOIN Heat h ON he.heatId = h.id
    JOIN Round r ON h.roundId = r.id
    JOIN Event e ON r.eventId = e.id
    WHERE s.teamId IN {team_ids}
      AND m.startDateUtc >= {JAN_1_2025_TIMESTAMP}
    ORDER BY m.startDateUtc DESC, s.lastName ASC
    """
    
    # Fix single element tuple comma issue if only 1 team found
    if len(team_ids) == 1:
        query = query.replace(f"IN {team_ids}", f"IN ({team_ids[0]})")
    
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No results found for 2025.")
    else:
        filename = "Reporte_CRNP_2025.csv"
        df.to_csv(filename, index=False)
        print(f"Success! Exported {len(df)} rows to '{filename}'")
        print(df.head())

    conn.close()

if __name__ == "__main__":
    extract_report()
