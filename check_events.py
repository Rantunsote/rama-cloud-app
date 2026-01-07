import sqlite3
import pandas as pd

conn = sqlite3.connect('/app/data/natacion.db')
query = "SELECT DISTINCT event_name FROM results ORDER BY event_name"
df = pd.read_sql_query(query, conn)

print(f"Total distinct events: {len(df)}")
print("Sample of events:")
print(df['event_name'].tolist())

# Check for potential un-normalized ones (containing 'Men', 'Women', 'Boys', 'Girls')
suspects = df[df['event_name'].str.contains(r'Men|Women|Boys|Girls|Ninos|Ninas|Mixto', case=False, regex=True)]
if not suspects.empty:
    print("\n POTENTIAL UN-NORMALIZED EVENTS:")
    print(suspects['event_name'].tolist())
else:
    print("\n No obvious un-normalized events found.")
