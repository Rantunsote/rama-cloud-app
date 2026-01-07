import pandas as pd
import os

# Adjust path if find command returned something different, but assuming standard Downloads
# The previous command output wasn't read yet, but I'll assume standard path or read from output if available.
# Actually I should wait for find command output. But for speed I'll try the likely path.
file_path = "/Users/jrb/Downloads/Nadadores.xlsx"

if not os.path.exists(file_path):
    print(f"File not found at {file_path}")
    # Fallback to search result if I could see it, but I can't yet.
else:
    try:
        df = pd.read_excel(file_path)
        print("Columns:", df.columns.tolist())
        print("First 5 rows:")
        print(df.head().to_string())
    except Exception as e:
        print(f"Error reading excel: {e}")
