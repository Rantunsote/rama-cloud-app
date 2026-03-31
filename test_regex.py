import re

test_lines = [
    "4 Davila, Bryan 15 Rama de Natacion Penalolen 2:00,46 1:58,81 5",
    "2 Davila, Bryan 15 Rama de Natacion Penalolen 2:01,50 2:00,46 q",
    "--- Alquinta, Amalia 9 Rama de Natacion Penalolen NT DQ",
    "14 Echeverria, Josefina Amanda 9 Nunoa Natacion NT 2:30,77",
    "--- Perez, Juan 15 CRNP 1:20,00 NS",
    "35 Hernandez, Pedro 12 Peñalolen 2:00,00 1:55,00",
]

teams = ["penalolen", "peñalolen", "peñalolén", "rama de natacion penalolen", "rama natacion penalolen", "crnp"]
teams.sort(key=len, reverse=True)

for line in test_lines:
    found_team = None
    for t in teams:
        idx = line.lower().find(t)
        if idx != -1:
            found_team = line[idx:idx+len(t)]
            break
            
    if found_team:
        parts = line.split(found_team)
        prefix = parts[0].strip()
        suffix = parts[1].strip()
        
        # Rank Name Age
        # Rank can be digits, ---, or empty sometimes.
        # Let's try: (Rank) (Name) (Age)
        m_prefix = re.match(r'^([\w\-]*)\s+(.+?)\s+(\d+)$', prefix)
        
        if m_prefix:
            rank = m_prefix.group(1)
            name = m_prefix.group(2)
            age = m_prefix.group(3)
        else:
            rank, name, age = "err", "err", "err"
            
        # Suffix handling
        s_tokens = suffix.split()
        seed_time, finals_time = "err", "err"
        if len(s_tokens) >= 2:
            seed_time = s_tokens[0]
            finals_time = s_tokens[1]
        elif len(s_tokens) == 1:
            seed_time = "NT" 
            finals_time = s_tokens[0]
            
        print(f"[{rank}] Name: '{name}', Age: {age}, Team: '{found_team}', Seed: {seed_time}, Finals: {finals_time}")

