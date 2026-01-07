
import re

def normalize_event_name_v2(raw_name):
    clean = raw_name.strip()
    
    # 1. Remove units
    clean = re.sub(r'\b(Metro|Meter|Metros|Meters)\b', '', clean, flags=re.IGNORECASE)
    
    # 2. Remove Gender prefixes
    prefixes = ['Hombres', 'Mujeres', 'Men', 'Women', 'Mixto', 'Niños', 'Niñas', 'Boys', 'Girls', 'Mixed', 'Ninas', 'Ninos']
    for p in prefixes:
        clean = re.sub(r'^' + p + r'\s*', '', clean, flags=re.IGNORECASE)
        
    clean = clean.strip()
    
    # 3. Remove "Open", "Todo Competidor"
    clean = re.sub(r'^(Open|Todo Competidor|Absoluto)\s*', '', clean, flags=re.IGNORECASE)

    # 4. Remove Age Groups - CAREFUL!
    # Valid Age Patterns:
    # - Range: 11-12, 9-10
    # - Over/Under: 11&O, 10&U, 18&O
    # - "11-12 años"
    
    # Remove explicit age strings
    clean = re.sub(r'\b\d{1,2}-\d{1,2}\s*(años|years)?\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b\d{1,2}[&][OUou]\b', '', clean)
    clean = re.sub(r'\b\d{1,2}\s+(años|years)\b', '', clean, flags=re.IGNORECASE)
    
    clean = clean.strip()
    
    # 5. Handle single number Age vs Distance
    # "10 50 Free" -> 10 is Age, 50 is Dist
    # "50 Free" -> 50 is Dist
    # "10 Free" -> 10 is Dist? No, 10m Free doesn't exist.
    # Distances: 25, 50, 100, 200, 400, 800, 1500, 4x50, 4x100
    
    # Regex for "Number followed by Number" (Age Distance)
    match_two = re.match(r'^(\d+)\s+(\d+)\s+(.*)', clean)
    if match_two:
        # e.g. "10 50 Free"
        # Remove first number (Age)
        clean = f"{match_two.group(2)} {match_two.group(3)}"
    else:
        # Check if starts with number not in valid distances?
        # A bit risky. 
        # Better: If starts with single number, is it a valid swimming distance?
        valid_dists = [25, 50, 100, 200, 400, 800, 1500]
        match_one = re.match(r'^(\d+)\s+(.*)', clean)
        if match_one:
            val = int(match_one.group(1))
            if val not in valid_dists:
                # Likely an age (e.g. "9 Free" -> 9 year old free?)
                # Actually "9 Free" is usually "Member of 9yo category swimming Free"?
                # But assume if not valid dist, strip it.
                # Exception: 4x50
                pass 
                # clean = match_one.group(2) # Strip if not valid distance?
                # Let's say yes for now, but be careful of 4x...
    
    # Translation
    translations = {
        'Libre': 'Free', 'Pecho': 'Breast', 'Espalda': 'Back',
        'Mariposa': 'Fly', 'Combinado': 'IM', 'CI': 'IM',
        'Relevo': 'Relay', 'Medley': 'Medley'
    }
    
    words = clean.split()
    new_words = []
    for w in words:
        cap = w.capitalize()
        if cap in translations:
            new_words.append(translations[cap])
        elif w.upper() == "CI":
             new_words.append("IM")
        else:
            new_words.append(w)
            
    clean = " ".join(new_words)
    if "IM Relay" in clean: clean = clean.replace("IM Relay", "Medley Relay")
    
    # Strip extra spaces
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

# Test Cases
tests = [
    ("Hombres 11&O 200 Metro Pecho", "200 Breast"),
    ("Mujeres 9-10 50 Metro Libre", "50 Free"),
    ("100 Free", "100 Free"),
    ("10 50 Back", "50 Back"),
    ("Ninas 10 100 Back", "100 Back"),
    ("Hombres 8 25 Metro Mariposa", "25 Fly"),
    ("200 IM", "200 IM"),
    ("Open 400 Free", "400 Free"),
    ("11-12 años 50 Espalda", "50 Back")
]

for orig, expected in tests:
    res = normalize_event_name_v2(orig)
    status = "✅" if res == expected else f"❌ Got '{res}'"
    print(f"'{orig}' -> {status}")
