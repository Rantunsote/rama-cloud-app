
from datetime import datetime

def calculate_category(dob_str):
    if not dob_str: return "Desconocida"
    try:
        d_str = str(dob_str).replace('/', '-')
        parts = d_str.split('-')
        birth_year = None
        if len(parts[0]) == 4:
            birth_year = int(parts[0]) 
        elif len(parts[-1]) == 4:
            birth_year = int(parts[-1]) 
            
        if birth_year is None: return "Formato Inválido"

        current_year = datetime.now().year
        age = current_year - birth_year
        
        if age <= 14: return f"{age} años"
        if age <= 17: return "15-17 años"
        return "18-99 años"
    except:
        return "Fecha Inválida"

# Test Cases (Assuming 2026 as current year context from system prompt, but datetime.now() uses machine time which is 2026 per context)
# Machine time says 2026.
# 10 years old (2016)
print(f"2016 -> {calculate_category('2016-01-01')}")
# 14 years old (2012)
print(f"2012 -> {calculate_category('2012-01-01')}")
# 15 years old (2011)
print(f"2011 -> {calculate_category('2011-01-01')}")
# 17 years old (2009)
print(f"2009 -> {calculate_category('2009-01-01')}")
# 18 years old (2008)
print(f"2008 -> {calculate_category('2008-01-01')}")
# 25 years old (2001)
print(f"2001 -> {calculate_category('2001-01-01')}")
