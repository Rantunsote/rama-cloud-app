import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import textwrap
from datetime import datetime
import time
import sys

# Configuration
# Configuration
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up two levels to reach project root (swim_scraper/frontend -> swim_scraper -> root), then data/
# Actually structure seems to be: ~/Documents/RAMA/swim_scraper/frontend/app.py
# And DB is at ~/Documents/RAMA/swim_scraper/data/natacion.db (based on update_dobs.py location)
# So we need to go up from frontend to swim_scraper, then into data.
ROOT_DIR = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(ROOT_DIR, "data", "natacion.db")
TEAM_ID = "10034725" # Rama Peñalolén

st.set_page_config(page_title="RamaCloud", page_icon="🏊", layout="wide")

import base64

def html_block(s):
    # Strip all leading indentation to avoid Markdown code blocks.
    return "\n".join([line.lstrip() for line in textwrap.dedent(s).splitlines()])

EVENT_DB_TO_ES = {
    "50 Free": "50 metros Libre",
    "100 Free": "100 metros Libre",
    "200 Free": "200 metros Libre",
    "400 Free": "400 metros Libre",
    "800 Free": "800 metros Libre",
    "1500 Free": "1500 metros Libre",
    "50 Back": "50 metros Espalda",
    "100 Back": "100 metros Espalda",
    "200 Back": "200 metros Espalda",
    "50 Breast": "50 metros Pecho",
    "100 Breast": "100 metros Pecho",
    "200 Breast": "200 metros Pecho",
    "50 Fly": "50 metros Mariposa",
    "100 Fly": "100 metros Mariposa",
    "200 Fly": "200 metros Mariposa",
    "100 IM": "100 metros Combinado (Solo en piscina corta 25m)",
    "200 IM": "200 metros Combinado",
    "400 IM": "400 metros Combinado",
    "200 Free Relay": "4 x 50 metros Libre",
    "400 Free Relay": "4 x 100 metros Libre",
    "800 Free Relay": "4 x 200 metros Libre",
    "200 Medley Relay": "4 x 50 metros Combinado",
    "400 Medley Relay": "4 x 100 metros Combinado",
    "400 Mixed Free Relay": "4 x 100 metros Mixto (Libre o Combinado)",
    "400 Mixed Medley Relay": "4 x 100 metros Mixto (Libre o Combinado)",
}

EVENT_ES_TO_DB = {
    "50 metros Libre": "50 Free",
    "100 metros Libre": "100 Free",
    "200 metros Libre": "200 Free",
    "400 metros Libre": "400 Free",
    "800 metros Libre": "800 Free",
    "1500 metros Libre": "1500 Free",
    "50 metros Espalda": "50 Back",
    "100 metros Espalda": "100 Back",
    "200 metros Espalda": "200 Back",
    "50 metros Pecho": "50 Breast",
    "100 metros Pecho": "100 Breast",
    "200 metros Pecho": "200 Breast",
    "50 metros Mariposa": "50 Fly",
    "100 metros Mariposa": "100 Fly",
    "200 metros Mariposa": "200 Fly",
    "100 metros Combinado (Solo en piscina corta 25m)": "100 IM",
    "200 metros Combinado": "200 IM",
    "400 metros Combinado": "400 IM",
    "4 x 50 metros Libre": "200 Free Relay",
    "4 x 100 metros Libre": "400 Free Relay",
    "4 x 200 metros Libre": "800 Free Relay",
    "4 x 50 metros Combinado": "200 Medley Relay",
    "4 x 100 metros Combinado": "400 Medley Relay",
    "4 x 100 metros Mixto (Libre o Combinado)": "400 Mixed Free Relay",
}

EVENT_ES_TO_DB_MULTI = {
    "4 x 100 metros Mixto (Libre o Combinado)": ["400 Mixed Free Relay", "400 Mixed Medley Relay"],
}

def get_event_display_name(event_name):
    if not isinstance(event_name, str):
        return event_name
    return EVENT_DB_TO_ES.get(event_name.strip(), event_name)

def normalize_scraped_event_name(raw_name):
    """
    Normalizes 'Mujeres 9-10 400 Metro Libre' -> '400 Free'
    Handles Relays to avoid false positives (e.g. '200 Relevo' != '200 Individual')
    """
    if not isinstance(raw_name, str):
        return raw_name
        
    name = raw_name.lower()
    
    # Check for Relay
    is_relay = "relevo" in name or "relay" in name
    
    # 1. Extract Distance
    import re
    # Match number at start of string or after space
    # Look for 4x50, 4x100 patterns for Relays
    if is_relay:
        # Simple logic: if it's a relay, we append "Relay" to style
        # But we must capture the distance logic if possible, or usually Relays are 200 (4x50) or 400 (4x100) or 800 (4x200)
        # For qualification purpose, if we return "200 Free Relay", it won't match "200 Free" standard.
        pass
    
    dist_match = re.search(r'(\b|^)(25|50|100|200|400|800|1500)(\b|$)', name)
    if not dist_match:
        # Maybe it's "4 x 50"
        if "4 x 50" in name or "4x50" in name: distance = "200"
        elif "4 x 100" in name or "4x100" in name: distance = "400"
        else: return raw_name
    else:
        distance = dist_match.group(2)
    
    # 2. Extract Style
    style = None
    if "libre" in name or "free" in name:
        style = "Free"
    elif "espalda" in name or "back" in name:
        style = "Back"
    elif "pecho" in name or "breast" in name:
        style = "Breast"
    elif "mariposa" in name or "fly" in name:
        style = "Fly"
    elif "combinado" in name or "medley" in name or "ci" in name or "im" in name:
        # "CI" = Combinado Individual usually, but sometimes used in Relays "Combinado Relevo"
        style = "IM"
        # If it's a relay, "IM Relay" is usually called "Medley Relay" in DB keys
        if is_relay: style = "Medley"
        
    if not style:
        return raw_name
        
    # 3. Construct DB Name
    final_name = f"{distance} {style}"
    
    if is_relay:
        final_name += " Relay"
        
    return final_name

def resolve_db_event_names(display_name):
    if not isinstance(display_name, str):
        return [display_name]
    if display_name in EVENT_ES_TO_DB_MULTI:
        return EVENT_ES_TO_DB_MULTI[display_name]
        
    # Try exact match first
    mapped = EVENT_ES_TO_DB.get(display_name)
    if mapped:
        return [mapped]
        
    # Try fallback normalization
    normalized = normalize_scraped_event_name(display_name)
    return [normalized]

def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


# Display Logo (Temporarily Removed)
# if os.path.exists("logo_rama_v15.png"):
#    st.logo("logo_rama_v15.png", size="large")

# Custom CSS for "App-like" feel
st.markdown("""
<style>
    html, body, [data-testid="stApp"] {
        background-color: #ffffff !important;
        color: #0f172a !important;
    }
    p, h1, h2, h3, h4, h5, span, div {
        color: #0f172a !important;
    }
    .stMarkdown, .stText, .stMetricValue, .stMetricLabel {
        color: #0f172a !important;
    }
    /* Fix for Selectbox Dropdowns (force light theme on popups) */
    div[data-baseweb="popover"], div[data-baseweb="menu"], div[role="listbox"], ul[data-testid="stSelectboxVirtualDropdown"] {
        background-color: #ffffff !important;
    }
    /* Ensure options text is visible */
    li[role="option"] {
        background-color: #ffffff !important;
    }
    li[role="option"]:hover {
        background-color: #f1f5f9 !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    div[data-testid="stTextInput"] input {
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"Database not found at {DB_PATH}. Please check volume mapping.")
        return None
    return sqlite3.connect(DB_PATH)

def load_national_records():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM national_records", conn)
    conn.close()
    return df

def load_minimas():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM minimum_standards", conn)
    conn.close()
    return df

def get_record_generic(df, event_name, pool_size, gender, category_code):
    mapping = {
        "Free": "Libre",
        "Back": "Espalda",
        "Breast": "Pecho",
        "Fly": "Mariposa",
        "IM": "Combinado",
        "Medley": "Combinado"
    }
    
    # 1. Translate to Spanish Keywords
    trans_event = event_name
    for k, v in mapping.items():
        if k.lower() in trans_event.lower():
            trans_event = trans_event.lower().replace(k.lower(), v).title()
            
    # 2. Normalize Function (Remove 'm', extra spaces, lower)
    def normalize(s):
        return s.lower().replace('m ', ' ').replace('m', '').replace('  ', ' ').strip()
        
    norm_target = normalize(trans_event)
             
    # Filter DF first by metadata
    subset = df[
        (df['pool_size'] == pool_size) & 
        (df['gender'] == gender) &
        (df['category_code'] == category_code)
    ]
    
    # Fuzzy Match
    for _, row in subset.iterrows():
        db_evt = normalize(row['event_name'])
        # Check if "100 espalda" is in "100 espalda"
        if db_evt == norm_target:
             return row
        # Fallback: "100 libre" in "100m libre"
        if db_evt in norm_target or norm_target in db_evt:
             return row
            
    return None

def update_swimmer_info(updates_dob, updates_gender):
    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    try:
        # Update DOB
        for swift_id, new_dob in updates_dob.items():
            safe_id = int(str(swift_id).replace(',', ''))
            
            cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (new_dob, safe_id))
            
            if cursor.rowcount > 0: count += 1
            
        # Update Gender
        for swift_id, new_gender in updates_gender.items():
             safe_id = int(str(swift_id).replace(',', ''))
             cursor.execute("UPDATE swimmers SET gender = ? WHERE id = ?", (new_gender, safe_id))
             if cursor.rowcount > 0: count += 1
            
        conn.commit()
    except Exception as e:
        st.error(f"Error updating info: {e}")
        sys.stdout.write(f"❌ ERROR: {e}\n")
        sys.stdout.flush()
    finally:
        conn.close()
    return count

def load_swimmers():
    # DEBUG
    print(f"DEBUG: Connecting to {DB_PATH}")
    if os.path.exists(DB_PATH):
        print(f"DEBUG: File exists. Size: {os.path.getsize(DB_PATH)}")
    else:
        print("DEBUG: File DOES NOT exist.")

    conn = get_connection()
    if not conn: 
        print("DEBUG: Connection failed.")
        return pd.DataFrame(columns=['id', 'name', 'birth_date', 'gender', 'url', 'team_id'])
    try:
        df = pd.read_sql("SELECT * FROM swimmers WHERE team_id = ? ORDER BY name", conn, params=(TEAM_ID,))
        print(f"DEBUG: Loaded {len(df)} swimmers.")
    except:
        df = pd.read_sql("SELECT id, name, url, team_id, NULL as birth_date, NULL as gender FROM swimmers WHERE team_id = ? ORDER BY name", conn, params=(TEAM_ID,))
    conn.close()
    
    # robust_date_parsing: Waterfall strategy
    # 1. Try ISO (YYYY-MM-DD) - Standard DB format
    d_iso = pd.to_datetime(df['birth_date'], format='%Y-%m-%d', errors='coerce')
    
    # 2. Try Day-First (DD-MM-YYYY) - User input or scraped format
    d_dmy = pd.to_datetime(df['birth_date'], dayfirst=True, errors='coerce')
    
    # 3. Combine: Prioritize ISO, fallback to DMY
    df['birth_date'] = d_iso.fillna(d_dmy)
    
    return df

# --- CONSTANTS ---
# TEAM_ID moved to top of file

def load_results(swimmer_id):
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT r.id, r.event_name, r.time, r.pool_size, r.points, r.place, m.date, m.name as meet_name
        FROM results r
        JOIN meets m ON r.meet_id = m.id
        WHERE r.swimmer_id = ?
        ORDER BY m.date DESC
    """
    df = pd.read_sql(query, conn, params=(swimmer_id,))
    conn.close()
    return df

def load_all_best_times():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT 
            s.id as swimmer_id, s.name, s.birth_date, s.gender,
            r.event_name, r.time, r.pool_size, r.points,
            m.date
        FROM results r
        JOIN swimmers s ON r.swimmer_id = s.id
        JOIN meets m ON r.meet_id = m.id
        WHERE s.team_id = ?
    """
    df = pd.read_sql(query, conn, params=(TEAM_ID,))
    conn.close()
    
    if df.empty: return df

    # Parse dates and categs
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    current_year = datetime.now().year
    
    def get_simple_cat(dob):
        if pd.isnull(dob): return "Desconocida"
        try:
            today = datetime.now()
            # Strict actual age calculation
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            
            if age <= 14: return f"{age} años"
            if age <= 17: return "15-17 años"
            return "18-99 años"
        except: return "Desconocida"

    df['category'] = df['birth_date'].apply(get_simple_cat)
    df['seconds'] = df['time'].apply(parse_time)
    df = df.dropna(subset=['seconds'])
    
    # Best Time Per Swimmer per Event per Pool
    best_times = df.loc[df.groupby(['name', 'event_name', 'pool_size'])['seconds'].idxmin()]
    
    return best_times

def load_all_results():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    query = """
        SELECT 
            s.id as swimmer_id, s.name, s.birth_date, s.gender,
            r.event_name, r.time, r.pool_size, r.points,
            m.date, m.name as meet_name
        FROM results r
        JOIN swimmers s ON r.swimmer_id = s.id
        JOIN meets m ON r.meet_id = m.id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty: return df
    df['seconds'] = df['time'].apply(parse_time)
    df = df.dropna(subset=['seconds'])
    df['event_display'] = df['event_name'].apply(get_event_display_name)
    df['date_clean'] = df['date'].apply(clean_date_str)
    df['date_obj'] = pd.to_datetime(df['date_clean'], errors='coerce')
    df['year'] = df['date_obj'].dt.year
    return df

def clean_date_str(d):
    if not isinstance(d, str): return d
    try:
        if ',' in d:
            parts = d.split(',')
            year = parts[-1].strip()
            date_part = parts[0]
            return f"{date_part}, {year}"
        return d
    except:
        return d

def compute_cagr_improvement(yearly_best):
    if yearly_best is None or len(yearly_best) < 2:
        return None
    first = yearly_best.iloc[0]
    last = yearly_best.iloc[-1]
    if first is None or last is None or first <= 0 or last <= 0:
        return None
    years = len(yearly_best) - 1
    try:
        rate = 1 - (last / first) ** (1 / years)
        return rate * 100
    except:
        return None

def compute_gap_to_pb(current_time, pb_time):
    if current_time is None or pb_time is None or pb_time <= 0:
        return None
    return ((current_time - pb_time) / pb_time) * 100

def best_time(df):
    if df is None or df.empty:
        return None
    return df['seconds'].min()

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

def extract_style(event_name):
    name = event_name.lower()
    if 'free' in name or 'libre' in name: return 'Libre'
    if 'back' in name or 'espalda' in name: return 'Espalda'
    if 'breast' in name or 'pecho' in name: return 'Pecho'
    if 'fly' in name or 'mariposa' in name: return 'Mariposa'
    if 'im' in name or 'medley' in name or 'combinado' in name: return 'Combinado'
    return 'Otro'

def format_seconds(total_seconds):
    try:
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:05.2f}"
    except:
        return f"{total_seconds:.2f}"

def calculate_category(dob_str):
    if dob_str is None or pd.isna(dob_str): return "Desconocida"
    
    current_date = datetime.now()
    birth_date = None

    try:
        # 1. If it's already a datetime/Timestamp
        if isinstance(dob_str, (datetime, pd.Timestamp)):
            birth_date = dob_str
        # 2. If it's a string, try parsing
        elif isinstance(dob_str, str):
            # Normalize
            s = dob_str.replace('/', '-')
            if ' ' in s: s = s.split()[0] # Remove time part if present
            
            try:
                birth_date = datetime.strptime(s, "%Y-%m-%d")
            except:
                # Fallback or try other formats?
                pass
        
        if not birth_date:
             return "Formato Inválido"

        # BIOLOGICAL AGE CALCULATION
        # Ensure birth_date is datetime-like for comparison
        age = current_date.year - birth_date.year - ((current_date.month, current_date.day) < (birth_date.month, birth_date.day))
        
        if age <= 10: return f"{age} años"
        if 11 <= age <= 12: return f"{age} años" 
        if age <= 14: return f"{age} años"
        if age <= 17: return "15-17 años"
        return "18-99 años"
    except Exception as e:
        # print(f"Error calculating age for {dob_str}: {e}")
        return "Fecha Inválida"

def update_swimmer_dob(updates):
    # Backward compatibility wrapper if needed, but update_swimmer_info is primary
    return update_swimmer_info(updates, {})

def update_pool_size(updates):
    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    try:
        for result_id, new_size in updates.items():
            cursor.execute("UPDATE results SET pool_size = ? WHERE id = ?", (new_size, result_id))
            count += 1
        conn.commit()
    except Exception as e:
        st.error(f"Error updating: {e}")
    finally:
        conn.close()
    return count

def update_meet_pool_size(meet_id, new_size):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # 1. Update Meet
        cursor.execute("UPDATE meets SET pool_size = ? WHERE id = ?", (new_size, meet_id))
        
        # 2. Update Results for this meet
        cursor.execute("UPDATE results SET pool_size = ? WHERE meet_id = ?", (new_size, meet_id))
        
        conn.commit()
    except Exception as e:
        st.error(f"Error updating pool size: {e}")
    finally:
        conn.close()



MONTH_MAP_ES = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sep': '09', 'sept': '09', 'oct': '10', 'nov': '11', 'dic': '12'
}

def parse_spanish_date_text(d_str):
    if not d_str or not d_str.strip(): return None
    try:
        parts = d_str.strip().split('-')
        if len(parts) != 3: return None
        day, month_txt, year_short = parts
        
        month = MONTH_MAP_ES.get(month_txt.lower())
        if not month: return None
        
        # Handle Year
        if len(year_short) == 2:
            y = int(year_short)
            if y < 30: year = 2000 + y
            else: year = 1900 + y
        else:
            year = int(year_short)
            
        return f"{year}-{month}-{str(day).zfill(2)}"
    except:
        return None

def process_bulk_dob_update(text_data):
    conn = get_connection()
    if not conn: return 0, ["No Connection"]
    cursor = conn.cursor()
    lines = text_data.strip().split('\n')
    updates = 0
    errors = []
    
    from difflib import SequenceMatcher

    try:
        # Pre-fetch swimmers to avoid nested cursor issues
        swimmers_cache = pd.read_sql("SELECT id, name FROM swimmers", conn)
        
        for line in lines:
            parts = line.split('\t')
            clean_parts = [p.strip() for p in parts if p.strip()]
            
            dob_candidate = None
            name_parts = []
            
            for p in clean_parts:
                if '-' in p and any(m in p.lower() for m in MONTH_MAP_ES.keys()):
                    dob_candidate = p
                    break
                if '@' in p or p.isdigit() or p.upper() in ['OK', 'SI', 'NO TIENE']: continue
                name_parts.append(p)
                
            if not dob_candidate or not name_parts: continue
            
            parsed_dob = parse_spanish_date_text(dob_candidate)
            if not parsed_dob: continue
            
            full_name = " ".join(name_parts)
            
            # Find best match
            best_match = None
            best_score = 0
            
            for _, row in swimmers_cache.iterrows():
                sid, sname = row['id'], row['name']
                score = SequenceMatcher(None, full_name.lower(), sname.lower()).ratio()
                if score > 0.7 and score > best_score:
                    best_score = score
                    best_match = sid
            
            if best_match and best_score > 0.8:
                cursor.execute("UPDATE swimmers SET birth_date = ? WHERE id = ?", (parsed_dob, int(best_match)))
                if cursor.rowcount > 0: updates += 1
                
        conn.commit()
    except Exception as e:
        errors.append(str(e))
    finally:
        conn.close()
    return updates, errors

# --- UI & NAVIGATION ---

# Initialize Session State
if 'selected_swimmer_id' not in st.session_state:
    st.session_state['selected_swimmer_id'] = None

def go_to_swimmer(swimmer_id):
    st.session_state['selected_swimmer_id'] = swimmer_id
    # st.rerun() # Ensure rerun happens

def go_to_home():
    st.session_state['selected_swimmer_id'] = None
    # st.rerun()

# --- VIEWS ---

def load_meets():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM meets", conn)
    conn.close()
    
    # Robust Date Parsing
    def parse_smart_date(d_str):
        if not isinstance(d_str, str): return pd.NaT
        try:
            # Clean up ranges: "Dec 5–6, 2025" -> "Dec 5, 2025"
            # Remove en-dash, em-dash, hyphen range
            clean = d_str
            for sep in ['–', '—', '-']:
                if sep in clean and ',' in clean:
                    # heuristic: split by sep, take first part, append year if year is at end
                    parts = clean.split(',')
                    year = parts[-1]
                    range_part = parts[0]
                    if sep in range_part:
                        start_day = range_part.split(sep)[0]
                        clean = f"{start_day}, {year}"
            
            return pd.to_datetime(clean, errors='coerce')
        except:
            return pd.NaT

    df['date_obj'] = df['date'].apply(parse_smart_date)
    # Sort by date_obj descending (Newest first)
    df = df.sort_values(by='date_obj', ascending=False)
    
    return df

def render_analysis_tab(swimmers_df):
    st.subheader("📊 Análisis Comparativo")
    
    # --- Filters ---
    c1, c2, c3, c4 = st.columns(4)
    
    # 1. Gender
    gender = c1.selectbox("Género", ["M", "F"], index=0)
    
    # 2. Pool (REMOVED as per request)
    # pool = c2.selectbox("Piscina", ["25m", "50m"], index=0)
    
    # 3. Category
    # Map friendly names to min-max ages
    # Map friendly names to min-max ages
    cat_map = {}
    # Individual ages 8 to 14
    for age_n in range(8, 15):
        # DB uses "11-11" for individual ages
        cat_map[f"{age_n} años"] = (age_n, age_n, f"{age_n}-{age_n}")
    
    # Groups
    cat_map["15-17 años"] = (15, 17, "15-17")
    cat_map["18-99 años"] = (18, 99, "18-99")
    
    cat_label = c3.selectbox("Categoría", list(cat_map.keys()), index=2) # Default Juvenil A
    cat_min, cat_max, cat_code = cat_map[cat_label]
    
    # --- Filter Swimmers ---
    # Calculate Age
    current_year = datetime.now().year
    
    def get_age(dob):
        if not dob: return 0
        try:
            today = datetime.now()
            return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        except: return 0
        
    # Apply Filters
    target_swimmers = []
    
    # We need to filter the dataframe
    # Normalize gender input
    g_target = gender # 'M' or 'F'
    
    for _, s in swimmers_df.iterrows():
        # Check Gender
        s_gen = str(s.get('gender', '')).upper().strip()
        # Normalize s_gen
        if s_gen in ['MALE', 'HOMBRE', 'MASCULINO']: s_gen = 'M'
        if s_gen in ['FEMALE', 'MUJER', 'FEMENINO']: s_gen = 'F'
        if s_gen.startswith('M'): s_gen = 'M'
        if s_gen.startswith('F'): s_gen = 'F'
        
        if s_gen != g_target: continue
        
        # Check Age
        age = get_age(s.get('birth_date'))
        if not (cat_min <= age <= cat_max): continue
        
        target_swimmers.append(s['id'])
        
    if not target_swimmers:
        st.warning(f"No hay nadadores en la categoría {cat_label} ({gender}).")
        return

    # --- Fetch Results ---
    # We need available events for these swimmers in this pool
    conn = get_connection()
    if not conn: return
    
    ph = ",".join(["?"] * len(target_swimmers))
    
    # Get distinct events (Ignoring Pool Size)
    q_events = f"""
        SELECT DISTINCT event_name 
        FROM results 
        WHERE swimmer_id IN ({ph})
        ORDER BY event_name ASC
    """
    params = target_swimmers
    events_df = pd.read_sql(q_events, conn, params=params)
    
    if events_df.empty:
        st.info("No hay resultados registrados para este grupo.")
        conn.close()
        return
        
    # 4. Event Selector - Using User Preferred List
    PREFERRED_EVENT_ORDER = {
        "🏊 Estilo Libre (Crol)": [
            "50 metros Libre", "100 metros Libre", "200 metros Libre", 
            "400 metros Libre", "800 metros Libre", "1500 metros Libre"
        ],
        "🏊 Estilo Espalda": [
            "50 metros Espalda", "100 metros Espalda", "200 metros Espalda"
        ],
        "🏊 Estilo Pecho (Braza)": [
            "50 metros Pecho", "100 metros Pecho", "200 metros Pecho"
        ],
        "🏊 Estilo Mariposa": [
            "50 metros Mariposa", "100 metros Mariposa", "200 metros Mariposa"
        ],
        "🏊 Estilo Combinado (Medley)": [
            "100 metros Combinado (Solo en piscina corta 25m)", "200 metros Combinado", "400 metros Combinado"
        ],
        "🏊 Relevos": [
            "4 x 50 metros Libre", "4 x 100 metros Libre", "4 x 200 metros Libre",
            "4 x 50 metros Combinado", "4 x 100 metros Combinado", "4 x 100 metros Mixto (Libre o Combinado)"
        ]
    }

    # Flatten list for selectbox
    display_list = []
    for group, items in PREFERRED_EVENT_ORDER.items():
        # Optional: Add group headers if Streamlit supported them in selectbox, but it doesn't really.
        # We will just list them.
        for item in items:
            # Filter 100 IM if not 25m? 
            # if item == "100 metros Combinado (Solo en piscina corta 25m)" and pool == "50m":
            #     continue 
            display_list.append(item)
    
    selected_event_display = c4.selectbox("Prueba", display_list)
    
    # Get DB Name
    selected_event_db = EVENT_ES_TO_DB.get(selected_event_display, selected_event_display)

    # --- Get Times ---
    q_times = f"""
        SELECT r.swimmer_id, r.time, s.name
        FROM results r
        JOIN swimmers s ON r.swimmer_id = s.id
        WHERE r.swimmer_id IN ({ph}) 
          AND r.event_name = ?
    """
    params_t = target_swimmers + [selected_event_db]
    times_df = pd.read_sql(q_times, conn, params=params_t)
    conn.close()
    
    if times_df.empty:
        st.warning("No hay tiempos.")
        return

    # Convert Time to Seconds
    times_df['seconds'] = times_df['time'].apply(parse_time)
    # Remove rows where seconds is None
    times_df = times_df.dropna(subset=['seconds'])
    
    # Keep only best time per swimmer
    times_df = times_df.sort_values('seconds').drop_duplicates('swimmer_id')
    
    # --- Get Reference Values (Minima/Record) ---
    minimas_df = load_minimas()
    records_df = load_national_records()
    
    val_minima = None
    minima_text = ""
    if not minimas_df.empty:
        # Filter (Try to find ANY matching minima, prefer 50m if usually standard, or lowest)
        m_row = minimas_df[
            (minimas_df['event_name'] == selected_event_db) &
            (minimas_df['gender'] == gender) &
            (minimas_df['category_code'] == cat_code)
        ]
        if not m_row.empty:
             val_minima = m_row['time_seconds'].min()
             # Get text for this minimum
             minima_text = m_row.loc[m_row['time_seconds'].idxmin()]['time_text']

    val_record = None
    record_text = ""
    if not records_df.empty:
         r_row = records_df[
            (records_df['event_name'] == selected_event_db) &
            (records_df['gender'] == gender) &
            (records_df['category_code'] == cat_code) 
         ]
         if r_row.empty:
             # Try fuzzy match
             potentials = records_df[
                (records_df['event_name'] == selected_event_db) &
                (records_df['gender'] == gender)
             ]
             for _, r in potentials.iterrows():
                 c_clean = r['category_code'].replace(' años', '').replace(' ', '')
                 if c_clean == cat_code:
                     r_row = pd.DataFrame([r])
                     break
                     
         if not r_row.empty:
             record_text = r_row.iloc[0]['time']
             val_record = parse_time(record_text)
             
    # --- Plot ---
    # Bar Chart
    fig = px.bar(
        times_df,
        x='name',
        y='seconds',
        text='time',
        title=f"Comparativa: {selected_event_display} - {cat_label}",
        color='seconds',
        color_continuous_scale='Blues_r'
    )
    
    fig.update_traces(textposition='auto')
    
    # Format Y axis with Time Strings INCLUDING REFERENCES
    # Calculate nice ticks
    if not times_df.empty:
        # Include reference values in range calculation
        all_vals = list(times_df['seconds'])
        if val_minima: all_vals.append(val_minima)
        if val_record: all_vals.append(val_record)

        min_sec = min(all_vals)
        max_sec = max(all_vals)
        
        # Add buffer
        min_y = max(0, min_sec * 0.9)
        max_y = max_sec * 1.1
        
        # Determine step size (e.g. 5s, 10s, 30s)
        rng = max_y - min_y
        step = 5
        if rng > 30: step = 10
        if rng > 60: step = 15
        if rng > 120: step = 30
        
        tick_vals = []
        curr = 0
        while curr < max_y * 1.2:
            if curr >= min_y * 0.8:
                tick_vals.append(curr)
            curr += step
            
        tick_text = [format_seconds(t) for t in tick_vals]
    else:
        tick_vals = []
        tick_text = []

    fig.update_layout(
        yaxis=dict(
            title="Tiempo",
            tickmode='array',
            tickvals=tick_vals,
            ticktext=tick_text,
            range=[max(0, min_sec - (rng*0.2)), max_y] if not times_df.empty else None
        )
    )
    
    # Add Reference Lines (Minima/Record)
    if val_minima:
        fig.add_hline(y=val_minima, line_dash="dash", line_color="orange", 
                      annotation_text=f"Minima ({minima_text})", annotation_position="top right")
                              
    if val_record:
         fig.add_hline(y=val_record, line_dash="dot", line_color="gold", 
                       annotation_text=f"Record ({record_text})", annotation_position="top left")

    st.plotly_chart(fig, use_container_width=True)
    

def get_category_code_for_minima(age):
    if age < 10: return None
    if age <= 14: return f"{age}-{age}"
    if 15 <= age <= 17: return "15-17"
    if age >= 18: return "18-99"
    return None

def render_qualifiers_tab(swimmers_df):
    st.subheader("🏅 Clasificados al Nacional")
    st.markdown("Lista de nadadores que han logrado marcas mínimas oficiales.")
    
    # 1. Load All Data
    with st.spinner("Analizando tiempos..."):
        all_results = load_all_best_times()
        minimas_df = load_minimas()
        
    if all_results.empty or minimas_df.empty:
        st.warning("No hay suficientes datos para calcular clasificados.")
        return

    # 2. Process Qualifiers
    qualifiers = []
    
    # Pre-process minimas for fast lookup: (event_name, gender, category_code, pool_size) -> seconds
    # Note: We should normalize event names. For now rely on consistent DB names (e.g. "50 Free", "100 Back")
    # But `all_results` has `event_name` from `results` table (Usually Spanish "50 metros Libre" etc or English?)
    # CHECK: `load_all_best_times` joins `results` table. `results.event_name` is typically what was scraped/imported.
    # Earlier we saw "400 metros Libre" in basic display, but "400 Free" in minima DB.
    # We must MAP result event names to minima event names.
    
    # Create lookup dict for minimas
    # Key: (event_db_name, gender, category_code, pool_size) -> time_seconds
    minima_lookup = {}
    for _, row in minimas_df.iterrows():
        k = (row['event_name'], row['gender'], row['category_code'], row['pool_size'])
        minima_lookup[k] = row['time_seconds']
        
    # Helper to calculate age
    current_year = datetime.now().year
    
    # Group results by swimmer to avoid re-calcing age too much
    # But we need to iterate every result? No, best time per event per swimmer.
    
    # Filter best times per swimmer/event/pool
    # Sort by time asc, drop duplicates
    best_times = all_results.sort_values('seconds').drop_duplicates(subset=['swimmer_id', 'event_name', 'pool_size'])
    
    total_checks = 0
    
    for _, row in best_times.iterrows():
        # Get Swimmer Info
        s_name = row['name']
        dob = row['birth_date']
        gender = row['gender']
        if not dob or not gender: continue
        
        
        # Calculate Age
        try:
            today = datetime.now()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        except: continue
        
        cat_code = get_category_code_for_minima(age)
        if not cat_code: continue
        
        # Get Event DB Name
        # `row['event_name']` is likely "50 metros Libre". Map to "50 Free".
        evt_display = row['event_name']
        evt_db_list = resolve_db_event_names(evt_display) # Returns list, usually 1
        
        pool_size = row['pool_size'] # "25m" or "50m"
        
        my_time = row['seconds']
        
        for evt_db in evt_db_list:
            # Check Minima
            # Key: (event_db, gender, cat_code, pool_size)
            # Try exact pool size match first
            limit = minima_lookup.get((evt_db, gender, cat_code, pool_size))
            
            # If valid limit and my_time <= limit
            if limit and my_time <= limit:
                # QUALIFIED!
                # Calculate % improvement or diff?
                diff = limit - my_time
                
                qualifiers.append({
                    "Nadador": s_name,
                    "Edad": age,
                    "Género": gender,
                    "Prueba": evt_display,
                    "Piscina": pool_size,
                    "Tiempo": row['time'], # Original string
                    "Mínima": format_seconds(limit),
                    "Diff": f"-{diff:.2f}s",
                    "Fecha": row['date'],
                    "Torneo": row.get('meet_name', '') # strictly load_all_best_times might not have meet_name but query above didn't select it? 
                    # Checking `load_all_best_times` query: it selects `m.date`, does it select `m.name`? 
                    # It selects `s.id... r.event_name... m.date`. NO m.name. 
                    # We can leave Torneo empty or add it to query. Let's leave empty for now to avoid breaking schema.
                })
                
    if not qualifiers:
        st.info(f"No se han encontrado marcas mínimas (Revisados {len(best_times)} tiempos).")
        return
        
    q_df = pd.DataFrame(qualifiers)
    
    # UI Stats
    c1, c2, c3 = st.columns(3)
    c1.metric("Nadadores Clasificados", q_df['Nadador'].nunique())
    c2.metric("Total Marcas", len(q_df))
    
    st.markdown("---")
    
    # DEBUG PROBE: verify DF contents
    # st.dataframe(q_df)
    
    # Sort Logic: Age ASC, then Surname ASC
    # 1. Create a summary DF of swimmers [Name, Age]
    swimmer_list = q_df[['Nadador', 'Edad']].drop_duplicates()
    
    # helper for surname sort
    def get_sort_key(full_name):
        parts = full_name.split()
        if len(parts) >= 2:
            # Assume last part is surname? Or second to last? 
            # In Chile "Name Paternal Maternal". Paternal is key.
            # If 3 parts: "First Paternal Maternal" -> sort by Paternal (parts[-2])
            # If 2 parts: "First Last" -> sort by Last (parts[-1])
            # If 4 parts: "First Middle Paternal Maternal" -> sort by Paternal (parts[-2])
            if len(parts) == 2: return parts[-1]
            return parts[-2]
        return full_name
        
    swimmer_list['SurnameKey'] = swimmer_list['Nadador'].apply(get_sort_key)
    swimmer_list = swimmer_list.sort_values(['Edad', 'SurnameKey', 'Nadador'], ascending=[True, True, True])
    
    unique_swimmers = swimmer_list['Nadador'].tolist()
    
    for swimmer_name in unique_swimmers:
        group = q_df[q_df['Nadador'] == swimmer_name]
        
        # Get basic info from first row
        info = group.iloc[0]
        n_events = len(group)
        gender_icon = "♀️" if info['Género'] == 'F' else "♂️"
        
        with st.expander(f"{gender_icon} {swimmer_name} ({n_events} pruebas)"):
            st.dataframe(
                group[['Prueba', 'Piscina', 'Tiempo', 'Mínima', 'Diff', 'Fecha']]
                .sort_values(['Piscina', 'Prueba']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Prueba": st.column_config.TextColumn("Prueba", width="medium"),
                    "Tiempo": st.column_config.TextColumn("Tiempo", help="Mejor tiempo del nadador"),
                    "Mínima": st.column_config.TextColumn("Mínima", help="Marca mínima exigida"),
                    "Diff": st.column_config.TextColumn("Margen", help="Tiempo por debajo de la mínima"),
                }
            )

def render_team_view(swimmers_df):
    def get_display_data(row):
        dob = row.get('birth_date')
        if not dob: return "Desconocido"
        return calculate_category(dob)
    
    swimmers_df['Categoría'] = swimmers_df.apply(get_display_data, axis=1)

    # --- TEAM HEADER ---
    # --- TEAM HEADER (HERO STYLE) ---
    # Use user-provided background "fondo_rama.png" from this folder.
    app_dir = os.path.dirname(os.path.abspath(__file__))
    fondo_path = os.path.join(app_dir, "fondo_rama.png")
    fallback_paths = [
        os.path.join(app_dir, "pool_header_bg.png"),
        os.path.abspath(os.path.join(app_dir, os.pardir, "pool_header_bg.png")),
    ]
    fallback_path = next((p for p in fallback_paths if os.path.exists(p)), fallback_paths[0])
    bg_image_file = fondo_path if os.path.exists(fondo_path) else fallback_path
    
    if os.path.exists(bg_image_file):
        img_b64 = get_img_as_base64(bg_image_file)
        
        st.markdown(html_block(f"""
        <style>
            .hero-container {{
                background-image: linear-gradient(rgba(0, 0, 0, 0.4), rgba(0, 0, 0, 0.7)), url("data:image/png;base64,{img_b64}");
                background-size: cover;
                background-position: center;
                height: 350px; /* Increased height to show more of the image */
                border-radius: 15px;
                display: flex;
                align-items: center; /* Vertically center the content */
                justify-content: center; /* Center content horizontally */
                padding: 2rem;
                position: relative;
                color: white;
                box-shadow: 0 4px 10px rgba(0, 0, 0, 0.3);
                text-align: center;
            }}
            .hero-content {{
                z-index: 2;
                max_width: 800px;
            }}
            .hero-title {{
                font-size: 3.5rem;
                font-weight: 800;
                margin: 0;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.8); /* Stronger shadow for readability */
                line-height: 1.1;
                color: #ffffff !important;
                letter-spacing: 1px;
            }}
            .hero-subtitle {{
                font-size: 1.2rem;
                margin-top: 1rem;
                font-weight: 500;
                color: #f1f5f9 !important;
                text-shadow: 1px 1px 3px rgba(0,0,0,0.8);
                display: flex;
                flex-direction: column;
                align-items: center;
                gap: 0.3rem;
            }}
            .coach-tag {{
                background-color: rgba(255, 255, 255, 0.15);
                padding: 4px 12px;
                border-radius: 20px;
                backdrop-filter: blur(5px);
                font-size: 1rem;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }}
        </style>
        
        <div class="hero-container">
            <div class="hero-content">
                <h1 class="hero-title">Rama de Natación Peñalolén</h1>
                <div class="hero-subtitle">
                    <div style="margin-bottom: 5px;">🇨🇱 Santiago, Chile</div>
                    
                    <div class="coach-tag">Head Coach: Cesar Cereceda</div>
                    
                    <div style="display: flex; gap: 10px; margin-top: 5px; flex-wrap: wrap; justify-content: center;">
                        <span class="coach-tag">Coach Infantiles: Oscar Cifuentes</span>
                        <span class="coach-tag">Coach Pre-Rama: Valentina Contalba</span>
                    </div>
                </div>
            </div>
        </div>
        """), unsafe_allow_html=True)
    else:
        # Fallback if image missing: Simple blue header, NO LOGO
        st.markdown(html_block("""
        <div style="background-color: #0f172a; padding: 2rem; border-radius: 15px; text-align: center; color: white; margin-bottom: 2rem;">
            <h1 style="margin:0; font-size: 3rem;">Rama de Natación Peñalolén</h1>
            <div style="margin-top: 1rem; opacity: 0.9;">
                <div>Santiago, Chile</div>
                <div style="margin-top: 5px;"><strong>Head Coach:</strong> Cesar Cereceda</div>
                <div><strong>Infantiles:</strong> Oscar Cifuentes • <strong>Pre-Rama:</strong> Valentina Contalba</div>
            </div>
        </div>
        """), unsafe_allow_html=True)
        
    st.markdown("---")
    
    tab_list = ["🏠 Inicio", "🏊 Plantel", "🏆 Torneos", "📊 Análisis", "📈 Estadisticas", "🏅 Clasificados"]
    is_admin = st.session_state.get("username") == "admin"
    if is_admin:
        tab_list.append("📝 Ingreso")
        
    tabs = st.tabs(tab_list)
    t_home, t_roster, t_meets, t_analysis, t_stats, t_qualifiers = tabs[0], tabs[1], tabs[2], tabs[3], tabs[4], tabs[5]
    t_ingreso = tabs[6] if is_admin else None
    
    with t_home:
        c1, c2, c3 = st.columns(3)
        c1.metric("Nadadores Activos", len(swimmers_df))
        
        meets_df = load_meets()
        c2.metric("Torneos Registrados", len(meets_df))
        
        # Recent Meets
        st.subheader("Competencias Recientes")
        if not meets_df.empty:
            for i in range(min(5, len(meets_df))):
                meet = meets_df.iloc[i]
                st.markdown(f"""
                <div style="padding: 1rem; background-color: #f8fafc; border-radius: 8px; margin-bottom: 0.5rem; border-left: 4px solid #3b82f6;">
                    <strong style="color: #0f172a;">{meet['name']}</strong><br>
                    <span style="color: #64748b; font-size: 0.9rem;">📅 {meet['date']} • 📏 {meet.get('pool_size', 'N/A')}</span>
                </div>
                """, unsafe_allow_html=True)

    with t_roster:
        st.markdown("### 🏊 Lista de Nadadores")
        
        search_query = st.text_input("Buscar Nadador...", "")
        
        if search_query:
            filtered = swimmers_df[swimmers_df['name'].str.lower().str.contains(search_query.lower())]
        else:
            filtered = swimmers_df

        display_df = filtered[['name', 'Categoría', 'gender']].copy()
        display_df.columns = ['Nombre', 'Categoría', 'Género']
        
        event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )
        
        if len(event.selection.rows) > 0:
            row_idx = event.selection.rows[0]
            selected_id = filtered.iloc[row_idx]['id']
            go_to_swimmer(selected_id)
            st.rerun()

    with t_meets:
        st.subheader("Historial de Torneos")
        st.info("💡 Puedes editar el tamaño de la piscina (25m/50m) directamente en la tabla.")
        
        # Prepare Data
        # We need ID for updates
        editor_df = meets_df.copy()
        
        # Config Columns
        column_cfg = {
            "id": None, 
            "url": None,
            "date_obj": None,
            "name": "Nombre",
            "date": "Fecha",
            "location": "Ciudad",
            "address": "Dirección",
            "pool_size": st.column_config.SelectboxColumn(
                "Piscina",
                options=["25m", "50m"],
                required=True,
                help="Selecciona el tamaño de la piscina"
            )
        }
        
        edited_df = st.data_editor(
            editor_df,
            column_config=column_cfg,
            disabled=["name", "date", "location", "address"], # Only pool_size editable
            use_container_width=True,
            hide_index=True,
            key="meets_editor"
        )
        
        # Detect Changes
        # This simple equality check works because source is sorted and index preserved
        if not edited_df.equals(editor_df):
            for i in editor_df.index:
                old_val = editor_df.loc[i, 'pool_size']
                new_val = edited_df.loc[i, 'pool_size']
                m_id = editor_df.loc[i, 'id'] # ID is hidden but present
                
                if old_val != new_val:
                    update_meet_pool_size(m_id, new_val)
                    st.toast(f"Piscina actualizada a {new_val}!", icon="✅")
                    time.sleep(1) # Visual feedback
                    st.rerun()

    with t_analysis:
        render_analysis_tab(swimmers_df)

    with t_stats:
        st.subheader("📈 Estadísticas avanzadas")
        all_results = load_all_results()
        if all_results.empty:
            st.info("No hay resultados suficientes para calcular estadísticas.")
        else:
            swimmer_names = sorted(all_results['name'].dropna().unique())
            selected_name = st.selectbox("Nadador", swimmer_names, key="stats_swimmer")
            swimmer_df = all_results[all_results['name'] == selected_name].copy()
            if swimmer_df.empty:
                st.info("No hay resultados para este nadador.")
            else:
                event_options = sorted(swimmer_df['event_display'].dropna().unique())
                selected_event = st.selectbox("Prueba", event_options, key="stats_event")
                event_all_df = swimmer_df[swimmer_df['event_display'] == selected_event].copy()
                pool_options = sorted(event_all_df['pool_size'].dropna().unique())
                selected_pool = st.selectbox("Piscina", pool_options, key="stats_pool") if pool_options else ""
                event_df = event_all_df.copy()
                if selected_pool:
                    event_df = event_all_df[event_all_df['pool_size'] == selected_pool].copy()

                event_df = event_df.dropna(subset=['seconds'])
                event_df = event_df.sort_values('date_obj')
                pb_time = best_time(event_df)
                latest_sec = event_df['seconds'].iloc[-1] if not event_df.empty else None
                yearly_best = event_df.dropna(subset=['year']).groupby('year')['seconds'].min().sort_index()
                cagr = compute_cagr_improvement(yearly_best)
                gap_pb = compute_gap_to_pb(latest_sec, pb_time)
                last_n = event_df['seconds'].tail(5)
                volatility = last_n.std(ddof=0) if len(last_n) >= 2 else None

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("PB", format_seconds(pb_time) if pb_time else "—")
                c2.metric("Gap vs PB", f"{gap_pb:.1f}%" if gap_pb is not None else "—")
                c3.metric("Volatilidad (últ. 5)", f"{volatility:.2f}s" if volatility is not None else "—")
                c4.metric("CAGR mejora anual", f"{cagr:.2f}%" if cagr is not None else "—")

                st.markdown("#### Conversión SC/LC (misma prueba)")
                sc_time = best_time(event_all_df[event_all_df['pool_size'] == "25m"])
                lc_time = best_time(event_all_df[event_all_df['pool_size'] == "50m"])
                if sc_time and lc_time:
                    delta_sc_lc = lc_time - sc_time
                    ratio_sc_lc = lc_time / sc_time
                    st.write(f"Mejor 25m: **{format_seconds(sc_time)}** • Mejor 50m: **{format_seconds(lc_time)}**")
                    st.write(f"Delta LC-SC: **{delta_sc_lc:.2f}s** • Ratio LC/SC: **{ratio_sc_lc:.3f}**")
                else:
                    st.info("No hay tiempos suficientes en 25m y 50m para esta prueba.")

                st.markdown("#### Índice de resistencia (50 vs 200 del mismo estilo)")
                style_key = extract_style(selected_event)
                style_map = {
                    "Libre": "Free",
                    "Espalda": "Back",
                    "Pecho": "Breast",
                    "Mariposa": "Fly",
                }
                endurance_rows = []
                if style_key in style_map:
                    db_style = style_map[style_key]
                    for pool in ["25m", "50m"]:
                        t50 = best_time(swimmer_df[(swimmer_df['event_name'] == f"50 {db_style}") & (swimmer_df['pool_size'] == pool)])
                        t200 = best_time(swimmer_df[(swimmer_df['event_name'] == f"200 {db_style}") & (swimmer_df['pool_size'] == pool)])
                        if t50 and t200:
                            pace50 = t50 / 50
                            pace200 = t200 / 200
                            endurance_rows.append({
                                "Estilo": style_key,
                                "Piscina": pool,
                                "Ritmo 50m (s/m)": f"{pace50:.3f}",
                                "Ritmo 200m (s/m)": f"{pace200:.3f}",
                                "Ratio 50/200": f"{(pace50 / pace200):.3f}",
                            })
                if endurance_rows:
                    st.dataframe(pd.DataFrame(endurance_rows), use_container_width=True, hide_index=True)
                else:
                    if style_key in style_map:
                        st.info(f"No hay tiempos suficientes en 50/200 {style_key} para calcular el índice.")
                    else:
                        st.info("Índice de resistencia no aplica para esta prueba.")

                st.markdown("#### IM Weakness (sumatoria 50s vs 200 IM)")
                im_rows = []
                for pool in ["25m", "50m"]:
                    t_fly = best_time(swimmer_df[(swimmer_df['event_name'] == "50 Fly") & (swimmer_df['pool_size'] == pool)])
                    t_back = best_time(swimmer_df[(swimmer_df['event_name'] == "50 Back") & (swimmer_df['pool_size'] == pool)])
                    t_breast = best_time(swimmer_df[(swimmer_df['event_name'] == "50 Breast") & (swimmer_df['pool_size'] == pool)])
                    t_free = best_time(swimmer_df[(swimmer_df['event_name'] == "50 Free") & (swimmer_df['pool_size'] == pool)])
                    t_im = best_time(swimmer_df[(swimmer_df['event_name'] == "200 IM") & (swimmer_df['pool_size'] == pool)])
                    if t_fly and t_back and t_breast and t_free and t_im:
                        sum_50s = t_fly + t_back + t_breast + t_free
                        delta = t_im - sum_50s
                        im_rows.append({
                            "Piscina": pool,
                            "Suma 4x50": format_seconds(sum_50s),
                            "200 IM": format_seconds(t_im),
                            "Delta (IM - sum)": f"{delta:.2f}s",
                            "Delta %": f"{(delta / t_im) * 100:.2f}%",
                        })
                if im_rows:
                    st.dataframe(pd.DataFrame(im_rows), use_container_width=True, hide_index=True)
                else:
                    st.info("No hay datos suficientes para IM Weakness en este nadador.")

                st.markdown("#### Puntos FINA (nivel relativo)")
                points = pd.to_numeric(swimmer_df['points'], errors='coerce').dropna()
                if not points.empty:
                    p1, p2 = st.columns(2)
                    p1.metric("Mejor Puntos FINA", f"{points.max():.0f}")
                    p2.metric("Promedio Puntos FINA", f"{points.mean():.0f}")
                else:
                    st.info("No hay puntos FINA suficientes para este nadador.")

                st.markdown("#### Datos no disponibles en la base actual")
                st.caption("Splits/parciales, preliminares vs finales y presión competitiva requieren datos adicionales.")



    with t_qualifiers:
        render_qualifiers_tab(swimmers_df)
        
    if is_admin and t_ingreso:
        with t_ingreso:
            st.subheader("📝 Ingreso y Actualización de Datos")
            
            t_dob, t_logs, t_other = st.tabs(["Actualizar Cumpleaños (Masivo)", "Registros de Acceso", "Otros"])
            
            with t_dob:
                st.markdown("""
                ### Pegar datos de Excel/Google Sheets
                Formato esperado (columnas): **Nombre | (Opcionales) | Fecha**
                El sistema buscará fechas como `DD-MMM-YY` (ej: `16-ene-10`) y tratará de asociarlas al nadador más parecido.
                """)
                
                raw_text = st.text_area("Pegar datos aquí:", height=300)
                
                if st.button("Procesar y Actualizar"):
                    if not raw_text.strip():
                        st.warning("Por favor pega algunos datos.")
                    else:
                        with st.status("Procesando...", expanded=True) as status:
                            st.write("Conectando base de datos...")
                            count, errors = process_bulk_dob_update(raw_text)
                            
                            if errors:
                                st.write(f"⚠️ Se encontraron {len(errors)} errores/advertencias.")
                                with st.expander("Ver detalles"):
                                    for e in errors:
                                        st.write(e)
                            
                            st.write(f"✅ Se actualizaron **{count}** fechas de nacimiento.")
                            status.update(label="Proceso Completado", state="complete", expanded=False)
                            
                        if count > 0:
                            st.success(f"¡Éxito! {count} nadadores actualizados.")
                            time.sleep(2)
                            st.rerun()

            with t_logs:
                st.subheader("🕵️ Registros de Acceso")
                if st.button("Actualizar Logs"):
                     conn = get_connection()
                     if conn:
                         try:
                             logs = pd.read_sql("SELECT * FROM access_logs ORDER BY timestamp DESC LIMIT 100", conn)
                             st.dataframe(logs, use_container_width=True)
                         except Exception as e:
                             st.error(f"Error cargando logs: {e}")
                         finally:
                             conn.close()
                else:
                    # Load default
                    conn = get_connection()
                    if conn:
                        try:
                            logs = pd.read_sql("SELECT * FROM access_logs ORDER BY timestamp DESC LIMIT 50", conn)
                            st.dataframe(logs, use_container_width=True)
                        except: pass
                        finally: conn.close()

            with t_other:
                st.info("Futuras herramientas de ingreso aquí.")

def render_profile_view(swimmer_id, swimmers_df):
    # Get Swimmer Data
    swimmer = swimmers_df[swimmers_df['id'] == swimmer_id].iloc[0]
    name = swimmer['name']
    
    # Navbar
    if st.button("⬅️ Volver al Roster"):
        go_to_home()
        st.rerun()
        
    # --- HEADER SECTION ---
    # --- HEADER SECTION ---
    # Mimic Swimcloud Header
    # Placeholder Avatar
    avatar_url = "https://www.swimcloud.com/img/avatar-default.png" # or local asset
    
    st.markdown(f"""
    <div style="background-color: #1e293b; padding: 2rem; border-radius: 10px; color: white; margin-bottom: 2rem; display: flex; align-items: center; gap: 2rem;">
        <div style="flex-shrink: 0;">
            <div style="width: 100px; height: 100px; background-color: #334155; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 2rem; border: 3px solid #64748b;">
                🏊
            </div>
        </div>
        <div style="flex-grow: 1;">
            <h1 style="margin:0; color: white; font-size: 2.5rem;">{name}</h1>
            <p style="opacity: 0.8; margin-top: 5px; font-size: 1.1rem;">Rama de Natación Peñalolén • ID: {swimmer_id}</p>
            <div style="display: flex; gap: 15px; margin-top: 20px; flex-wrap: wrap;">
                <div style="background: rgba(255,255,255,0.1); padding: 5px 15px; border-radius: 20px; border: 1px solid rgba(255,255,255,0.2);">
                    <small style="text-transform: uppercase; font-size: 0.7rem; opacity: 0.7;">Categoría</small><br>
                    <strong>{calculate_category(swimmer.get('birth_date'))}</strong>
                </div>
                <div style="background: rgba(255,255,255,0.1); padding: 5px 15px; border-radius: 20px; border: 1px solid rgba(255,255,255,0.2);">
                    <small style="text-transform: uppercase; font-size: 0.7rem; opacity: 0.7;">Género</small><br>
                    <strong>{swimmer.get('gender', '?')}</strong>
                </div>
                 <div style="background: rgba(255,255,255,0.1); padding: 5px 15px; border-radius: 20px; border: 1px solid rgba(255,255,255,0.2);">
                    <small style="text-transform: uppercase; font-size: 0.7rem; opacity: 0.7;">Nacimiento</small><br>
                    <strong>{swimmer.get('birth_date', 'N/A')}</strong>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # --- DATA LOADING ---
    results_df = load_results(swimmer_id)
    
    if results_df.empty:
        st.info("No hay resultados registrados.")
        return

    # Process Data
    def clean_date(d):
        if not isinstance(d, str): return d
        try:
            if ',' in d:
                parts = d.split(',')
                year = parts[-1].strip()
                date_part = parts[0]
                return f"{date_part}, {year}"
            return d
        except: return d

    results_df['date_clean'] = results_df['date'].apply(clean_date)
    results_df['date_obj'] = pd.to_datetime(results_df['date_clean'], errors='coerce')
    results_df['date_str'] = results_df['date_obj'].dt.date 
    
    graph_df = results_df.dropna(subset=['date_obj']).copy()
    graph_df['seconds'] = graph_df['time'].apply(parse_time)
    results_df['seconds'] = results_df['time'].apply(parse_time)
    results_df['style'] = results_df['event_name'].apply(extract_style)
    results_df['event_display'] = results_df['event_name'].apply(get_event_display_name)
    graph_df['event_display'] = graph_df['event_name'].apply(get_event_display_name)
    
    # --- TABS ---
    t_times, t_progression, t_stats = st.tabs(["⏱️ Tiempos", "📈 Progresión", "📊 Estadísticas"])
    
    with t_times:
        st.markdown("### Historial Completo")
        
        # Ensure numeric for formatting
        results_df['points'] = pd.to_numeric(results_df['points'], errors='coerce').fillna(0)
        
        # Style function for Place
        def highlight_podium(s):
            return ['background-color: #ffd700; color: black' if v == 1 else 
                    'background-color: #c0c0c0; color: black' if v == 2 else 
                    'background-color: #cd7f32; color: black' if v == 3 else '' for v in s]

        display_res = (results_df[['date_str', 'meet_name', 'event_display', 'pool_size', 'time', 'place', 'points']]
            .rename(columns={
                'date_str': 'Fecha', 'meet_name': 'Torneo', 'event_display': 'Prueba', 
                'pool_size': 'Piscina', 'time': 'Tiempo', 'place': 'Lugar', 'points': 'Puntos FINA'
            })
            .sort_values('Fecha', ascending=False)
        )
        
        st.dataframe(
            display_res.style.map(lambda x: 'color: goldenrod; font-weight: bold;' if x == 1 else ('color: silver; font-weight: bold;' if x == 2 else ('color: #cd7f32; font-weight: bold;' if x == 3 else '')), subset=['Lugar'])
            .format({"Puntos FINA": "{:.0f}"}),
            use_container_width=True,
            hide_index=True
        )
        
    with t_progression:
         # Simplified Chart Logic for Redesign
         # Filter to only events with valid times/dates
         graph_data = graph_df.dropna(subset=['seconds', 'date_obj'])
         
         if graph_data.empty:
             st.info("No hay datos suficientes para graficar.")
         else:
             unique_events = sorted(graph_data['event_display'].unique())
             evt_display = st.selectbox("Seleccionar Prueba", unique_events)
         
             db_event_names = resolve_db_event_names(evt_display)
             subset = graph_df[graph_df['event_name'].isin(db_event_names)].sort_values('date_obj')
             
             # CRITICAL FIX: Remove rows with no time to ensure unconnected lines don't appear mid-chart
             subset = subset.dropna(subset=['seconds'])
             
             if not subset.empty:
                 # Create formatted time for tooltip
                 subset['time_str'] = subset['seconds'].apply(format_seconds)
                 
                 fig = px.line(
                     subset, 
                     x='date_obj', 
                     y='seconds', 
                     color='pool_size', 
                     markers=True, 
                     title=f"Historia: {evt_display}",
                     custom_data=['time_str', 'meet_name']
                 )
                 
                 # Custom Tooltip
                 fig.update_traces(
                     hovertemplate="<b>%{x|%d %b %Y}</b><br>Tiempo: %{customdata[0]}<br>Torneo: %{customdata[1]}"
                 )
                 
                 # Custom Y-Axis Formatting
                 # Determine range to generate nice ticks
                 min_y = subset['seconds'].min() * 0.95
                 max_y = subset['seconds'].max() * 1.05
                 
                 # Generate ticks every X seconds depending on range
                 import numpy as np
                 y_range = max_y - min_y
                 if y_range < 5: step = 0.5
                 elif y_range < 15: step = 1
                 elif y_range < 60: step = 5
                 else: step = 10
                 
                 tick_vals = np.arange(int(min_y), int(max_y)+1, step)
                 tick_text = [format_seconds(v) for v in tick_vals]
                 
                 fig.update_layout(
                     yaxis=dict(
                         title="Tiempo",
                         tickmode='array',
                         tickvals=tick_vals,
                         ticktext=tick_text
                     ),
                     xaxis=dict(title="Fecha"),
                     hovermode="x unified"
                 )
                 
                # Add Minimas and Records
                 minimas_df = load_minimas()
                 records_df = load_national_records()
                 
                 # Robust Gender Normalization
                 raw_gen = swimmer.get('gender')
                 swimmer_gen = 'F' # Default fallback
                 
                 if raw_gen:
                     s_str = str(raw_gen).upper().strip()
                     if s_str in ['M', 'MALE', 'HOMBRE', 'MASCULINO']: swimmer_gen = 'M'
                     elif s_str in ['F', 'FEMALE', 'MUJER', 'FEMENINO']: swimmer_gen = 'F'
                 else:
                     # Heuristic for Null gender
                     name_first = swimmer.get('name', '').split()[0].upper()
                     if name_first.endswith('A') and name_first not in ['LUCA', 'NICOLA']:
                         swimmer_gen = 'F'
                     else:
                         swimmer_gen = 'M'
                 
                 swimmer_dob = swimmer.get('birth_date')
                 
                 # Calculate Age for Lookup
                 current_age = 0
                 if swimmer_dob:
                     try:
                         # Ensure DOB is datetime
                         if isinstance(swimmer_dob, str):
                            bd = pd.to_datetime(swimmer_dob)
                         else: bd = swimmer_dob
                         current_age = datetime.now().year - bd.year
                     except: pass
                 
                 # Helper to find closest category match
                 def find_match_val(df_source, p_size, s_gen, s_age, val_col):
                     if df_source.empty: return None
                     # Filter by Event, Pool, Gender
                     # Normalized event names should match now
                     f = df_source[
                         (df_source['event_name'].isin(db_event_names)) & 
                         (df_source['pool_size'] == p_size) & 
                         (df_source['gender'] == s_gen)
                     ]
                     if f.empty: return None
                     
                     # Match Category Code
                     # Simple heuristics for now as codes vary (11-12, 10-10, 11-12 años)
                     # Strategy: Look for explicitly matching age range
                     for _, r in f.iterrows():
                         code = r['category_code'].replace(' años', '').replace(' ', '')
                         # Handle "11-12"
                         if '-' in code:
                             try:
                                 low, high = map(int, code.split('-'))
                                 if low <= s_age <= high: return r[val_col]
                             except: pass
                         # Handle single digit "10" (often stored as 10-10)
                         elif code.isdigit():
                             if int(code) == s_age: return r[val_col]
                         # Handle "Open"
                         # Handle "Open"
                         elif 'open' in code.lower() or 'todo' in code.lower() or 'absoluto' in code.lower():
                             # Return the value for Open/Absoluto
                             return r[val_col]
                     return None

                 # Iterate over pools present in graph
                 pools = subset['pool_size'].unique()
                 for p in pools:
                     # Minima
                     # Column name for time in seconds? 
                     # minimum_standards: time_seconds
                     val_min = find_match_val(minimas_df, p, swimmer_gen, current_age, 'time_seconds')
                     if val_min:
                         fig.add_hline(y=val_min, line_dash="dash", line_color="red" if p=='50m' else "orange", 
                                       annotation_text=f"Minima {p} ({format_seconds(val_min)})", annotation_position="bottom right")

                     # Record
                     # national_records: time text. Need to parse? 
                     # It doesn't have seconds column. Need helper.
                     # Or assuming I adding check? The table schema didn't show seconds.
                     # load_national_records just reads *.
                     # Let's parse on fly.
                     rec_row_time = find_match_val(records_df, p, swimmer_gen, current_age, 'time')
                     if rec_row_time:
                         rec_sec = parse_time(rec_row_time)
                         if rec_sec:
                             fig.add_hline(y=rec_sec, line_dash="dot", line_color="gold",
                                           annotation_text=f"Record {p} ({format_seconds(rec_sec)})", annotation_position="top right")

                 st.plotly_chart(fig, use_container_width=True)
                 
                 # Results Table
                 st.markdown("##### Resultados Detallados")
                 st.dataframe(
                     subset[['date_str', 'meet_name', 'time', 'pool_size', 'place', 'points']]
                     .rename(columns={'date_str':'Fecha', 'meet_name':'Torneo', 'time':'Tiempo', 'pool_size':'Piscina', 'place':'Lugar', 'points':'Puntos'}),
                     use_container_width=True,
                     hide_index=True
                 )
                 
             else:
                 st.info("Datos insuficientes para graficar.")
             
    with t_stats:
        # PBs Table
        st.subheader("Mejores Marcas Personales (PB)")
        
        # Filter for valid times only
        valid_times = results_df.dropna(subset=['seconds'])
        
        if not valid_times.empty:
            best_df = valid_times.loc[valid_times.groupby(['event_name', 'pool_size'])['seconds'].idxmin()].sort_values('event_name')
            best_df['event_display'] = best_df['event_name'].apply(get_event_display_name)
            st.dataframe(
            best_df[['event_display', 'time', 'pool_size', 'date_str', 'meet_name']]
            .rename(columns={'event_display':'Prueba', 'time':'Mejor Tiempo', 'pool_size':'Piscina', 'date_str':'Fecha', 'meet_name':'Torneo'}),
            hide_index=True,
            use_container_width=True
        )


# --- LOGGING & ADMIN ---
def log_access(username):
    try:
        conn = get_connection()
        if conn:
            # Try to get IP (Best Effort)
            ip_addr = "Unknown"
            try:
                # Modern Streamlit (via st.context)
                if hasattr(st, "context") and st.context.headers:
                    headers = st.context.headers
                    ip_addr = headers.get("X-Forwarded-For", headers.get("Remote-Addr", "Unknown"))
                else:
                     # Fallback or older versions
                     from streamlit.web.server.websocket_headers import _get_websocket_headers
                     headers = _get_websocket_headers()
                     if headers:
                        ip_addr = headers.get("X-Forwarded-For", headers.get("Remote-Addr", "Unknown"))
            except:
                pass
            
            cursor = conn.cursor()
            cursor.execute("INSERT INTO access_logs (username, ip_address) VALUES (?, ?)", (username, ip_addr))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Log Error: {e}")

def check_password():
    """Returns `True` if the user had a correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in st.secrets["passwords"] and \
                st.session_state["password"] == st.secrets["passwords"][st.session_state["username"]]:
            st.session_state["password_correct"] = True
            log_access(st.session_state["username"]) # Log successful login
            # del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show inputs for username + password.
        show_login_form(password_entered)
        return False
        
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        show_login_form(password_entered, error="😕 Usuario o clave incorrecta")
        return False
        
    else:
        return True

def show_login_form(callback, error=None):
    # Load background image
    app_dir = os.path.dirname(os.path.abspath(__file__))
    fondo_path = os.path.join(app_dir, "fondo_rama.png")
    
    img_css = ""
    if os.path.exists(fondo_path):
        b64 = get_img_as_base64(fondo_path)
        img_css = f"""
            background-image: linear-gradient(rgba(0, 0, 0, 0.5), rgba(0, 0, 0, 0.7)), url("data:image/png;base64,{b64}");
            background-size: cover;
            background-position: center;
        """
    
    st.markdown(f"""
    <style>
        .stApp {{
            {img_css}
        }}
        /* Move content UP */
        .block-container {{
            padding-top: 10vh !important;
            padding-bottom: 2rem !important;
        }}
        /* Hide main menu etc on login */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        [data-testid="stSidebar"] {{display: none;}}
        
        /* Style Inputs */
        div[data-testid="stTextInput"] label {{
            color: #ffffff !important; /* Pure White */
            font-size: 1rem;
            text-shadow: 0px 1px 3px rgba(0,0,0,0.8);
        }}
        div[data-testid="stTextInput"] input {{
            background-color: rgba(15, 23, 42, 0.9) !important; /* Dark Blue-Gray */
            color: white !important;
            border: 1px solid rgba(255, 255, 255, 0.2) !important;
        }}
        
    </style>
    """, unsafe_allow_html=True)
    
    # Render Form
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        # Just the inputs, no wrapper div
        st.text_input("Usuario", key="username")
        st.text_input("Contraseña", type="password", on_change=callback, key="password")
        
        if error:
            st.markdown(f'<p style="color: #ff4b4b; margin-top: 10px; background: rgba(0,0,0,0.5); padding: 5px; border-radius: 5px;">{error}</p>', unsafe_allow_html=True)

# --- MAIN APP LOGIC ---
def main():
    # Admin Panel (Sidebar) - REMOVED, now in Ingreso Tab
    # user = st.session_state.get("username", "Guest")
    # if user == "admin": ... (Removed)
    
    # Global CSS for Safari/Dark Mode Dropdowns - FINAL FIX
    st.markdown("""
        <style>
            /* 1. Closed Input Box */
            .stSelectbox div[data-baseweb="select"] div {
                color: white !important;
                -webkit-text-fill-color: white !important;
            }

            /* 2. The Main Dropdown List Container */
            ul[data-testid="stSelectboxVirtualDropdown"] {
                background-color: #0f172a !important;
            }

            /* 3. Individual Options (Default State) */
            li[role="option"] {
                background-color: #0f172a !important;
                color: white !important;
            }
            
            /* 4. Text inside Options */
            li[role="option"] div,
            li[role="option"] span {
                color: white !important;
                -webkit-text-fill-color: white !important;
            }

            /* 5. Hover & Selected State */
            li[role="option"]:hover,
            li[role="option"][aria-selected="true"] {
                background-color: #334155 !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
    swimmers = load_swimmers()
    if swimmers.empty:
        st.error("No se encontraron nadadores en la base de datos.")
        st.stop()

    if st.session_state['selected_swimmer_id']:
        render_profile_view(st.session_state['selected_swimmer_id'], swimmers)
    else:
        render_team_view(swimmers)

if __name__ == "__main__":
    if check_password():
        main()
