import sqlite3
import urllib.request
from bs4 import BeautifulSoup
import ssl
import re
import os
import pdfplumber
import pandas as pd
from difflib import SequenceMatcher
from datetime import datetime
import time

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/natacion.db")
if not os.path.exists(DB_PATH):
    # try one dir up
    alt_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/natacion.db")
    if os.path.exists(alt_path):
        DB_PATH = alt_path

TEAMS_REGEX = ["penalolen", "peñalolen", "peñalolén", "rama de natacion penalolen", "rama natacion penalolen", "crnp", "penilolen", "peñilolen", "penálolen"]

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS fechida_scrapes (
            url_id TEXT PRIMARY KEY,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def is_already_scraped(url_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT 1 FROM fechida_scrapes WHERE url_id = ?", (url_id,))
    res = c.fetchone()
    conn.close()
    return res is not None

def mark_as_scraped(url_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO fechida_scrapes (url_id) VALUES (?)", (url_id,))
    conn.commit()
    conn.close()

def get_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def map_event_name(raw_event_line):
    # Example: "Evento 6 Hombres 15-17 200 CL Metro Estilo Libre"
    # Example: "Event 1 Girls 9 Year Olds 100 LC Meter Butterfly"
    
    # 1. Distance
    dist_match = re.search(r'\b(25|50|100|200|400|800|1500)\b', raw_event_line)
    distance = dist_match.group(1) if dist_match else None
    
    # 2. Style
    lower_line = raw_event_line.lower()
    style = None
    if 'libre' in lower_line or 'free' in lower_line:
        style = 'Free'
    elif 'espalda' in lower_line or 'back' in lower_line:
        style = 'Back'
    elif 'pecho' in lower_line or 'breast' in lower_line:
        style = 'Breast'
    elif 'mariposa' in lower_line or 'fly' in lower_line or 'butterfly' in lower_line:
        style = 'Fly'
    elif 'ci' in lower_line or 'im' in lower_line or 'combinado' in lower_line or 'medley' in lower_line:
        style = 'IM'
        
    # Check for relay
    is_relay = 'relevo' in lower_line or 'relay' in lower_line
    if is_relay:
        if style == 'IM': style = 'Medley'
        style += ' Relay'
        
    if not distance or not style:
        return raw_event_line # fallback
        
    return f"{distance} {style}"

def map_pool_size(raw_event_line):
    if 'SC' in raw_event_line or '25m' in raw_event_line.lower():
        return '25m'
    return '50m' # CL usually means 50m (Course Long)

def parse_pdf(pdf_path, meet_id):
    results = []
    current_event = None
    
    # sort teams just in case
    teams = sorted(TEAMS_REGEX, key=len, reverse=True)
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                for line in text.split('\n'):
                    if re.match(r'^(?:Event|Evento)\s+\d+', line, re.IGNORECASE):
                        current_event = line.strip()
                        continue
                        
                    if current_event:
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
                            
                            m_prefix = re.match(r'^([\w\-]*)\s+(.+?)\s+(\d+)$', prefix)
                            if m_prefix:
                                rank = m_prefix.group(1)
                                name = m_prefix.group(2) # Surname, Name
                                age = m_prefix.group(3)
                            else:
                                continue # not a swimmer line
                                
                            s_tokens = suffix.split()
                            if len(s_tokens) >= 2:
                                finals_time = s_tokens[1]
                            elif len(s_tokens) == 1:
                                finals_time = s_tokens[0]
                            else:
                                continue
                                
                            if finals_time.upper() in ['DQ', 'NS', 'NT']:
                                finals_time = finals_time.upper()
                            else:
                                # standardize time format from 2:00,46 to 2:00.46
                                finals_time = finals_time.replace(',', '.')
                                
                            results.append({
                                'raw_name': name,
                                'age': age,
                                'raw_event': current_event,
                                'finals_time': finals_time,
                                'rank': rank
                            })
    except Exception as e:
        print(f"Error parsing PDF: {e}")
        
    return results

def parse_puntajes_pdf(pdf_path):
    club_place = None
    teams = sorted(TEAMS_REGEX, key=len, reverse=True)
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for line in text.split('\n'):
                    lower_line = line.lower()
                    if any(t in lower_line for t in teams):
                        m = re.match(r'^\s*(\d+)', line)
                        if m:
                            club_place = m.group(1)
                            return club_place
    except Exception as e:
        print(f"Error parsing Puntajes PDF: {e}")
    return club_place

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def format_fechida_name(raw):
    # "Perez, Juan" -> "Juan Perez"
    if ',' in raw:
        parts = raw.split(',')
        if len(parts) >= 2:
            return f"{parts[1].strip()} {parts[0].strip()}".title()
    return raw.title()

def sync_results_to_db(results, meet_name, meet_date, meet_location=None, meet_pool=None, club_place=None):
    if not results and not club_place: return 0
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Ensure meet exists
    c.execute("SELECT id FROM meets WHERE name = ?", (meet_name,))
    meet_row = c.fetchone()
    if meet_row:
        meet_id = meet_row[0]
        # Actualizar metadatos del torneo existente si es necesario
        if meet_date and meet_date != datetime.now().strftime("%Y-%m-%d"):
            c.execute("UPDATE meets SET date = ? WHERE id = ?", (meet_date, meet_id))
        if meet_location:
            c.execute("UPDATE meets SET location = ? WHERE id = ?", (meet_location, meet_id))
        if meet_pool:
            c.execute("UPDATE meets SET pool_size = ? WHERE id = ?", (meet_pool, meet_id))
        if club_place:
            c.execute("UPDATE meets SET address = ? WHERE id = ?", (str(club_place), meet_id))
    else:
        import random
        meet_id = f"F_{int(time.time())}_{random.randint(100,999)}"
        c.execute("INSERT INTO meets (id, name, date, location, pool_size, address) VALUES (?, ?, ?, ?, ?, ?)",
                  (meet_id, meet_name, meet_date, meet_location, meet_pool, str(club_place) if club_place else None))
    
    # Load swimmers for matching (Now including birth_date)
    c.execute("SELECT id, name, birth_date FROM swimmers")
    swimmers = c.fetchall()
    
    inserts = 0
    for r in results:
        formatted_name = format_fechida_name(r['raw_name'])
        result_age_str = r.get('age', '')
        
        try:
            result_age_val = int(result_age_str)
        except ValueError:
            result_age_val = None
        
        # Match swimmer
        best_match_id = None
        best_score = 0
        for sid, sname, sbirth in swimmers:
            score = similar(formatted_name.lower(), sname.lower())
            
            # If name is a strong match, verify age parity to avoid matching adults with kids
            if score > 0.75:
                age_is_plausible = True
                if result_age_val is not None and sbirth:
                    try:
                        # Calculate approximate age in DB
                        birth_year = pd.to_datetime(sbirth).year
                        current_year = datetime.now().year
                        db_age = current_year - birth_year
                        
                        # Fechida "Master" category ages can be reported weirdly, but if the difference is huge (> 3 years), reject match
                        if abs(db_age - result_age_val) > 3:
                            age_is_plausible = False
                    except:
                        pass # if date parsing fails, give benefit of doubt
                        
                if age_is_plausible and score > best_score:
                    best_score = score
                    best_match_id = sid
                
        if best_match_id and best_score > 0.75:
            db_event = map_event_name(r['raw_event'])
            db_pool = map_pool_size(r['raw_event'])
            
            # Check if this result exists (with same meet and event)
            c.execute("SELECT id FROM results WHERE swimmer_id=? AND event_name=? AND meet_id=?", 
                      (best_match_id, db_event, meet_id))
            exist = c.fetchone()
            
            if exist:
                # OVERWRITE!
                c.execute("UPDATE results SET time=?, pool_size=?, place=? WHERE id=?", 
                          (r['finals_time'], db_pool, r['rank'], exist[0]))
            else:
                c.execute("INSERT INTO results (swimmer_id, meet_id, event_name, time, pool_size, place) VALUES (?, ?, ?, ?, ?, ?)",
                          (best_match_id, meet_id, db_event, r['finals_time'], db_pool, r['rank']))
            inserts += 1
            
    conn.commit()
    conn.close()
    return inserts

def scrape_fechida(log_callback=print):
    init_db()
    ctx = get_ctx()
    
    log_callback("Iniciando acceso a Fechida: /campeonatos-natacion/")
    html = urllib.request.urlopen("https://fechida.cl/campeonatos-natacion/", context=ctx).read()
    soup = BeautifulSoup(html, 'html.parser')
    
    links = [a['href'] for a in soup.find_all('a', href=True) if 'campeonato-info' in a['href']]
    links = list(set(links))
    
    log_callback(f"Encontrados {len(links)} campeonatos. Verificando nuevos...")
    
    total_new = 0
    added_meets = []
    
    for l in links:
        try:
            url_id_match = re.search(r'id=(\d+)', l)
            if not url_id_match: continue
            
            c_id = url_id_match.group(1)
            if is_already_scraped(c_id):
                continue
                
            full_url = f"https://fechida.cl/{l}"
            log_callback(f"Investigando Campeonato ID {c_id}...")
            
            chtml = urllib.request.urlopen(full_url, context=ctx).read()
            csoup = BeautifulSoup(chtml, 'html.parser')
            
            pdf_url = None
            meet_name = f"Campeonato Fechida {c_id}"
            meet_date = datetime.now().strftime("%Y-%m-%d")
            meet_location = None
            meet_pool = None
            
            # Extraer Metadatos (Fecha, Lugar, Piscina)
            try:
                for tag in csoup.find_all('strong'):
                    text = tag.get_text(strip=True)
                    val = tag.next_sibling.strip() if (tag.next_sibling and hasattr(tag.next_sibling, 'strip')) else None
                    if not val:
                        parent_text = tag.parent.get_text(strip=True)
                        val = parent_text.replace(text, "").strip()
                    if not val: continue
                    
                    if "Lugar" in text:
                        meet_location = val
                    elif "Fecha" in text:
                        m = re.search(r'(\d{2})/(\d{2})/(\d{4})', val)
                        if m:
                            meet_date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                    elif "Piscina" in text:
                        meet_pool = f"{val}m" if val.isdigit() else val
            except Exception as e:
                log_callback(f"Error extrayendo metadatos de HTML: {e}")
                
            puntajes_pdf_url = None
            for text_node in csoup.find_all(string=lambda text: text and ('puntaje' in text.lower() or 'consolidado' in text.lower())):
                row = text_node.find_parent('tr')
                if row:
                    a = row.find('a', href=True)
                    if a:
                        puntajes_pdf_url = a['href']
                        break
                        
            for text_node in csoup.find_all(string=lambda text: text and 'resultados completos' in text.lower()):
                row = text_node.find_parent('tr')
                if row:
                    a = row.find('a', href=True)
                    if a:
                        pdf_url = a['href']
                        clean_name = text_node.strip().replace('.pdf', '')
                        if len(clean_name) > 5:
                            meet_name = clean_name.title()
                        break
                        
            if pdf_url or puntajes_pdf_url:
                club_place = None
                if puntajes_pdf_url:
                    full_p_url = f"https://fechida.cl/{puntajes_pdf_url}" if not puntajes_pdf_url.startswith("http") else puntajes_pdf_url
                    log_callback(f"  Encontrado Puntajes PDF: Descargando {full_p_url}...")
                    try:
                        req_p = urllib.request.Request(full_p_url, headers={'User-Agent': 'Mozilla/5.0'})
                        p_pdf_path = f"/tmp/fechida_puntaje_{c_id}.pdf"
                        with urllib.request.urlopen(req_p, context=ctx) as p_resp, open(p_pdf_path, 'wb') as pf:
                            pf.write(p_resp.read())
                        club_place = parse_puntajes_pdf(p_pdf_path)
                        if club_place:
                            log_callback(f"  Lugar del Club encontrado: {club_place}")
                    except Exception as e:
                        log_callback(f"  Error procesando Puntajes PDF: {e}")

                results = []
                if pdf_url:
                    full_pdf_url = f"https://fechida.cl/{pdf_url}" if not pdf_url.startswith("http") else pdf_url
                    log_callback(f"  Encontrado PDF de Resultados: Descargando...")
                    req = urllib.request.Request(full_pdf_url, headers={'User-Agent': 'Mozilla/5.0'})
                    pdf_path = f"/tmp/fechida_{c_id}.pdf"
                    
                    with urllib.request.urlopen(req, context=ctx) as response, open(pdf_path, 'wb') as out_file:
                        out_file.write(response.read())
                        
                    log_callback(f"  Parseando PDF para {meet_name}...")
                    results = parse_pdf(pdf_path, meet_name)
                
                if results or club_place:
                    log_callback(f"  Encontrados {len(results)} resultados de Peñalolén. Guardando en DB...")
                    inserted = sync_results_to_db(results, meet_name, meet_date, meet_location, meet_pool, club_place)
                    log_callback(f"  -> {inserted} registros actualizados/insertados.")
                    if inserted > 0 or club_place:
                        added_meets.append(meet_name)
                    total_new += inserted
                else:
                    log_callback("  No se encontraron resultados de Peñalolén en este torneo.")
                    
                # Mark scraped regardless if penalized or not
                mark_as_scraped(c_id)
            else:
                # No complete results yet, don't mark as scraped so we can check later
                log_callback("  Aún no publican 'Resultados Completos'.")
                
        except Exception as e:
            log_callback(f"Error procesando {l}: {e}")
            
    log_callback(f"Proceso finalizado. {total_new} nuevos resultados integrados.")
    
    # Get the last registered meet
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM meets ORDER BY date DESC LIMIT 1")
    row = c.fetchone()
    last_meet = row[0] if row else "Ninguna"
    conn.close()
    
    return {
        "total_new": total_new,
        "added_meets": added_meets,
        "last_meet": last_meet
    }

if __name__ == "__main__":
    scrape_fechida()
