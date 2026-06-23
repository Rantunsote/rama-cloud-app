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
import itertools
import html

def map_points(place):
    try:
        p = int(place)
        if p == 1: return 9
        if p == 2: return 7
        if p == 3: return 6
        if p == 4: return 5
        if p == 5: return 4
        if p == 6: return 3
        if p == 7: return 2
        if p == 8: return 1
    except:
        pass
    return 0

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

st.set_page_config(page_title="RamaCloud", page_icon="🏊", layout="wide", initial_sidebar_state="collapsed")

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
    "25 Free": "25 metros Libre",
    "25 Back": "25 metros Espalda",
    "25 Breast": "25 metros Pecho",
    "25 Fly": "25 metros Mariposa",
    "25 IM": "25 metros Combinado",
    "100 Free Relay": "4 x 25 metros Libre",
    "100 Medley Relay": "4 x 25 metros Combinado",
    "4x50 Free Relay": "4 x 50 metros Libre",
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
    "25 metros Libre": "25 Free",
    "25 metros Espalda": "25 Back",
    "25 metros Pecho": "25 Breast",
    "25 metros Mariposa": "25 Fly",
    "25 metros Combinado": "25 IM",
    "4 x 25 metros Libre": "100 Free Relay",
    "4 x 25 metros Combinado": "100 Medley Relay",
}

EVENT_ES_TO_DB_MULTI = {
    "4 x 100 metros Mixto (Libre o Combinado)": ["400 Mixed Free Relay", "400 Mixed Medley Relay"],
    "4 x 50 metros Libre": ["200 Free Relay", "4x50 Free Relay"],
}

def get_event_display_name(event_name):
    if not isinstance(event_name, str):
        return event_name
    en = event_name.strip()
    if en in EVENT_DB_TO_ES:
        return EVENT_DB_TO_ES[en]
        
    # If not exactly matching, try normalizing it first (e.g. "Mujeres 9-10 400 Metro Libre" -> "400 Free")
    norm = normalize_scraped_event_name(en)
    if norm in EVENT_DB_TO_ES:
        return EVENT_DB_TO_ES[norm]
        
    return norm

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
    :root {
        --rama-navy: #0f172a;
        --rama-blue: #0284c7;
        --rama-sky: #0ea5e9;
        --rama-teal: #14b8a6;
        --rama-gold: #f59e0b;
        --rama-bg: #f8fafc;
        --rama-card: #ffffff;
        --rama-muted: #64748b;
        --rama-border: #e2e8f0;
        --rama-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
    }

    html, body, [data-testid="stApp"] {
        background:
            radial-gradient(circle at top left, rgba(14, 165, 233, 0.14), transparent 34rem),
            linear-gradient(180deg, #ffffff 0%, var(--rama-bg) 42%, #eef6fb 100%) !important;
        color: #0f172a !important;
    }

    h1, h2, h3, h4 {
        letter-spacing: -0.03em;
        color: var(--rama-navy) !important;
    }

    p, .stMarkdown, .stText {
        color: #1e293b;
    }

    header[data-testid="stHeader"],
    div[data-testid="stToolbar"],
    #MainMenu,
    footer {
        display: none !important;
        height: 0 !important;
    }

    .block-container {
        padding-top: 0;
        padding-bottom: 2.5rem;
        max-width: 1320px;
    }

    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.86);
        border: 1px solid rgba(226, 232, 240, 0.9);
        border-radius: 18px;
        padding: 1rem 1.1rem;
        box-shadow: var(--rama-shadow);
    }

    div[data-testid="stMetricLabel"] p {
        color: var(--rama-muted) !important;
        font-size: 0.78rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        font-weight: 700;
    }

    div[data-testid="stMetricValue"] {
        color: var(--rama-navy) !important;
        font-weight: 850;
    }

    .rama-card {
        background: rgba(255, 255, 255, 0.9);
        border: 1px solid rgba(226, 232, 240, 0.95);
        border-radius: 22px;
        padding: 1.25rem;
        box-shadow: var(--rama-shadow);
    }

    .rama-section-title {
        font-size: 1.2rem;
        font-weight: 850;
        margin: 0.5rem 0 1rem 0;
        color: var(--rama-navy);
    }

    .rama-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.38rem 0.7rem;
        border-radius: 999px;
        background: #e0f2fe;
        color: #075985;
        border: 1px solid #bae6fd;
        font-size: 0.8rem;
        font-weight: 750;
    }

    .rama-muted {
        color: var(--rama-muted);
        font-size: 0.92rem;
    }

    .rama-main-nav-spacer {
        height: 4.4rem;
    }

    .st-key-rama_nav_label {
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        z-index: 100000;
        width: 100vw !important;
        padding: 0.65rem max(0.65rem, calc((100vw - 1320px) / 2)) 0.6rem max(0.65rem, calc((100vw - 1320px) / 2));
        background: rgba(248, 250, 252, 0.97);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border-bottom: 1px solid rgba(203, 213, 225, 0.9);
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
        overflow-x: auto;
    }

    .st-key-rama_nav_label [data-testid="stButtonGroup"] {
        width: min(1320px, calc(100vw - 1.25rem)) !important;
        margin: 0 auto !important;
    }

    .st-key-rama_nav_label [data-testid="stButtonGroup"] > div {
        flex-wrap: nowrap !important;
        overflow-x: auto !important;
        scrollbar-width: none;
    }

    .st-key-rama_nav_label [data-testid="stButtonGroup"] > div::-webkit-scrollbar {
        display: none;
    }

    .st-key-rama_nav_label button[data-testid*="segmented_control"] {
        white-space: nowrap !important;
    }

    .swimmer-profile-card {
        background: linear-gradient(135deg, #0f172a 0%, #075985 58%, #0f766e 100%);
        padding: 1.35rem 1.5rem;
        border-radius: 24px;
        color: white;
        margin-bottom: 1.35rem;
        display: grid;
        grid-template-columns: 88px minmax(0, 1fr);
        align-items: center;
        gap: 1.25rem;
        box-shadow: 0 18px 42px rgba(15, 23, 42, 0.18);
        border: 1px solid rgba(255, 255, 255, 0.16);
    }

    .swimmer-profile-avatar {
        width: 88px;
        height: 88px;
        background: rgba(255,255,255,0.14);
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 2rem;
        border: 2px solid rgba(255,255,255,0.28);
        box-shadow: inset 0 0 24px rgba(255,255,255,0.08);
    }

    .swimmer-profile-content {
        min-width: 0;
    }

    .swimmer-profile-badge {
        display: inline-flex;
        padding: 0.28rem 0.62rem;
        border-radius: 999px;
        background: rgba(255,255,255,0.14);
        border: 1px solid rgba(255,255,255,0.18);
        color: white;
        font-weight: 800;
        font-size: 0.68rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
    }

    .swimmer-profile-title {
        margin: 0.45rem 0 0 0;
        color: white !important;
        font-size: clamp(1.9rem, 3.5vw, 2.65rem);
        font-weight: 900;
        letter-spacing: -0.04em;
        line-height: 1.05;
    }

    .swimmer-profile-meta {
        opacity: 0.86;
        margin: 0.3rem 0 0 0;
        font-size: 0.95rem;
        color: #e0f2fe !important;
    }

    .swimmer-profile-stats {
        display: flex;
        gap: 0.65rem;
        margin-top: 0.85rem;
        flex-wrap: wrap;
    }

    .swimmer-profile-stat {
        background: rgba(255,255,255,0.13);
        padding: 0.5rem 0.85rem;
        border-radius: 15px;
        border: 1px solid rgba(255,255,255,0.22);
        min-width: 98px;
    }

    .swimmer-profile-stat small {
        text-transform: uppercase;
        font-size: 0.62rem;
        letter-spacing: 0.04em;
        opacity: 0.78;
        color: #e0f2fe;
    }

    .swimmer-profile-stat strong {
        display: block;
        margin-top: 0.15rem;
        font-size: 1rem;
        line-height: 1.1;
    }

    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            padding-bottom: 1.75rem !important;
        }

        .rama-main-nav-spacer {
            height: 3.35rem;
        }

        .st-key-rama_nav_label {
            padding: 0.42rem 0.55rem 0.4rem 0.55rem !important;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        }

        .st-key-rama_nav_label [data-testid="stButtonGroup"] {
            width: 100% !important;
        }

        .st-key-rama_nav_label [data-testid="stButtonGroup"] > div {
            gap: 0.25rem !important;
            padding-bottom: 0.1rem;
        }

        .st-key-rama_nav_label button[data-testid*="segmented_control"] {
            min-width: max-content !important;
            min-height: 2.15rem !important;
            padding: 0.35rem 0.58rem !important;
            border-radius: 14px !important;
            font-size: 0.82rem !important;
        }

        .rama-card {
            border-radius: 18px;
            padding: 1rem;
        }

        .rama-section-title {
            font-size: 1.05rem;
        }

        .rama-muted {
            font-size: 0.86rem;
        }

        div[data-testid="stMetric"] {
            border-radius: 16px;
            padding: 0.85rem 0.95rem;
        }

        h2 {
            font-size: 1.7rem !important;
            line-height: 1.15 !important;
        }

        .hero-container {
            min-height: 235px !important;
            border-radius: 20px !important;
            padding: 1rem !important;
            margin-top: 0.15rem !important;
            background-position: center !important;
        }

        .hero-title {
            font-size: clamp(1.85rem, 9.5vw, 2.45rem) !important;
            line-height: 1.05 !important;
        }

        .hero-subtitle {
            font-size: 0.95rem !important;
            gap: 0.45rem !important;
        }

        .coach-tag {
            padding: 6px 10px !important;
            font-size: 0.82rem !important;
        }

        .swimmer-profile-card {
            padding: 0.85rem;
            border-radius: 18px;
            grid-template-columns: 54px minmax(0, 1fr);
            gap: 0.75rem;
            margin-bottom: 1rem;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.16);
        }

        .swimmer-profile-avatar {
            width: 54px;
            height: 54px;
            font-size: 1.35rem;
            border-width: 2px;
        }

        .swimmer-profile-badge {
            padding: 0.2rem 0.48rem;
            font-size: 0.55rem;
            letter-spacing: 0.08em;
        }

        .swimmer-profile-title {
            margin-top: 0.32rem;
            font-size: clamp(1.45rem, 7.5vw, 2rem) !important;
            line-height: 1.02;
        }

        .swimmer-profile-meta {
            margin-top: 0.2rem;
            font-size: 0.78rem;
            line-height: 1.25;
        }

        .swimmer-profile-stats {
            gap: 0.45rem;
            margin-top: 0.6rem;
        }

        .swimmer-profile-stat {
            flex: 1 1 calc(50% - 0.45rem);
            min-width: 0;
            padding: 0.42rem 0.58rem;
            border-radius: 13px;
        }

        .swimmer-profile-stat small {
            font-size: 0.54rem;
        }

        .swimmer-profile-stat strong {
            font-size: 0.88rem;
        }
    }

    @media (max-width: 420px) {
        .rama-main-nav-spacer {
            height: 3.1rem;
        }

        .st-key-rama_nav_label button[data-testid*="segmented_control"] {
            min-height: 2rem !important;
            padding: 0.3rem 0.48rem !important;
            font-size: 0.78rem !important;
        }

        .hero-container {
            min-height: 205px !important;
            border-radius: 18px !important;
        }

        .hero-title {
            font-size: clamp(1.65rem, 10vw, 2.05rem) !important;
        }

        .hero-subtitle {
            font-size: 0.86rem !important;
        }

        .coach-tag {
            padding: 5px 9px !important;
            font-size: 0.76rem !important;
        }

        .swimmer-profile-card {
            grid-template-columns: 46px minmax(0, 1fr);
            padding: 0.72rem;
            border-radius: 16px;
            gap: 0.62rem;
        }

        .swimmer-profile-avatar {
            width: 46px;
            height: 46px;
            font-size: 1.15rem;
        }

        .swimmer-profile-title {
            font-size: clamp(1.28rem, 7.6vw, 1.72rem) !important;
        }

        .swimmer-profile-meta {
            font-size: 0.72rem;
        }

        .swimmer-profile-stat {
            padding: 0.38rem 0.5rem;
        }

        .swimmer-profile-stat strong {
            font-size: 0.82rem;
        }
    }

    div[data-testid="stTabs"] button {
        border-radius: 999px !important;
        padding: 0.5rem 0.9rem !important;
        color: #334155 !important;
        font-weight: 750 !important;
        border-bottom: 0 !important;
    }

    div[data-testid="stTabs"] button[aria-selected="true"] {
        background: linear-gradient(135deg, var(--rama-blue), var(--rama-teal)) !important;
        color: white !important;
        box-shadow: 0 12px 24px rgba(2, 132, 199, 0.18);
    }

    div[data-testid="stDataFrame"],
    div[data-testid="stDataEditor"] {
        border-radius: 18px;
        overflow: hidden;
        border: 1px solid var(--rama-border);
        box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
    }

    .stButton > button {
        border-radius: 999px;
        border: 1px solid #bae6fd;
        background: #ffffff;
        color: var(--rama-blue);
        font-weight: 750;
    }

    .stButton > button:hover {
        border-color: var(--rama-blue);
        color: var(--rama-navy);
        box-shadow: 0 10px 24px rgba(14, 165, 233, 0.14);
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
    div[data-testid="stTextInput"] input {
        border-radius: 999px;
        border-color: var(--rama-border);
    }

    /* HIDE SIDEBAR COMPLETELY */
    section[data-testid="stSidebar"] {
        display: none;
    }
    div[data-testid="stSidebarCollapsedControl"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

def esc(value):
    return html.escape("" if value is None else str(value))

def render_kpi_card(label, value, helper="", accent="#0ea5e9"):
    st.markdown(html_block(f"""
    <div class="rama-card" style="min-height: 132px; border-top: 4px solid {accent};">
        <div style="color:#64748b; font-size:0.78rem; font-weight:800; text-transform:uppercase; letter-spacing:0.06em;">{esc(label)}</div>
        <div style="color:#0f172a; font-size:2.05rem; font-weight:900; line-height:1.05; margin-top:0.45rem;">{esc(value)}</div>
        <div class="rama-muted" style="margin-top:0.5rem;">{esc(helper)}</div>
    </div>
    """), unsafe_allow_html=True)

def render_meet_card(name, date, pool_size, count=None, accent="#0ea5e9"):
    count_text = f" • {count} resultados" if count is not None else ""
    st.markdown(html_block(f"""
    <div class="rama-card" style="margin-bottom:0.85rem; padding:1rem 1.1rem; border-left:5px solid {accent};">
        <div style="font-weight:850; color:#0f172a; font-size:1rem;">{esc(name)}</div>
        <div class="rama-muted" style="margin-top:0.3rem;">📅 {esc(date)} • 📏 {esc(pool_size or 'N/A')}{esc(count_text)}</div>
    </div>
    """), unsafe_allow_html=True)

def render_swimmer_card(row):
    gender = row.get("gender") or "?"
    gender_label = {"M": "Varón", "F": "Dama"}.get(str(gender).upper(), str(gender))
    st.markdown(html_block(f"""
    <div class="rama-card" style="height:100%; min-height:124px;">
        <div style="display:flex; align-items:center; gap:0.8rem;">
            <div style="width:42px; height:42px; border-radius:50%; background:linear-gradient(135deg,#0ea5e9,#14b8a6); display:flex; align-items:center; justify-content:center; color:white; font-size:1.2rem;">🏊</div>
            <div>
                <div style="font-weight:850; color:#0f172a; line-height:1.15;">{esc(row.get('name'))}</div>
                <div class="rama-muted">{esc(row.get('Categoría', 'Desconocida'))} • {esc(gender_label)}</div>
            </div>
        </div>
    </div>
    """), unsafe_allow_html=True)

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

    return df

def load_minimas():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM minimum_standards", conn)
    conn.close()
    return df

def calculate_category(dob):
    if pd.isnull(dob): return "Desconocida"
    try:
        today = datetime.now()
        # Biological Age: Age on Dec 31st of current year
        age = today.year - dob.year
        
        if age <= 10: return "Infantil A (y menor)"
        if age == 11 or age == 12: return "Infantil B"
        if age == 13 or age == 14: return "Juvenil A"
        if age >= 15 and age <= 17: return "Juvenil B"
        if age >= 18: return "Todo Competidor"
        return "Desconocida"
    except: return "Desconocida"

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
    
    # Parse dates
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    df['category'] = df['birth_date'].apply(calculate_category)
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
            r.event_name, r.time, r.pool_size, r.points, r.place,
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
    
    # Calculate chronological years passed
    year_first = yearly_best.index[0]
    year_last = yearly_best.index[-1]
    years = year_last - year_first
    
    if years <= 0:
        return None
        
    try:
        # standard CAGR: (EV/BV)^(1/n) - 1. But since a lower time is better,
        # we calculate the rate of decay: rate = 1 - (last / first)^(1/n)
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

def update_meet_info(meet_id, new_name, new_date):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE meets SET name = ?, date = ? WHERE id = ?", (new_name, new_date, meet_id))
        conn.commit()
    except Exception as e:
        st.error(f"Error updating meet info: {e}")
    finally:
        conn.close()

def update_meet_address(meet_id, new_address):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE meets SET address = ? WHERE id = ?", (new_address, meet_id))
        conn.commit()
    except Exception as e:
        st.error(f"Error updating meet address: {e}")
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
    
    def render_home_hero():
        if os.path.exists(bg_image_file):
            img_b64 = get_img_as_base64(bg_image_file)

            st.markdown(html_block(f"""
            <style>
                .hero-container {{
                    background-image: linear-gradient(135deg, rgba(15, 23, 42, 0.55), rgba(2, 132, 199, 0.38), rgba(15, 23, 42, 0.78)), url("data:image/png;base64,{img_b64}");
                    background-size: cover;
                    background-position: center;
                    min-height: 340px;
                    border-radius: 28px;
                    display: flex;
                    align-items: center; /* Vertically center the content */
                    justify-content: center; /* Center content horizontally */
                    padding: 2rem;
                    position: relative;
                    color: white;
                    box-shadow: 0 24px 60px rgba(15, 23, 42, 0.22);
                    text-align: center;
                    overflow: hidden;
                    border: 1px solid rgba(255, 255, 255, 0.24);
                    margin-top: 0.75rem;
                }}
                .hero-container::after {{
                    content: "";
                    position: absolute;
                    inset: auto -10% -28% -10%;
                    height: 55%;
                    background: radial-gradient(ellipse at center, rgba(14,165,233,0.35), transparent 65%);
                }}
                .hero-content {{
                    z-index: 2;
                    max-width: 820px;
                }}
                .hero-title {{
                    font-size: clamp(2.4rem, 5vw, 4.6rem);
                    font-weight: 900;
                    margin: 0;
                    text-shadow: 0 14px 38px rgba(0,0,0,0.5);
                    line-height: 1.1;
                    color: #ffffff !important;
                    letter-spacing: -0.04em;
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
                    background-color: rgba(255, 255, 255, 0.16);
                    padding: 7px 14px;
                    border-radius: 999px;
                    backdrop-filter: blur(10px);
                    font-size: 1rem;
                    border: 1px solid rgba(255, 255, 255, 0.24);
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
                            <span class="coach-tag">Coach Menores: Valeria Contalba</span>
                        </div>
                    </div>
                </div>
            </div>
            """), unsafe_allow_html=True)
        else:
            # Fallback if image missing: Simple blue header, NO LOGO
            st.markdown(html_block("""
            <div style="background-color: #0f172a; padding: 2rem; border-radius: 15px; text-align: center; color: white; margin: 0.75rem 0 2rem 0;">
                <h1 style="margin:0; font-size: 3rem;">Rama de Natación Peñalolén</h1>
                <div style="margin-top: 1rem; opacity: 0.9;">
                    <div>Santiago, Chile</div>
                    <div style="margin-top: 5px;"><strong>Head Coach:</strong> Cesar Cereceda</div>
                    <div><strong>Infantiles:</strong> Oscar Cifuentes • <strong>Menores:</strong> Valeria Contalba</div>
                </div>
            </div>
            """), unsafe_allow_html=True)
    
    nav_items = [
        ("inicio", "🏠 Inicio"),
        ("plantel", "🏊 Plantel"),
        ("torneos", "🏆 Torneos"),
        ("puntajes", "🥇 Puntajes"),
        ("analisis", "📊 Análisis"),
        ("estadisticas", "📈 Estadisticas"),
        ("clasificados", "🏅 Clasificados"),
        ("relevos", "🏊 Relevos"),
    ]
    is_admin = st.session_state.get("logged_user") == "admin"
    if is_admin:
        nav_items.append(("ingreso", "📝 Ingreso"))

    valid_sections = {key for key, _ in nav_items}
    labels_by_key = dict(nav_items)
    keys_by_label = {label: key for key, label in nav_items}
    if st.session_state.get("rama_nav_section") not in valid_sections:
        st.session_state["rama_nav_section"] = "inicio"

    current_label = labels_by_key[st.session_state["rama_nav_section"]]
    nav_labels = [label for _, label in nav_items]
    if st.session_state.get("rama_nav_label") not in nav_labels:
        st.session_state["rama_nav_label"] = current_label

    if hasattr(st, "segmented_control"):
        selected_label = st.segmented_control(
            "Menú principal",
            options=nav_labels,
            key="rama_nav_label",
            label_visibility="collapsed",
        )
    else:
        selected_label = st.radio(
            "Menú principal",
            options=nav_labels,
            key="rama_nav_label",
            horizontal=True,
            label_visibility="collapsed",
        )

    selected_section = keys_by_label.get(selected_label, "inicio")
    st.session_state["rama_nav_section"] = selected_section
    st.markdown('<div class="rama-main-nav-spacer"></div>', unsafe_allow_html=True)

    if selected_section == "inicio":
        render_home_hero()
        st.markdown('<div style="height:1.25rem;"></div>', unsafe_allow_html=True)

        meets_df = load_meets()
        if not meets_df.empty:
            meets_df['_sort_date'] = pd.to_datetime(meets_df['date'], errors='coerce')
            meets_df = meets_df.sort_values(by='_sort_date', ascending=False)

        all_results = load_all_results()
        total_results = len(all_results) if not all_results.empty else 0
        latest_meet = meets_df.iloc[0] if not meets_df.empty else None
        latest_meet_name = latest_meet['name'] if latest_meet is not None else "Sin registros"
        latest_meet_date = latest_meet['date'] if latest_meet is not None else "—"

        st.markdown(html_block("""
        <div style="margin:0.25rem 0 1.25rem 0;">
            <div class="rama-pill">🏊 Panel deportivo</div>
            <h2 style="margin:0.65rem 0 0.25rem 0;">Resumen de la Rama</h2>
            <div class="rama-muted">Vista rápida del plantel, competencias y volumen de resultados registrados.</div>
        </div>
        """), unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_kpi_card("Nadadores activos", len(swimmers_df), "Plantel visible en la app", "#0ea5e9")
        with c2:
            render_kpi_card("Torneos", len(meets_df), "Competencias registradas", "#14b8a6")
        with c3:
            render_kpi_card("Resultados", total_results, "Tiempos individuales", "#f59e0b")
        with c4:
            render_kpi_card("Última carga", latest_meet_date, latest_meet_name, "#6366f1")

        st.markdown('<div style="height:1.1rem;"></div>', unsafe_allow_html=True)

        left_col, right_col = st.columns([1.35, 0.95])
        with left_col:
            st.markdown('<div class="rama-section-title">Competencias recientes</div>', unsafe_allow_html=True)
            if not meets_df.empty:
                result_counts = {}
                if not all_results.empty and 'meet_name' in all_results.columns:
                    result_counts = all_results.groupby('meet_name').size().to_dict()
                for i in range(min(6, len(meets_df))):
                    meet = meets_df.iloc[i]
                    render_meet_card(
                        meet.get('name', ''),
                        meet.get('date', ''),
                        meet.get('pool_size', 'N/A'),
                        result_counts.get(meet.get('name')),
                        accent="#0ea5e9" if i == 0 else "#cbd5e1"
                    )
            else:
                st.info("No hay competencias registradas.")

        with right_col:
            st.markdown('<div class="rama-section-title">Distribución del plantel</div>', unsafe_allow_html=True)
            cat_counts = swimmers_df['Categoría'].value_counts().head(6) if 'Categoría' in swimmers_df.columns else pd.Series(dtype=int)
            if not cat_counts.empty:
                for category, count in cat_counts.items():
                    st.markdown(html_block(f"""
                    <div class="rama-card" style="margin-bottom:0.75rem; padding:0.85rem 1rem;">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-weight:800; color:#0f172a;">{esc(category)}</span>
                            <span class="rama-pill">{int(count)} nadadores</span>
                        </div>
                    </div>
                    """), unsafe_allow_html=True)
            else:
                st.info("No hay categorías disponibles.")

    if selected_section == "plantel":
        st.markdown(html_block("""
        <div style="margin-bottom:1rem;">
            <div class="rama-pill">🏊 Plantel</div>
            <h2 style="margin:0.65rem 0 0.25rem 0;">Nadadores</h2>
            <div class="rama-muted">Busca, filtra y entra al perfil individual de cada nadador.</div>
        </div>
        """), unsafe_allow_html=True)

        f1, f2, f3 = st.columns([2, 1, 1])
        with f1:
            search_query = st.text_input("Buscar Nadador...", "", placeholder="Nombre o apellido")
        with f2:
            category_options = ["Todas"] + sorted([str(x) for x in swimmers_df['Categoría'].dropna().unique()])
            selected_category = st.selectbox("Categoría", category_options)
        with f3:
            gender_options = ["Todos"] + sorted([str(x) for x in swimmers_df['gender'].dropna().unique()])
            selected_gender = st.selectbox("Género", gender_options)

        filtered = swimmers_df.copy()
        if search_query:
            filtered = filtered[filtered['name'].str.lower().str.contains(search_query.lower(), na=False)]
        if selected_category != "Todas":
            filtered = filtered[filtered['Categoría'].astype(str) == selected_category]
        if selected_gender != "Todos":
            filtered = filtered[filtered['gender'].astype(str) == selected_gender]

        st.caption(f"{len(filtered)} nadadores encontrados")

        preview_df = filtered.head(12)
        if not preview_df.empty:
            card_cols = st.columns(3)
            for idx, (_, row) in enumerate(preview_df.iterrows()):
                with card_cols[idx % 3]:
                    render_swimmer_card(row)
                    if st.button("Ver perfil", key=f"swimmer_card_{row['id']}"):
                        go_to_swimmer(row['id'])
                        st.rerun()
        else:
            st.info("No hay nadadores que coincidan con los filtros.")

        st.markdown('<div class="rama-section-title">Tabla completa</div>', unsafe_allow_html=True)

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

    if selected_section == "torneos":
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
            "_sort_date": None,
            "name": "Nombre",
            "date": "Fecha",
            "location": "Ciudad",
            "address": "Lugar Club",
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
            disabled=["location"], # Allowed name, date, pool_size, address
            use_container_width=True,
            hide_index=True,
            key="meets_editor"
        )
        
        # Detect Changes
        if not edited_df.equals(editor_df):
            for i in editor_df.index:
                old_row = editor_df.loc[i]
                new_row = edited_df.loc[i]
                m_id = old_row['id'] # ID is hidden but present
                
                changed = []
                if old_row['pool_size'] != new_row['pool_size']:
                    update_meet_pool_size(m_id, new_row['pool_size'])
                    changed.append("Piscina")
                if old_row['name'] != new_row['name'] or old_row['date'] != new_row['date']:
                    update_meet_info(m_id, new_row['name'], new_row['date'])
                    changed.append("Info")
                if old_row['address'] != new_row['address']:
                    update_meet_address(m_id, new_row['address'])
                    changed.append("Posición")
                    
                if changed:
                    st.toast(f"Torneo actualizado!", icon="✅")
                    time.sleep(1) # Visual feedback
                    st.rerun()

    if selected_section == "analisis":
        render_analysis_tab(swimmers_df)

    if selected_section == "estadisticas":
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
                            "Delta %": f"{(delta / sum_50s) * 100:.2f}%",
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



    if selected_section == "clasificados":
        render_qualifiers_tab(swimmers_df)
        
    if selected_section == "relevos":
        render_relay_builder()
        
    if selected_section == "puntajes":
        st.subheader("🥇 Ranking de Puntajes por Nadador")
        st.markdown("Tabla calculada automáticamente por el sistema basándose en la posición (`Lugar`) final. \n *(1º=9pts, 2º=7pts, 3º=6pts, 4º=5pts, 5º=4pts, 6º=3pts, 7º=2pts, 8º=1pt)*")
        
        all_res = load_all_results()
        if all_res.empty:
            st.info("No hay resultados registrados.")
        else:
            # Calcular puntos para cada resultado
            all_res['puntos_calc'] = all_res['place'].apply(map_points)
            
            # Obtener lista de torneos ordenados de más reciente a más antiguo
            meets_ordered = all_res.drop_duplicates('meet_name').sort_values('date_obj', ascending=False)['meet_name'].tolist()
            meets_ordered = [m for m in meets_ordered if pd.notna(m)]
            
            sel_meet = st.selectbox("🏆 Seleccionar Competencia", meets_ordered, key="scores_meet_filter")
            
            df_filtered = all_res[all_res['meet_name'] == sel_meet]
            
            # Agrupar por nadador
            ranking = df_filtered.groupby(['name', 'gender'])['puntos_calc'].sum().reset_index()
            ranking = ranking[ranking['puntos_calc'] > 0].sort_values('puntos_calc', ascending=False)
            
            if ranking.empty:
                st.warning("No hay nadadores con puntos (1º a 8º lugar) en esta selección.")
            else:
                total_team = ranking['puntos_calc'].sum()
                st.metric(f"Total Peñalolén ({sel_meet})", int(total_team))
                
                # Tabular presentation
                ranking = ranking.reset_index(drop=True)
                ranking.index += 1
                ranking.rename(columns={'name': 'Nadador', 'gender': 'Género', 'puntos_calc': 'Puntos Aportados'}, inplace=True)
                
                st.dataframe(ranking, use_container_width=True)
        
    if selected_section == "ingreso":
        with st.container():
            st.subheader("📝 Ingreso y Actualización de Datos")
            
            t_dob, t_fechida, t_logs, t_other = st.tabs(["Actualizar Cumpleaños (Masivo)", "Sincronizar Global", "Registros de Acceso", "Otros"])
            
            with t_fechida:
                st.markdown("### Sincronización Automática Global")
                st.info("Esta acción buscará nuevos resultados en la página oficial de Fechida, luego escaneará la plataforma internacional Swimcloud buscando competencias faltantes del plantel, y finalmente correrá el algoritmo deduplicador inteligente.")
                if st.button("Sincronizar", type="primary"):
                    with st.spinner("Conectando con Fechida y procesando PDFs..."):
                        try:
                            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                            if root_dir not in sys.path:
                                sys.path.insert(0, root_dir)
                            
                            class LogCapture:
                                def __init__(self):
                                    self.logs = []
                                def __call__(self, msg):
                                    self.logs.append(msg)
                                    print(msg)
                            
                            logger = LogCapture()
                            
                            import scraper_fechida_pdf
                            import importlib
                            importlib.reload(scraper_fechida_pdf)
                            resultado_f = scraper_fechida_pdf.scrape_fechida(log_callback=logger)
                            
                            # 2. Swimcloud Sync via Subprocess
                            logger("\\n🕒 Iniciando recolección internacional en Swimcloud (aislado)...")
                            import subprocess
                            try:
                                # Run main_swimcloud as isolated process to avoid Streamlit cache, using the verified venv
                                proc = subprocess.run(
                                    [sys.executable, "main_swimcloud.py"],
                                    cwd=root_dir,
                                    capture_output=True,
                                    text=True,
                                    check=True
                                )
                                logger("✅ Escaneo de Swimcloud finalizado satisfactoriamente.")
                                # Add the subprocess output to logs for debugging (last 1500 chars)
                                if proc.stdout:
                                    logger(f"(Log Swimcloud:\\n{proc.stdout[-1500:]})")
                            except subprocess.CalledProcessError as e:
                                logger(f"❌ Error en Swimcloud Sync:\\n{e.stderr[-1000:] if e.stderr else 'Error desconocido'}")
                                raise Exception(f"Swimcloud Scraper Error: {e}")
                            
                            # 3. Deduplicador Inteligente
                            logger("\\n🧹 Ejecutando Deduplicación Inteligente de Base de Datos...")
                            import auto_deduplicate
                            import importlib
                            importlib.reload(auto_deduplicate)
                            resultado_d = auto_deduplicate.run_deduplicator()
                            logger(f"✅ Deduplicación lista: {resultado_d.get('meets_merged')} torneos fusionados y {resultado_d.get('results_deleted')} tiempos idénticos purgados.")
                            
                            nuevos = resultado_f.get("total_new", 0)
                            added_meets = resultado_f.get("added_meets", [])
                            
                            st.success(f"¡Sincronización Total Completada!")
                            if added_meets:
                                st.write(f"- Competencias Fechida agregadas: **{', '.join(added_meets)}**")
                            st.write(f"- Competencias consolidadas: **{resultado_d.get('meets_merged')}**")
                            
                            with st.expander("Ver bitácora de sincronización global"):
                                st.text('\\n'.join(logger.logs))
                        except Exception as e:
                            import traceback
                            st.error("Error crítico detallado:")
                            st.code(traceback.format_exc())

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
                st.markdown("### Mantenimiento de Base de Datos")
                st.info("Esta acción escanea toda la base de datos buscando resultados duplicados donde el nadador, la prueba y el tiempo sean idénticos, para luego fusionar los torneos redundantes (priorizando los nombres oficiales de Fechida).")
                if st.button("🧹 Deduplicar Torneos y Resultados", type="primary"):
                    with st.spinner("Analizando entropía en la tabla de resultados..."):
                        try:
                            import auto_deduplicate
                            result = auto_deduplicate.run_deduplicator()
                            
                            st.success(f"¡Limpieza finalizada!")
                            st.write(f"- Torneos redundantes fusionados: **{result.get('meets_merged', 0)}**")
                            st.write(f"- Filas duplicadas idénticas eliminadas: **{result.get('results_deleted', 0)}**")
                            
                            time.sleep(2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error durante la limpieza: {e}")

def render_profile_view(swimmer_id, swimmers_df):
    # Get Swimmer Data
    swimmer = swimmers_df[swimmers_df['id'] == swimmer_id].iloc[0]
    name = swimmer['name']
    
    # Navbar
    if st.button("⬅️ Volver al Roster"):
        go_to_home()
        st.rerun()
        
    # --- HEADER SECTION ---
    birth_raw = swimmer.get('birth_date', 'N/A')
    birth_display = "N/A"
    if pd.notna(birth_raw) and str(birth_raw).strip() not in ("", "None", "NaT"):
        birth_dt = pd.to_datetime(birth_raw, errors='coerce')
        birth_display = birth_dt.strftime("%Y-%m-%d") if pd.notna(birth_dt) else str(birth_raw).split()[0]

    st.markdown(html_block(f"""
    <div class="swimmer-profile-card">
        <div class="swimmer-profile-avatar">🏊</div>
        <div class="swimmer-profile-content">
            <div class="swimmer-profile-badge">Perfil de nadador</div>
            <h1 class="swimmer-profile-title">{esc(name)}</h1>
            <p class="swimmer-profile-meta">Rama de Natación Peñalolén • ID: {esc(swimmer_id)}</p>
            <div class="swimmer-profile-stats">
                <div class="swimmer-profile-stat">
                    <small>Categoría</small>
                    <strong>{esc(calculate_category(swimmer.get('birth_date')))}</strong>
                </div>
                <div class="swimmer-profile-stat">
                    <small>Género</small>
                    <strong>{esc(swimmer.get('gender', '?'))}</strong>
                </div>
                <div class="swimmer-profile-stat">
                    <small>Nacimiento</small>
                    <strong>{esc(birth_display)}</strong>
                </div>
            </div>
        </div>
    </div>
    """), unsafe_allow_html=True)
    
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
                         today = datetime.now()
                         # Proper age calculation: subtract 1 if birthday hasn't occurred yet this year
                         current_age = today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
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
                     .sort_values(by='date_str', ascending=False)
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
            st.session_state["logged_user"] = st.session_state["username"]
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

# --- RELAY BUILDER ---
def render_relay_builder():
    st.subheader("🏊 Armado de Relevos")
    st.markdown("Herramienta para proyectar los relevos más rápidos según **Mejores Tiempos Históricos**.")
    
    # Load Data
    with st.spinner("Cargando tiempos..."):
        df = load_all_best_times()
    
    if df.empty:
        st.warning("No hay tiempos registrados en el sistema.")
        return

    # --- FILTERS ---
    c1, c2 = st.columns([2, 1])
    
    # Category Sort Order
    cat_order = ["Infantil A (y menor)", "Infantil B", "Juvenil A", "Juvenil B", "Todo Competidor"]
    categories = df['category'].unique()
    avail_cats = [c for c in cat_order if c in categories]
    # Add any others not in list
    others = [c for c in categories if c not in cat_order]
    avail_cats.extend(others)
    
    selected_cat = c1.selectbox("Categoría", avail_cats, key="relay_cat")
    selected_pool = c2.radio("Piscina", ["50m", "25m"], index=0, horizontal=True, key="relay_pool")
    
    # Filter Dataset
    subset = df[
        (df['category'] == selected_cat) & 
        (df['pool_size'] == selected_pool)
    ].copy()
    
    if subset.empty:
        st.info(f"No se encontraron nadadores para **{selected_cat}** en piscina de **{selected_pool}**.")
        return
        
    # --- HELPER FUNCTIONS ---
    def get_top_swimmers(sub_df, style_abbr, n=4, exclude_names=[]):
        # style_abbr: "Free", "Back", "Breast", "Fly"
        evt_name = f"50 {style_abbr}"
        candidates = sub_df[
            (sub_df['event_name'] == evt_name) & 
            (~sub_df['name'].isin(exclude_names))
        ].sort_values('seconds', ascending=True)
        return candidates.head(n)

    def display_relay_team(team_list, title, projected_time):
        st.markdown(f"#### {title}")
        st.markdown(f"**Tiempo Proyectado:** `{format_seconds(projected_time)}`")
        disp_df = pd.DataFrame(team_list)
        if not disp_df.empty:
             st.dataframe(
                disp_df[['Posta', 'Nadador', 'Tiempo', 'Fecha']], 
                use_container_width=True, 
                hide_index=True
             )
        else:
            st.warning("No hay suficientes nadadores.")

    # --- RELAY TABS ---
    t_free, t_medley = st.tabs(["🏊 4x50 Libre", "🏊 4x50 Combinado"])
    
    # 1. FREE RELAY
    with t_free:
        st.caption("Selecciona los 4 mejores tiempos en **50 Free**.")
        c_men, c_women, c_mixed = st.tabs(["Varones", "Damas", "Mixto"])
        
        # MEN FREE
        with c_men:
            m_df = subset[subset['gender'] == 'M']
            team = get_top_swimmers(m_df, "Free", 4)
            if len(team) >= 4:
                total_time = team['seconds'].sum()
                team_display = []
                for i, (_, row) in enumerate(team.iterrows()):
                    team_display.append({
                        "Posta": f"N° {i+1}", 
                        "Nadador": row['name'], 
                        "Tiempo": format_seconds(row['seconds']),
                        "Fecha": row['date'].strftime('%Y-%m-%d') if pd.notnull(row['date']) else "?"
                    })
                display_relay_team(team_display, "4x50 Libre Varones", total_time)
            else:
                st.warning(f"Solo hay {len(team)} nadadores hombres disponibles (se requieren 4).")

        # WOMEN FREE
        with c_women:
            f_df = subset[subset['gender'] == 'F']
            team = get_top_swimmers(f_df, "Free", 4)
            if len(team) >= 4:
                total_time = team['seconds'].sum()
                team_display = []
                for i, (_, row) in enumerate(team.iterrows()):
                    team_display.append({
                        "Posta": f"N° {i+1}", 
                        "Nadador": row['name'], 
                        "Tiempo": format_seconds(row['seconds']),
                        "Fecha": row['date'].strftime('%Y-%m-%d') if pd.notnull(row['date']) else "?"
                    })
                display_relay_team(team_display, "4x50 Libre Damas", total_time)
            else:
                st.warning(f"Solo hay {len(team)} nadadoras mujeres disponibles (se requieren 4).")

        # MIXED FREE
        with c_mixed:
            m_top = get_top_swimmers(subset[subset['gender'] == 'M'], "Free", 2)
            f_top = get_top_swimmers(subset[subset['gender'] == 'F'], "Free", 2)
            if len(m_top) == 2 and len(f_top) == 2:
                combined = pd.concat([m_top, f_top]).sort_values('seconds')
                total_time = combined['seconds'].sum()
                team_display = []
                for i, (_, row) in enumerate(combined.iterrows()):
                    gender_icon = "♂️" if row['gender'] == 'M' else "♀️"
                    team_display.append({
                        "Posta": f"{gender_icon}", "Nadador": row['name'], 
                        "Tiempo": format_seconds(row['seconds']), "Fecha": row['date'].strftime('%Y-%m-%d')
                    })
                display_relay_team(team_display, "4x50 Libre Mixto (2H + 2M)", total_time)
            else:
                st.warning(f"Faltan nadadores. Disp: {len(m_top)} H, {len(f_top)} M.")

    # 2. MEDLEY RELAY
    with t_medley:
        st.caption("Orden Oficial: Espalda -> Pecho -> Mariposa -> Libre")
        styles = ["Back", "Breast", "Fly", "Free"]
        c_men, c_women, c_mixed = st.tabs(["Varones", "Damas", "Mixto"])
        
        def solve_medley_team(gender_df):
            # Gather Candidates (Top 3 per style)
            cands = {}
            u_names = set()
            for s in styles:
                sub = get_top_swimmers(gender_df, s, 3) 
                cands[s] = sub
                u_names.update(sub['name'].tolist())
            
            unique_names = list(u_names)
            if len(unique_names) < 4: return None, 9999
            
            swimmer_times = {n: {} for n in unique_names}
            for s in styles:
                for _, row in cands[s].iterrows():
                    swimmer_times[row['name']][s] = row['seconds']
            
            best_team = None
            best_total = float('inf')
            
            # Permutations of name assignment
            for p in itertools.permutations(unique_names, 4):
                current_time = 0
                valid = True
                team_assignment = []
                for i, swimmer_name in enumerate(p):
                    style = styles[i]
                    t = swimmer_times[swimmer_name].get(style)
                    if t is None:
                        valid = False
                        break
                    current_time += t
                    team_assignment.append((style, swimmer_name, t))
                
                if valid and current_time < best_total:
                    best_total = current_time
                    best_team = team_assignment
            return best_team, best_total

        # MEN MEDLEY
        with c_men:
            sol, t = solve_medley_team(subset[subset['gender'] == 'M'])
            if sol:
                team_display = [{"Posta": s, "Nadador": n, "Tiempo": format_seconds(sec), "Fecha": "Calc"} for (s,n,sec) in sol]
                display_relay_team(team_display, "4x50 Combinado Varones (Optimizado)", t)
            else:
                 st.warning("No se pudo armar equipo.")

        # WOMEN MEDLEY
        with c_women:
            sol, t = solve_medley_team(subset[subset['gender'] == 'F'])
            if sol:
                team_display = [{"Posta": s, "Nadador": n, "Tiempo": format_seconds(sec), "Fecha": "Calc"} for (s,n,sec) in sol]
                display_relay_team(team_display, "4x50 Combinado Damas (Optimizado)", t)
            else:
                 st.warning("No se pudo armar equipo.")

        # MIXED MEDLEY
        with c_mixed:
            st.markdown("Busca la combinación óptima de **2 H hombres + 2 Mujeres** probando las 6 permutaciones de género.")
            permutations_pattern = set(itertools.permutations(['M', 'M', 'F', 'F']))
            global_best_team = None
            global_best_time = float('inf')
            m_df_mixed = subset[subset['gender'] == 'M']
            f_df_mixed = subset[subset['gender'] == 'F']
            
            for pattern in permutations_pattern:
                valid_pattern = True
                curr_cands = {}  
                for i, gender in enumerate(pattern):
                    style = styles[i]
                    src = m_df_mixed if gender == 'M' else f_df_mixed
                    top = get_top_swimmers(src, style, 3)
                    if top.empty:
                        valid_pattern = False; break
                    curr_cands[style] = top
                
                if not valid_pattern: continue
                
                # Iterate product of candidates
                cand_lists = [curr_cands[s] for s in styles]
                for team_combo in itertools.product(*[d.iterrows() for d in cand_lists]):
                    assigned_names = [row['name'] for _, row in team_combo]
                    if len(set(assigned_names)) != 4: continue
                    curr_t = sum([row['seconds'] for _, row in team_combo])
                    
                    if curr_t < global_best_time:
                        global_best_time = curr_t
                        formatted_sol = []
                        for idx, (_, row) in enumerate(team_combo):
                            formatted_sol.append({
                                "Posta": f"{styles[idx]} ({row['gender']})",
                                "Nadador": row['name'], "Tiempo": format_seconds(row['seconds']), "Fecha": "Calc"
                            })
                        global_best_team = formatted_sol

            if global_best_team:
                display_relay_team(global_best_team, "4x50 Combinado Mixto (Mejor Combinación)", global_best_time)
            else:
                st.warning("No se pudo armar un equipo mixto válido.")

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

    # --- SIDEBAR FOOTER REMOVED ---
    # Moved to main page footer
    st.markdown("---")
    db_timestamp = "Unknown"
    if os.path.exists(DB_PATH):
        ts = os.path.getmtime(DB_PATH)
        db_timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
    
    st.caption(f"DB Version: {db_timestamp} | v1.2 - Historical Sync")

if __name__ == "__main__":
    if check_password():
        main()
