import sys
import logging
from pathlib import Path
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Path setup ────────────────────────────────────────────────────────────────
# Add the repo root (parent of this file's directory) to sys.path so that
# `from agents.x import y` resolves correctly. The folder containing this file
# must NOT be named 'streamlit' — that shadows the installed streamlit package.
# This folder is named 'app/' in the repo to avoid that conflict.

repo_root = Path(__file__).parent.parent
app_root  = Path(__file__).parent

for p in [str(repo_root), str(app_root)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from components.overview import render_overview
from components.chat import render_chat
from components.event_study import render_event_study

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Equity Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Inject CSS ────────────────────────────────────────────────────────────────

css_path = Path(__file__).parent / "styles" / "theme.css"
st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_overview, tab_chat, tab_events = st.tabs([
    "01 · Overview",
    "02 · AI Analytics",
    "03 · Event Study",
])

with tab_overview:
    render_overview()

with tab_chat:
    render_chat()

with tab_events:
    render_event_study()