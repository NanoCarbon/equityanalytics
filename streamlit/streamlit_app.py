import sys
import logging
from pathlib import Path
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Path setup ────────────────────────────────────────────────────────────────
# streamlit_app.py lives in streamlit/ but agents/ is at the repo root.
# Add the repo root to sys.path so `from agents.x import y` resolves correctly
# whether running locally (streamlit run streamlit/streamlit_app.py) or on
# Streamlit Community Cloud (main file path: streamlit/streamlit_app.py).

repo_root = Path(__file__).parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from streamlit.components.overview import render_overview
from streamlit.components.chat import render_chat
from streamlit.components.event_study import render_event_study

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
