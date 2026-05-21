"""
Equity Analytics — Admin Dashboard
Win Forms-aesthetic Streamlit app. Run on port 8502 (separate from main app on 8501).

Launch via: streamlit run admin_dashboard/app.py --server.port 8502
Or use launch.bat from repo root for the full startup sequence.
"""

import os
import sys

# Make the repo root importable (ingestion/, admin_dashboard/)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv()

import streamlit as st

# ── Page config — must be first Streamlit call ────────────────────────────────
st.set_page_config(
    page_title="Equity Analytics Admin",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Win Forms CSS ─────────────────────────────────────────────────────────────
_CSS_PATH = os.path.join(os.path.dirname(__file__), "styles", "winforms.css")
with open(_CSS_PATH) as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ── Title bar ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="title-bar">
      <span class="title-bar-icon">📊</span>
      Equity Analytics — Admin Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Status bar (always visible) ───────────────────────────────────────────────
from admin_dashboard.components import status_bar
status_bar.render()

st.markdown("<hr>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_dags, tab_logs, tab_quality = st.tabs([
    "🗓  DAG Monitor",
    "📋  Log Viewer",
    "🔍  Data Quality",
])

with tab_dags:
    from admin_dashboard.components import dag_monitor
    dag_monitor.render()

with tab_logs:
    from admin_dashboard.components import log_viewer
    log_viewer.render()

with tab_quality:
    from admin_dashboard.components import data_quality
    data_quality.render()
