"""
Status bar — connectivity indicators for Docker, Airflow, and Snowflake.
Rendered as a single row of traffic-light dots with labels.
"""

import subprocess
import streamlit as st
from admin_dashboard.airflow_client import airflow_healthy
from ingestion.load import get_connection


def _docker_running() -> tuple[bool, str]:
    """Check if the Airflow Docker containers are up."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--filter", "status=running"],
            capture_output=True, text=True, timeout=8,
            cwd=None,  # uses CWD of Streamlit process
        )
        services = [s.strip() for s in result.stdout.strip().splitlines() if s.strip()]
        expected = {"airflow-webserver", "airflow-scheduler", "airflow-db"}
        running = set(services)
        missing = expected - running
        if not missing:
            return True, f"All services up ({', '.join(sorted(running))})"
        return False, f"Missing: {', '.join(sorted(missing))}"
    except FileNotFoundError:
        return False, "docker not found in PATH"
    except Exception as e:
        return False, str(e)


def _snowflake_ok() -> tuple[bool, str]:
    try:
        conn = get_connection()
        conn.close()
        return True, "Auth OK"
    except Exception as e:
        return False, str(e)[:80]


def _dot(ok: bool) -> str:
    return "🟢" if ok else "🔴"


def render():
    """Render the connectivity status bar."""
    docker_ok, docker_msg = _docker_running()
    airflow_ok, airflow_msg = airflow_healthy()
    snow_ok, snow_msg = _snowflake_ok()

    col1, col2, col3, col4 = st.columns([1, 1, 1, 4])
    with col1:
        st.markdown(
            f"{_dot(docker_ok)} **Docker**  \n"
            f"<small>{docker_msg}</small>",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"{_dot(airflow_ok)} **Airflow**  \n"
            f"<small>{airflow_msg}</small>",
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f"{_dot(snow_ok)} **Snowflake**  \n"
            f"<small>{snow_msg}</small>",
            unsafe_allow_html=True,
        )
    with col4:
        # Overall health pill
        all_ok = docker_ok and airflow_ok and snow_ok
        label = "ALL SYSTEMS NOMINAL" if all_ok else "DEGRADED"
        color = "#006600" if all_ok else "#990000"
        st.markdown(
            f'<div style="background:{color};color:#fff;padding:4px 10px;'
            f'font-weight:bold;font-size:12px;display:inline-block;">'
            f'{label}</div>',
            unsafe_allow_html=True,
        )
