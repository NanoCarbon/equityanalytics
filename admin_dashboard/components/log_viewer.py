"""
Log Viewer tab — reads Airflow task logs directly from the filesystem.

Airflow writes logs to:
  ./airflow/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=1.log

The viewer lets the user pick DAG → Run → Task → Attempt,
then streams the file contents into a dark terminal-style box.
Auto-refresh checkbox re-reads the file every N seconds.
"""

import os
import glob
import time
import streamlit as st

# Log root relative to the equityanalytics repo root (where docker-compose.yml is)
# Streamlit CWD is set to the repo root in launch.bat
LOG_ROOT = os.path.join(os.getcwd(), "airflow", "logs")


# ── Filesystem helpers ────────────────────────────────────────────────────────

def _subdirs(path: str) -> list[str]:
    """Return sorted list of immediate subdirectory base names."""
    if not os.path.isdir(path):
        return []
    return sorted(
        e.name for e in os.scandir(path) if e.is_dir()
    )


def _strip_prefix(name: str, prefix: str) -> str:
    """'dag_id=equity_daily' → 'equity_daily'"""
    if name.startswith(prefix):
        return name[len(prefix):]
    return name


def _list_dags() -> list[str]:
    entries = _subdirs(LOG_ROOT)
    return [_strip_prefix(e, "dag_id=") for e in entries if e.startswith("dag_id=")]


def _list_runs(dag_id: str) -> list[str]:
    dag_dir = os.path.join(LOG_ROOT, f"dag_id={dag_id}")
    entries = _subdirs(dag_dir)
    # Newest first — run_ids are timestamped so reverse-sort works
    runs = [_strip_prefix(e, "run_id=") for e in entries if e.startswith("run_id=")]
    return list(reversed(runs))


def _list_tasks(dag_id: str, run_id: str) -> list[str]:
    run_dir = os.path.join(LOG_ROOT, f"dag_id={dag_id}", f"run_id={run_id}")
    entries = _subdirs(run_dir)
    return [_strip_prefix(e, "task_id=") for e in entries if e.startswith("task_id=")]


def _list_attempts(dag_id: str, run_id: str, task_id: str) -> list[str]:
    task_dir = os.path.join(
        LOG_ROOT, f"dag_id={dag_id}", f"run_id={run_id}", f"task_id={task_id}"
    )
    if not os.path.isdir(task_dir):
        return []
    logs = sorted(
        f for f in os.listdir(task_dir) if f.endswith(".log")
    )
    return logs


def _read_log(dag_id, run_id, task_id, attempt_file) -> str:
    path = os.path.join(
        LOG_ROOT,
        f"dag_id={dag_id}",
        f"run_id={run_id}",
        f"task_id={task_id}",
        attempt_file,
    )
    if not os.path.isfile(path):
        return f"[Log file not found: {path}]"
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception as e:
        return f"[Error reading log: {e}]"


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.markdown("### Log Viewer")

    if not os.path.isdir(LOG_ROOT):
        st.warning(
            f"Airflow log directory not found at `{LOG_ROOT}`. "
            "Make sure Docker containers are running and logs are volume-mounted."
        )
        return

    # ── Selectors ─────────────────────────────────────────────────
    dags = _list_dags()
    if not dags:
        st.info("No DAG log folders found yet. Run a DAG first.")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        dag_id = st.selectbox("DAG", dags, key="log_dag")

    runs = _list_runs(dag_id) if dag_id else []
    with col2:
        run_id = st.selectbox(
            "Run",
            runs if runs else ["— no runs —"],
            key="log_run",
        )

    tasks = _list_tasks(dag_id, run_id) if run_id and run_id != "— no runs —" else []
    with col3:
        task_id = st.selectbox(
            "Task",
            tasks if tasks else ["— no tasks —"],
            key="log_task",
        )

    attempts = (
        _list_attempts(dag_id, run_id, task_id)
        if task_id and task_id != "— no tasks —" else []
    )
    with col4:
        attempt = st.selectbox(
            "Attempt",
            attempts if attempts else ["— none —"],
            key="log_attempt",
        )

    # ── Auto-refresh ───────────────────────────────────────────────
    refresh_col, interval_col, _ = st.columns([2, 2, 8])
    with refresh_col:
        auto_refresh = st.checkbox("Auto-refresh", value=False, key="log_autorefresh")
    with interval_col:
        refresh_secs = st.selectbox(
            "Every", [5, 10, 30, 60], index=1,
            key="log_interval", label_visibility="collapsed"
        ) if auto_refresh else 10

    # ── Log content ────────────────────────────────────────────────
    if attempt and attempt != "— none —":
        content = _read_log(dag_id, run_id, task_id, attempt)
        line_count = content.count("\n")

        st.markdown(
            f"<div style='font-size:10px;color:#666;margin-bottom:4px;'>"
            f"Path: airflow/logs/dag_id={dag_id}/run_id={run_id}/"
            f"task_id={task_id}/{attempt} &nbsp;|&nbsp; {line_count:,} lines"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Show last N lines to keep the widget responsive
        max_lines = st.slider(
            "Max lines to display (from end)", 50, 2000, 500, step=50,
            key="log_maxlines",
        )
        lines = content.splitlines()
        display = "\n".join(lines[-max_lines:]) if len(lines) > max_lines else content

        st.markdown(
            f'<div class="log-viewer">{_escape_html(display)}</div>',
            unsafe_allow_html=True,
        )

        if auto_refresh:
            time.sleep(refresh_secs)
            st.rerun()
    else:
        st.info("Select a DAG, run, task, and attempt above to view logs.")


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
    )
