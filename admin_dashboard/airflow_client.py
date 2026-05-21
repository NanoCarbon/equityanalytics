"""
Thin wrapper around the Airflow REST API (v1).
Base URL: http://localhost:8080/api/v1
Auth: HTTP Basic using AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from env.

All functions return plain dicts/lists — no Airflow SDK dependency needed.
"""

import os
import requests
from datetime import datetime, timezone
from typing import Optional


def _session() -> requests.Session:
    s = requests.Session()
    s.auth = (
        os.environ.get("AIRFLOW_ADMIN_USER", "admin"),
        os.environ.get("AIRFLOW_ADMIN_PASSWORD", ""),
    )
    s.headers["Content-Type"] = "application/json"
    return s


BASE = "http://localhost:8080/api/v1"


# ── DAG listing ───────────────────────────────────────────────────────────────

def list_dags() -> list[dict]:
    """
    Return all DAGs the Airflow scheduler knows about.
    Each dict includes: dag_id, is_paused, schedule_interval, next_dagrun,
    last_parsed_time, tags.
    """
    try:
        r = _session().get(f"{BASE}/dags", params={"limit": 100}, timeout=10)
        r.raise_for_status()
        return r.json().get("dags", [])
    except Exception as e:
        return [{"_error": str(e)}]


def get_dag(dag_id: str) -> dict:
    try:
        r = _session().get(f"{BASE}/dags/{dag_id}", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}


# ── DAG runs ──────────────────────────────────────────────────────────────────

def list_dag_runs(dag_id: str, limit: int = 10) -> list[dict]:
    """
    Return the most recent DAG runs for a given DAG, newest first.
    Each dict includes: dag_run_id, state, start_date, end_date, logical_date.
    """
    try:
        r = _session().get(
            f"{BASE}/dags/{dag_id}/dagRuns",
            params={"limit": limit, "order_by": "-start_date"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("dag_runs", [])
    except Exception as e:
        return [{"_error": str(e)}]


def get_latest_run(dag_id: str) -> Optional[dict]:
    """Return the single most-recent DAG run, or None."""
    runs = list_dag_runs(dag_id, limit=1)
    if runs and "_error" not in runs[0]:
        return runs[0]
    return None


# ── Task instances ────────────────────────────────────────────────────────────

def list_task_instances(dag_id: str, run_id: str) -> list[dict]:
    """
    Return all task instances for a specific DAG run.
    Includes: task_id, state, start_date, end_date, duration.
    """
    try:
        r = _session().get(
            f"{BASE}/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("task_instances", [])
    except Exception as e:
        return [{"_error": str(e)}]


# ── Trigger ───────────────────────────────────────────────────────────────────

def trigger_dag(dag_id: str, conf: Optional[dict] = None) -> dict:
    """
    Trigger a manual DAG run.
    Returns the new DagRun object on success, or dict with '_error' key.
    """
    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": conf or {},
    }
    try:
        r = _session().post(
            f"{BASE}/dags/{dag_id}/dagRuns",
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        return {"_error": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
    except Exception as e:
        return {"_error": str(e)}


# ── Pause / unpause ───────────────────────────────────────────────────────────

def set_dag_paused(dag_id: str, paused: bool) -> dict:
    try:
        r = _session().patch(
            f"{BASE}/dags/{dag_id}",
            json={"is_paused": paused},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}


# ── Connection / health ───────────────────────────────────────────────────────

def airflow_healthy() -> tuple[bool, str]:
    """
    Returns (True, version_string) if Airflow API is reachable, else (False, error).
    """
    try:
        r = _session().get(f"{BASE}/version", timeout=5)
        r.raise_for_status()
        return True, r.json().get("version", "unknown")
    except Exception as e:
        return False, str(e)
