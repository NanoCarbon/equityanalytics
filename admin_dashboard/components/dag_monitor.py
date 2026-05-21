"""
DAG Monitor tab — shows all DAGs with their last run status, next run time,
and a trigger button with a confirmation dialog before firing.
"""

import streamlit as st
from datetime import datetime, timezone
from admin_dashboard import airflow_client as af


# ── Helpers ───────────────────────────────────────────────────────────────────

STATE_COLORS = {
    "success":  ("🟢", "#006600"),
    "running":  ("🔵", "#000080"),
    "failed":   ("🔴", "#990000"),
    "queued":   ("🟡", "#886600"),
    "skipped":  ("⬜", "#888888"),
    None:       ("⬜", "#888888"),
}

# Human-readable schedule descriptions
SCHEDULE_LABELS = {
    "0 4 * * 2-6":  "Mon–Fri 11 pm ET",
    "0 4 * * 0":    "Saturday 11 pm ET",
    "0 4 2 * *":    "1st of month 11 pm ET",
    None:            "Manual only",
}

# DAGs we always want visible, even if Airflow hasn't parsed them yet
KNOWN_DAGS = [
    "equity_daily",
    "macro_daily",
    "fundamentals_weekly",
    "valuation_daily",
    "equity_supplemental_weekly",
    "fred_catalog_refresh",
    "backfill_prices",
    "backfill_new_tickers",
    "macro_backfill",
]


def _fmt_dt(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso


def _duration(run: dict) -> str:
    s = run.get("start_date")
    e = run.get("end_date")
    if not s:
        return "—"
    try:
        start = datetime.fromisoformat(s.replace("Z", "+00:00"))
        end = (
            datetime.fromisoformat(e.replace("Z", "+00:00"))
            if e else datetime.now(timezone.utc)
        )
        secs = int((end - start).total_seconds())
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m {secs % 60}s"
    except Exception:
        return "—"


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.markdown("### DAG Monitor")

    # ── Refresh ───────────────────────────────────────────────────
    if st.button("Refresh DAG list"):
        st.cache_data.clear()

    dags_raw = af.list_dags()
    if dags_raw and "_error" in dags_raw[0]:
        st.error(f"Cannot reach Airflow API: {dags_raw[0]['_error']}")
        st.info("Make sure Docker containers are running (see status bar above).")
        return

    dag_by_id = {d["dag_id"]: d for d in dags_raw}

    # ── Trigger confirmation state ────────────────────────────────
    # We keep the pending trigger in session_state so the confirmation
    # dialog survives a Streamlit re-run.
    if "pending_trigger" not in st.session_state:
        st.session_state.pending_trigger = None

    if st.session_state.pending_trigger:
        _render_confirm_dialog()
        return  # Show ONLY the confirm dialog until resolved

    # ── DAG table ─────────────────────────────────────────────────
    visible_ids = list(dict.fromkeys(
        KNOWN_DAGS + [d["dag_id"] for d in dags_raw]
    ))

    for dag_id in visible_ids:
        dag = dag_by_id.get(dag_id)
        latest = af.get_latest_run(dag_id)

        state = latest.get("state") if latest else None
        icon, _ = STATE_COLORS.get(state, STATE_COLORS[None])
        schedule_raw = dag.get("schedule_interval") if dag else None
        schedule_str = (
            SCHEDULE_LABELS.get(schedule_raw, schedule_raw or "Manual only")
            if dag else "—"
        )
        paused = dag.get("is_paused", False) if dag else None
        paused_badge = " ⏸" if paused else ""

        with st.expander(
            f"{icon} **{dag_id}**{paused_badge}  ·  {schedule_str}  ·  "
            f"Last: {_fmt_dt(latest.get('start_date') if latest else None)}  ·  "
            f"State: {state or 'never run'}",
            expanded=False,
        ):
            col_info, col_btn = st.columns([3, 1])

            with col_info:
                if latest:
                    st.write(f"**Run ID:** `{latest.get('dag_run_id', '—')}`")
                    st.write(f"**Started:** {_fmt_dt(latest.get('start_date'))}")
                    st.write(f"**Ended:** {_fmt_dt(latest.get('end_date'))}")
                    st.write(f"**Duration:** {_duration(latest)}")

                    run_id = latest.get("dag_run_id")
                    if run_id:
                        tasks = af.list_task_instances(dag_id, run_id)
                        if tasks and "_error" not in tasks[0]:
                            st.write("**Tasks:**")
                            for t in tasks:
                                t_icon, _ = STATE_COLORS.get(
                                    t.get("state"), STATE_COLORS[None]
                                )
                                st.write(
                                    f"  {t_icon} `{t['task_id']}` — "
                                    f"{t.get('state', '?')}  "
                                    f"({_duration(t)})"
                                )
                else:
                    st.info("No runs recorded yet.")

            with col_btn:
                if st.button("Trigger", key=f"trigger_{dag_id}"):
                    st.session_state.pending_trigger = dag_id
                    st.rerun()

                if dag and paused is not None:
                    label = "Unpause" if paused else "Pause"
                    if st.button(label, key=f"pause_{dag_id}"):
                        result = af.set_dag_paused(dag_id, not paused)
                        if "_error" in result:
                            st.error(result["_error"])
                        else:
                            st.success(f"DAG {'unpaused' if paused else 'paused'}.")
                            st.rerun()


def _render_confirm_dialog():
    """Full-screen confirmation before firing a trigger."""
    dag_id = st.session_state.pending_trigger

    st.warning(f"### Confirm Manual Trigger")
    st.markdown(
        f"""
        You are about to manually trigger:

        > **`{dag_id}`**

        This will start a new DAG run immediately. If the DAG is already running,
        a second instance will start in parallel (Airflow allows concurrent runs
        unless `max_active_runs` is set to 1 in the DAG definition).

        **Are you sure?**
        """
    )

    col_yes, col_no, _ = st.columns([1, 1, 5])
    with col_yes:
        if st.button("Yes, Trigger", type="primary"):
            result = af.trigger_dag(dag_id)
            st.session_state.pending_trigger = None
            if "_error" in result:
                st.error(f"Trigger failed: {result['_error']}")
            else:
                run_id = result.get("dag_run_id", "?")
                st.success(f"Triggered `{dag_id}` — run ID: `{run_id}`")
            st.rerun()

    with col_no:
        if st.button("Cancel"):
            st.session_state.pending_trigger = None
            st.rerun()
