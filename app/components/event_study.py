import re
import logging
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from db.snowflake import execute_sql_cached

logger = logging.getLogger(__name__)

# Forward look horizons in calendar days
FWD_DAYS   = [1, 3, 5, 30, 90, 180, 365, 730]
FWD_COLS   = [f"d{d}" for d in FWD_DAYS]
FWD_LABELS = ["1D", "3D", "5D", "30D", "90D", "180D", "1Y", "2Y"]
MAX_FWD    = max(FWD_DAYS)  # 730 — outer bound for the JOIN


def render_event_study():
    st.markdown("### Event Study")
    st.caption("Define a market event condition and measure forward returns across all historical occurrences.")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.text_input("Ticker", value="SPY").upper().strip()
    with col2:
        condition = st.selectbox("Event condition", [
            "Daily return \u2265 X%", "Daily return \u2264 -X%",
            "Price within X% of 52-week high", "Price within X% of 52-week low",
            "Volume spike \u2265 X\u00d7 average"
        ])
    with col3:
        threshold = st.number_input("Threshold (X)", min_value=0.1, max_value=50.0, value=3.0, step=0.5)

    if st.button("Run Event Study", type="primary"):
        if not re.match(r'^[A-Z0-9\-]{1,10}$', ticker):
            st.error("Invalid ticker. Use letters, numbers, and hyphens only (e.g. AAPL, BRK-B).")
            st.stop()

        td = threshold / 100
        if condition == "Daily return \u2265 X%":                  where_clause = f"daily_return >= {td}"
        elif condition == "Daily return \u2264 -X%":               where_clause = f"daily_return <= -{td}"
        elif condition == "Price within X% of 52-week high":  where_clause = f"pct_of_52w_high >= {1 - td}"
        elif condition == "Price within X% of 52-week low":   where_clause = f"close_price <= week_52_low * {1 + td}"
        else:                                                  where_clause = f"volume >= {threshold}"

        pivot_cases = ",\n                   ".join(
            f"MAX(CASE WHEN days_forward = {d} THEN cum_return END) AS d{d}"
            for d in FWD_DAYS
        )

        event_sql = f"""
        WITH events AS (
            SELECT ticker, price_date AS event_date,
                   daily_return AS event_return, close_price AS event_close
            FROM EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES
            WHERE ticker = '{ticker}' AND {where_clause}
        ),
        forward_returns AS (
            SELECT e.event_date, e.event_return,
                   DATEDIFF(day, e.event_date, f.price_date) AS days_forward,
                   (f.close_price - e.event_close) / e.event_close AS cum_return
            FROM events e
            JOIN EQUITY_ANALYTICS.MARTS.FACT_DAILY_PRICES f
                ON f.ticker = e.ticker
                AND f.price_date > e.event_date
                AND f.price_date <= DATEADD(day, {MAX_FWD}, e.event_date)
        ),
        pivoted AS (
            SELECT event_date, ROUND(event_return * 100, 2) AS event_return_pct,
                   {pivot_cases}
            FROM forward_returns GROUP BY event_date, event_return
        )
        SELECT * FROM pivoted ORDER BY event_date DESC
        """

        logger.info("Event study: ticker=%s condition=%s threshold=%s", ticker, condition, threshold)

        with st.spinner(f"Scanning history for {ticker}\u2026"):
            try:
                df = execute_sql_cached(event_sql)
                if df.empty:
                    st.warning(f"No events found for {ticker} \u00b7 {condition} \u00b7 {threshold}%")
                    return

                st.markdown(f"**{len(df)} events** \u00b7 {ticker} \u00b7 {condition} \u00b7 threshold {threshold}%")

                summary = {
                    "Horizon":    FWD_LABELS,
                    "Avg %":      [round(df[c].mean() * 100, 2) for c in FWD_COLS],
                    "Median %":   [round(df[c].median() * 100, 2) for c in FWD_COLS],
                    "% Positive": [round((df[c] > 0).sum() * 100 / df[c].notna().sum(), 1) for c in FWD_COLS],
                    "Best %":     [round(df[c].max() * 100, 2) for c in FWD_COLS],
                    "Worst %":    [round(df[c].min() * 100, 2) for c in FWD_COLS],
                }
                st.dataframe(pd.DataFrame(summary), hide_index=True, use_container_width=True)

                medians = [df[c].median() * 100 for c in FWD_COLS]
                p25 = [df[c].quantile(0.25) * 100 for c in FWD_COLS]
                p75 = [df[c].quantile(0.75) * 100 for c in FWD_COLS]
                p10 = [df[c].quantile(0.10) * 100 for c in FWD_COLS]
                p90 = [df[c].quantile(0.90) * 100 for c in FWD_COLS]

                fig = go.Figure()
                fig.add_trace(go.Scatter(x=FWD_LABELS+FWD_LABELS[::-1], y=p90+p10[::-1],
                    fill="toself", fillcolor="rgba(37,99,235,0.07)",
                    line=dict(color="rgba(0,0,0,0)"), name="10th\u201390th pct"))
                fig.add_trace(go.Scatter(x=FWD_LABELS+FWD_LABELS[::-1], y=p75+p25[::-1],
                    fill="toself", fillcolor="rgba(37,99,235,0.14)",
                    line=dict(color="rgba(0,0,0,0)"), name="25th\u201375th pct"))
                fig.add_trace(go.Scatter(x=FWD_LABELS, y=medians,
                    line=dict(color="#2563eb", width=2), name="Median",
                    mode="lines+markers", marker=dict(size=5, color="#2563eb")))
                fig.add_hline(y=0, line_dash="dash", line_color="#cbd5e1", opacity=0.8)
                fig.update_layout(
                    title=f"{ticker} \u2014 forward returns after {condition} {threshold}% ({len(df)} events)",
                    xaxis_title="Trading horizon", yaxis_title="Cumulative return %",
                    template="plotly_white", hovermode="x unified",
                    font=dict(family="DM Mono, monospace", size=11),
                    title_font=dict(size=12, color="#64748b"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)

                with st.expander(f"All {len(df)} event instances"):
                    ddisp = df.copy()
                    for c in FWD_COLS:
                        ddisp[c] = (ddisp[c] * 100).round(2)
                    ddisp.columns = ["Event Date", "Event Return %"] + FWD_LABELS
                    st.dataframe(ddisp, hide_index=True, use_container_width=True)

                with st.expander("SQL"):
                    st.code(event_sql, language="sql")

            except Exception as e:
                logger.error("Event study error: %s", e)
                st.error(f"Error running event study: {str(e)}")