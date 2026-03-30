import re
import logging
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from streamlit.db.snowflake import execute_sql_cached

logger = logging.getLogger(__name__)


def render_event_study():
    st.markdown("### Event Study")
    st.caption("Define a market event condition and measure forward returns across all historical occurrences.")

    col1, col2, col3 = st.columns(3)
    with col1:
        ticker = st.text_input("Ticker", value="SPY").upper().strip()
    with col2:
        condition = st.selectbox("Event condition", [
            "Daily return ≥ X%", "Daily return ≤ -X%",
            "Price within X% of 52-week high", "Price within X% of 52-week low",
            "Volume spike ≥ X× average"
        ])
    with col3:
        threshold = st.number_input("Threshold (X)", min_value=0.1, max_value=50.0, value=3.0, step=0.5)

    if st.button("Run Event Study", type="primary"):
        if not re.match(r'^[A-Z0-9\-]{1,10}$', ticker):
            st.error("Invalid ticker. Use letters, numbers, and hyphens only (e.g. AAPL, BRK-B).")
            st.stop()

        td = threshold / 100
        if condition == "Daily return ≥ X%":                  where_clause = f"daily_return >= {td}"
        elif condition == "Daily return ≤ -X%":               where_clause = f"daily_return <= -{td}"
        elif condition == "Price within X% of 52-week high":  where_clause = f"pct_of_52w_high >= {1 - td}"
        elif condition == "Price within X% of 52-week low":   where_clause = f"close_price <= week_52_low * {1 + td}"
        else:                                                  where_clause = f"volume >= {threshold}"

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
                AND f.price_date <= DATEADD(day, 63, e.event_date)
        ),
        pivoted AS (
            SELECT event_date, ROUND(event_return * 100, 2) AS event_return_pct,
                   MAX(CASE WHEN days_forward = 1  THEN cum_return END) AS d1,
                   MAX(CASE WHEN days_forward = 2  THEN cum_return END) AS d2,
                   MAX(CASE WHEN days_forward = 3  THEN cum_return END) AS d3,
                   MAX(CASE WHEN days_forward = 5  THEN cum_return END) AS d5,
                   MAX(CASE WHEN days_forward = 10 THEN cum_return END) AS d10,
                   MAX(CASE WHEN days_forward = 21 THEN cum_return END) AS d21,
                   MAX(CASE WHEN days_forward = 42 THEN cum_return END) AS d42,
                   MAX(CASE WHEN days_forward = 63 THEN cum_return END) AS d63
            FROM forward_returns GROUP BY event_date, event_return
        )
        SELECT * FROM pivoted ORDER BY event_date DESC
        """

        logger.info("Event study: ticker=%s condition=%s threshold=%s", ticker, condition, threshold)

        with st.spinner(f"Scanning history for {ticker}…"):
            try:
                df = execute_sql_cached(event_sql)
                if df.empty:
                    st.warning(f"No events found for {ticker} · {condition} · {threshold}%")
                    return

                fwd_cols = ['d1','d2','d3','d5','d10','d21','d42','d63']
                labels   = ['1D','2D','3D','5D','10D','21D','42D','63D']

                st.markdown(f"**{len(df)} events** · {ticker} · {condition} · threshold {threshold}%")

                summary = {
                    'Horizon':    labels,
                    'Avg %':      [round(df[c].mean() * 100, 2) for c in fwd_cols],
                    'Median %':   [round(df[c].median() * 100, 2) for c in fwd_cols],
                    '% Positive': [round((df[c] > 0).sum() * 100 / df[c].notna().sum(), 1) for c in fwd_cols],
                    'Best %':     [round(df[c].max() * 100, 2) for c in fwd_cols],
                    'Worst %':    [round(df[c].min() * 100, 2) for c in fwd_cols],
                }
                st.dataframe(pd.DataFrame(summary), hide_index=True, use_container_width=True)

                medians = [df[c].median() * 100 for c in fwd_cols]
                p25 = [df[c].quantile(0.25) * 100 for c in fwd_cols]
                p75 = [df[c].quantile(0.75) * 100 for c in fwd_cols]
                p10 = [df[c].quantile(0.10) * 100 for c in fwd_cols]
                p90 = [df[c].quantile(0.90) * 100 for c in fwd_cols]

                fig = go.Figure()
                fig.add_trace(go.Scatter(x=labels+labels[::-1], y=p90+p10[::-1],
                    fill='toself', fillcolor='rgba(37,99,235,0.07)',
                    line=dict(color='rgba(0,0,0,0)'), name='10th–90th pct'))
                fig.add_trace(go.Scatter(x=labels+labels[::-1], y=p75+p25[::-1],
                    fill='toself', fillcolor='rgba(37,99,235,0.14)',
                    line=dict(color='rgba(0,0,0,0)'), name='25th–75th pct'))
                fig.add_trace(go.Scatter(x=labels, y=medians,
                    line=dict(color='#2563eb', width=2), name='Median',
                    mode='lines+markers', marker=dict(size=5, color='#2563eb')))
                fig.add_hline(y=0, line_dash="dash", line_color="#cbd5e1", opacity=0.8)
                fig.update_layout(
                    title=f"{ticker} — forward returns after {condition} {threshold}% ({len(df)} events)",
                    xaxis_title="Trading horizon", yaxis_title="Cumulative return %",
                    template="plotly_white", hovermode="x unified",
                    font=dict(family="DM Mono, monospace", size=11),
                    title_font=dict(size=12, color="#64748b"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)

                with st.expander(f"All {len(df)} event instances"):
                    ddisp = df.copy()
                    for c in fwd_cols:
                        ddisp[c] = (ddisp[c] * 100).round(2)
                    ddisp.columns = ['Event Date', 'Event Return %'] + labels
                    st.dataframe(ddisp, hide_index=True, use_container_width=True)

                with st.expander("SQL"):
                    st.code(event_sql, language="sql")

            except Exception as e:
                logger.error("Event study error: %s", e)
                st.error(f"Error running event study: {str(e)}")
