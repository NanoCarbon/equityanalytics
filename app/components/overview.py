import streamlit as st
import plotly.express as px
import pandas as pd
from db.snowflake import load_summary_stats, load_securities, load_macro_series
from agents.prompts import FRED_CATEGORIES


def render_overview():
    series_to_cat = {s: cat for cat, series in FRED_CATEGORIES.items() for s in series}

    # Hero
    st.markdown("""
    <div class="hero">
        <div class="hero-eyebrow">Portfolio Project · Data Engineering</div>
        <div class="hero-title">Equity Analytics Pipeline</div>
        <div class="hero-sub">
            A production-style ELT pipeline covering the full S&amp;P 500 universe,
            95 Federal Reserve macro indicators, and complete fundamental financial data —
            modeled into a Kimball dimensional warehouse and exposed through a
            natural language analytics interface powered by Claude.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Live stats
    with st.spinner(""):
        stats = load_summary_stats()

    if stats:
        equity_count = int(stats.get("equity_count", 0))
        macro_count  = int(stats.get("macro_series_count", 0))
        price_rows   = int(stats.get("price_rows", 0))
        price_start  = str(stats.get("price_start", ""))[:4]
        price_end    = str(stats.get("price_end", ""))[:4]

        st.markdown(f"""
        <div class="metric-strip">
            <div class="metric-cell">
                <div class="metric-value">{equity_count:,}</div>
                <div class="metric-label">Securities</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{macro_count}</div>
                <div class="metric-label">FRED series</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{price_rows/1_000_000:.1f}M</div>
                <div class="metric-label">Price observations</div>
            </div>
            <div class="metric-cell">
                <div class="metric-value">{price_start}–{price_end}</div>
                <div class="metric-label">History</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Feature cards
    st.markdown("""
    <div class="card-grid">
        <div class="card">
            <div class="card-tab">Tab 02</div>
            <div class="card-title">AI Analytics Chat</div>
            <div class="card-body">
                Ask questions in plain English. Claude translates your prompt into
                Snowflake SQL, executes it against the mart layer, and renders
                an interactive chart — no SQL required.
            </div>
            <div class="card-pills">
                <div class="card-pill">"Compare cumulative returns for SPY, QQQ, IWM"</div>
                <div class="card-pill">"Show AAPL revenue over 4 years"</div>
                <div class="card-pill">"Which stocks have the highest FCF yield?"</div>
            </div>
        </div>
        <div class="card">
            <div class="card-tab">Tab 03</div>
            <div class="card-title">Event Study</div>
            <div class="card-body">
                Define a market condition and measure forward returns across all
                historical occurrences. Returns a fan chart with median, IQR,
                and 10th–90th percentile bands out to 63 trading days.
            </div>
            <div class="card-pills">
                <div class="card-pill">"SPY daily return ≥ 3%"</div>
                <div class="card-pill">"AAPL within 2% of 52-week high"</div>
                <div class="card-pill">"QQQ daily return ≤ -5%"</div>
            </div>
        </div>
        <div class="card">
            <div class="card-tab">Below</div>
            <div class="card-title">Data Explorer</div>
            <div class="card-body">
                Search the full universe of securities and macro indicators.
                Filter by sector, market cap, or FRED category to discover
                what's available before writing a prompt.
            </div>
            <div class="card-pills">
                <div class="card-pill">S&amp;P 500 + top 100 ETFs by AUM</div>
                <div class="card-pill">95 FRED series across 11 categories</div>
                <div class="card-pill">Income stmt · Balance sheet · Cash flow</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Stack
    st.markdown('<div class="section-header">Stack</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="stack-grid">
        <div class="stack-item"><div class="stack-layer">Ingestion</div><div class="stack-tech">Python · yfinance · FRED API</div></div>
        <div class="stack-item"><div class="stack-layer">Orchestration</div><div class="stack-tech">Prefect Cloud</div></div>
        <div class="stack-item"><div class="stack-layer">Warehouse</div><div class="stack-tech">Snowflake</div></div>
        <div class="stack-item"><div class="stack-layer">Transformation</div><div class="stack-tech">dbt Cloud · Kimball model</div></div>
        <div class="stack-item"><div class="stack-layer">Quality</div><div class="stack-tech">54 dbt tests · CI gate</div></div>
        <div class="stack-item"><div class="stack-layer">CI/CD</div><div class="stack-tech">GitHub Actions · RSA auth</div></div>
        <div class="stack-item"><div class="stack-layer">AI</div><div class="stack-tech">Claude API · Anthropic</div></div>
        <div class="stack-item"><div class="stack-layer">Application</div><div class="stack-tech">Streamlit · Plotly</div></div>
    </div>
    """, unsafe_allow_html=True)

    # Data Explorer
    st.markdown('<div class="section-header">Data Explorer</div>', unsafe_allow_html=True)

    ex_eq, ex_macro = st.tabs(["Equities & ETFs", "Macro Indicators"])

    with ex_eq:
        with st.spinner(""):
            sec_df = load_securities()

        if not sec_df.empty:
            c1, c2, c3 = st.columns([3, 2, 2])
            with c1:
                search = st.text_input("", placeholder="Search ticker, company, sector, industry…",
                                       key="eq_search", label_visibility="collapsed")
            with c2:
                sectors = ["All sectors"] + sorted(sec_df["sector"].dropna().unique().tolist())
                sel_sector = st.selectbox("", sectors, key="eq_sector", label_visibility="collapsed")
            with c3:
                cap_opts = ["All sizes", "Mega (>$200B)", "Large ($10–200B)", "Mid ($2–10B)", "Small (<$2B)", "ETFs / N/A"]
                sel_cap = st.selectbox("", cap_opts, key="eq_cap", label_visibility="collapsed")

            filt = sec_df.copy()
            if search:
                m = (filt["ticker"].str.contains(search.upper(), na=False) |
                     filt["company_name"].str.contains(search, case=False, na=False) |
                     filt["sector"].str.contains(search, case=False, na=False) |
                     filt["industry"].str.contains(search, case=False, na=False))
                filt = filt[m]
            if sel_sector != "All sectors":
                filt = filt[filt["sector"] == sel_sector]
            if sel_cap == "Mega (>$200B)":      filt = filt[filt["market_cap_usd"] >= 200e9]
            elif sel_cap == "Large ($10–200B)": filt = filt[(filt["market_cap_usd"] >= 10e9) & (filt["market_cap_usd"] < 200e9)]
            elif sel_cap == "Mid ($2–10B)":     filt = filt[(filt["market_cap_usd"] >= 2e9) & (filt["market_cap_usd"] < 10e9)]
            elif sel_cap == "Small (<$2B)":     filt = filt[filt["market_cap_usd"] < 2e9]
            elif sel_cap == "ETFs / N/A":       filt = filt[filt["market_cap_usd"].isna()]

            disp = filt.copy()
            disp["market_cap_usd"] = disp["market_cap_usd"].apply(lambda x: f"${x/1e9:.1f}B" if pd.notna(x) else "—")
            disp["first_trading_date"] = pd.to_datetime(disp["first_trading_date"]).dt.strftime("%Y-%m-%d")
            disp["last_trading_date"]  = pd.to_datetime(disp["last_trading_date"]).dt.strftime("%Y-%m-%d")
            disp.columns = ["Ticker", "Company", "Sector", "Industry", "Mkt Cap", "First", "Last"]

            st.caption(f"{len(filt):,} of {len(sec_df):,} securities")
            st.dataframe(disp, use_container_width=True, hide_index=True, height=400,
                column_config={
                    "Ticker":  st.column_config.TextColumn(width="small"),
                    "Mkt Cap": st.column_config.TextColumn(width="small"),
                    "First":   st.column_config.TextColumn(width="small"),
                    "Last":    st.column_config.TextColumn(width="small"),
                })

            sector_counts = (sec_df[sec_df["sector"].notna()]
                             .groupby("sector").size().reset_index(name="n")
                             .sort_values("n", ascending=True))
            fig_s = px.bar(sector_counts, x="n", y="sector", orientation="h",
                           title="Securities by Sector", template="plotly_white",
                           color_discrete_sequence=["#2563eb"])
            fig_s.update_layout(height=340, margin=dict(l=0,r=0,t=40,b=0),
                                xaxis_title="", yaxis_title="", showlegend=False,
                                font=dict(family="DM Mono, monospace", size=11),
                                title_font=dict(size=12, color="#64748b"))
            st.plotly_chart(fig_s, use_container_width=True)

    with ex_macro:
        with st.spinner(""):
            macro_df = load_macro_series()

        if not macro_df.empty:
            macro_df["category"] = macro_df["series_id"].map(series_to_cat).fillna("Other")

            mc1, mc2 = st.columns([3, 2])
            with mc1:
                msearch = st.text_input("", placeholder="Search series ID or name…",
                                        key="macro_search", label_visibility="collapsed")
            with mc2:
                mcats = ["All categories"] + sorted(macro_df["category"].unique().tolist())
                sel_cat = st.selectbox("", mcats, key="macro_cat", label_visibility="collapsed")

            mfilt = macro_df.copy()
            if msearch:
                mm = (mfilt["series_id"].str.contains(msearch.upper(), na=False) |
                      mfilt["series_name"].str.contains(msearch, case=False, na=False) |
                      mfilt["category"].str.contains(msearch, case=False, na=False))
                mfilt = mfilt[mm]
            if sel_cat != "All categories":
                mfilt = mfilt[mfilt["category"] == sel_cat]

            mdisp = mfilt.copy()
            mdisp["first_observation"] = pd.to_datetime(mdisp["first_observation"]).dt.strftime("%Y-%m-%d")
            mdisp["last_observation"]  = pd.to_datetime(mdisp["last_observation"]).dt.strftime("%Y-%m-%d")
            mdisp["observation_count"] = mdisp["observation_count"].apply(lambda x: f"{x:,}")
            mdisp = mdisp[["series_id","series_name","category","first_observation","last_observation","observation_count"]]
            mdisp.columns = ["Series ID","Name","Category","First Obs.","Last Obs.","Observations"]

            st.caption(f"{len(mfilt):,} of {len(macro_df):,} series")
            st.dataframe(mdisp, use_container_width=True, hide_index=True, height=400,
                column_config={
                    "Series ID":    st.column_config.TextColumn(width="small"),
                    "Observations": st.column_config.TextColumn(width="small"),
                })

            cat_counts = (macro_df.groupby("category").size()
                          .reset_index(name="n").sort_values("n", ascending=True))
            fig_m = px.bar(cat_counts, x="n", y="category", orientation="h",
                           title="Series by Category", template="plotly_white",
                           color_discrete_sequence=["#2563eb"])
            fig_m.update_layout(height=340, margin=dict(l=0,r=0,t=40,b=0),
                                xaxis_title="", yaxis_title="", showlegend=False,
                                font=dict(family="DM Mono, monospace", size=11),
                                title_font=dict(size=12, color="#64748b"))
            st.plotly_chart(fig_m, use_container_width=True)