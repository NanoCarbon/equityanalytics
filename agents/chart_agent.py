import json
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from anthropic import Anthropic
from agents.prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

client = Anthropic()

CHART_SYSTEM_PROMPT = """You are a data visualization and financial analysis expert.

Given a DataFrame and the user's original question, you must:
1. Write a concise analytical insight (2-4 sentences) about what the data shows
2. Determine the best chart type and configuration

Return ONLY a valid JSON object with exactly these keys:
- "insight": string — analytical observation about the data (mention specific values, trends, comparisons)
- "chart_type": one of "line", "bar", "grouped_bar", "area", "scatter", "dual_axis", "heatmap"
- "x": column name for x axis
- "y": column name for y axis (for dual_axis, this is the primary y axis)
- "y2": column name for secondary y axis (only for dual_axis, otherwise null)
- "color": column name for color grouping (or null)
- "title": short descriptive chart title

Chart type selection guide:
- "line": time series for one or few metrics over time
- "area": cumulative returns, percentage growth over time  
- "bar": ranking or single-period comparisons across categories
- "grouped_bar": comparing multiple metrics across categories side by side
- "scatter": correlation between two numeric variables
- "dual_axis": two metrics with very different scales on same time axis (e.g. price + volume, stock + macro)
- "heatmap": correlation matrix or multi-ticker multi-period grid

Use only column names that exist in the provided columns list.
Return raw JSON only — no markdown, no explanation.

Important: In the insight field, never use dollar signs ($) before numbers.
Write "394B" not "$394B", "100B revenue" not "$100B revenue".
Streamlit renders dollar signs as LaTeX math which breaks the display."""


def generate_sql(messages: list) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=messages,
        timeout=30,
    )
    return response.content[0].text.strip()


def _parse_response(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(l for l in lines[1:] if l.strip() != "```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Chart config parse failed, using fallback")
        return {
            "insight": "",
            "chart_type": "line",
            "x": None, "y": None, "y2": None,
            "color": None, "title": "Chart"
        }


def _maybe_melt(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    """
    If the DataFrame has multiple numeric series that should all be plotted
    as lines (e.g. close_price + moving_avg_30d for a single ticker),
    melt it from wide to long so both become rows under a single y column
    with a 'series' color grouping.

    Only triggers when:
    - chart_type is line or area
    - there is no color column already set
    - there are 2+ numeric columns beyond the x axis
    - the config y2 is not set (dual_axis handles that separately)
    """
    chart_type = config.get("chart_type", "line")
    if chart_type not in ("line", "area"):
        return df, config
    if config.get("color") or config.get("y2"):
        return df, config

    x = config["x"]
    numeric_cols = [c for c in df.select_dtypes("number").columns if c != x]

    # Only melt when there are exactly 2-4 numeric columns and no ticker/color
    # grouping — more than 4 series on one chart becomes unreadable
    if len(numeric_cols) < 2 or len(numeric_cols) > 4:
        return df, config

    # Check the data isn't already long-format (multiple tickers stacked)
    # by looking for a low-cardinality string column that looks like a grouper
    str_cols = df.select_dtypes("object").columns.tolist()
    already_grouped = any(df[c].nunique() <= 20 for c in str_cols if c != x)
    if already_grouped:
        return df, config

    id_cols = [c for c in df.columns if c not in numeric_cols]
    melted = df.melt(id_vars=id_cols, value_vars=numeric_cols,
                     var_name="series", value_name="value")
    config = dict(config)
    config["y"] = "value"
    config["color"] = "series"
    return melted, config


def analyse_and_chart(df: pd.DataFrame, user_prompt: str) -> tuple[dict, object]:
    """
    Single LLM call that returns both an analytical insight and chart configuration.
    Returns (config_dict, plotly_figure).
    """
    sample = df.head(5).to_dict()
    col_types = {col: str(df[col].dtype) for col in df.columns}
    numeric_cols = df.select_dtypes("number").columns.tolist()

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=CHART_SYSTEM_PROMPT,
        messages=[{"role": "user", "content":
            f"User question: {user_prompt}\n"
            f"Columns and types: {col_types}\n"
            f"Numeric columns available: {numeric_cols}\n"
            f"Row count: {len(df)}\n"
            f"Sample data: {sample}"}],
        timeout=20,
    )

    config = _parse_response(response.content[0].text)
    cols = list(df.columns)

    # Resolve missing/invalid axis columns
    if not config.get("x") or config["x"] not in df.columns:
        config["x"] = cols[0]
    if not config.get("y") or config["y"] not in df.columns:
        config["y"] = next((c for c in numeric_cols if c != config["x"]), cols[-1])
    if config.get("color") and config["color"] not in df.columns:
        config["color"] = None
    if config.get("y2") and config["y2"] not in df.columns:
        config["y2"] = None

    # Auto-melt wide DataFrames with multiple numeric series into long format
    df, config = _maybe_melt(df, config)

    fig = _build_figure(df, config)
    return config, fig


def _build_figure(df: pd.DataFrame, config: dict):
    chart_type = config.get("chart_type", "line")
    x = config["x"]
    y = config["y"]
    color = config.get("color")
    title = config.get("title", "")
    y2 = config.get("y2")

    common = dict(template="plotly_white")
    layout_font = dict(family="DM Mono, monospace", size=11)

    if chart_type == "area":
        fig = px.area(df, x=x, y=y, color=color, title=title, **common)

    elif chart_type == "bar":
        fig = px.bar(df, x=x, y=y, color=color, title=title,
                     barmode="relative", **common)

    elif chart_type == "grouped_bar":
        fig = px.bar(df, x=x, y=y, color=color, title=title,
                     barmode="group", **common)

    elif chart_type == "scatter":
        fig = px.scatter(df, x=x, y=y, color=color, title=title,
                         trendline="ols" if not color else None, **common)

    elif chart_type == "dual_axis" and y2:
        color_vals = df[color].unique() if color else [None]
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        colors_primary   = px.colors.qualitative.Plotly
        colors_secondary = ["#f97316", "#84cc16", "#a855f7"]

        if color:
            for i, grp in enumerate(df[color].unique()):
                sub = df[df[color] == grp]
                fig.add_trace(
                    go.Scatter(x=sub[x], y=sub[y], name=f"{grp} ({y})",
                               line=dict(color=colors_primary[i % len(colors_primary)])),
                    secondary_y=False
                )
            fig.add_trace(
                go.Scatter(x=df[x], y=df[y2], name=y2,
                           line=dict(color=colors_secondary[0], dash="dot")),
                secondary_y=True
            )
        else:
            fig.add_trace(
                go.Scatter(x=df[x], y=df[y], name=y,
                           line=dict(color=colors_primary[0])),
                secondary_y=False
            )
            fig.add_trace(
                go.Scatter(x=df[x], y=df[y2], name=y2,
                           line=dict(color=colors_secondary[0], dash="dot")),
                secondary_y=True
            )

        fig.update_layout(title=title, font=layout_font, **common)
        fig.update_yaxes(title_text=y.replace("_", " ").title(), secondary_y=False)
        fig.update_yaxes(title_text=y2.replace("_", " ").title(), secondary_y=True)
        return fig

    elif chart_type == "heatmap":
        numeric_cols = df.select_dtypes("number").columns.tolist()
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            fig = px.imshow(corr, text_auto=".2f", title=title,
                            color_continuous_scale="RdBu_r", **common)
        else:
            fig = px.imshow(df.set_index(x)[[y]], title=title, **common)

    else:
        # Default: line
        fig = px.line(df, x=x, y=y, color=color, title=title, **common)

    fig.update_layout(
        font=layout_font,
        xaxis_title=x.replace("_", " ").title(),
        yaxis_title=y.replace("_", " ").title(),
        legend_title=color.replace("_", " ").title() if color else "",
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1) if color else {}
    )
    return fig