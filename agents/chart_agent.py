import json
import logging
import pandas as pd
import plotly.express as px
from anthropic import Anthropic
from agents.prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

client = Anthropic()

CHART_SYSTEM_PROMPT = """Return ONLY a JSON object with keys:
chart_type (line/bar/scatter), x (column name), y (column name),
color (column name or null), title (string).
Use only column names from the provided list. No markdown, no explanation."""


def generate_sql(messages: list) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=messages,
        timeout=30,
    )
    return response.content[0].text.strip()


def _parse_chart_config(raw: str) -> dict:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(l for l in lines[1:] if l.strip() != "```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Chart config parse failed, using fallback")
        return {"chart_type": "line", "x": None, "y": None, "color": None, "title": "Chart"}


def generate_chart(df: pd.DataFrame, user_prompt: str):
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        system=CHART_SYSTEM_PROMPT,
        messages=[{"role": "user", "content":
            f"Request: {user_prompt}\nColumns: {list(df.columns)}\nSample: {df.head(2).to_dict()}"}],
        timeout=15,
    )
    config = _parse_chart_config(response.content[0].text)
    cols = list(df.columns)

    if not config.get("x") or config["x"] not in df.columns:
        config["x"] = cols[0]
    if not config.get("y") or config["y"] not in df.columns:
        numeric = df.select_dtypes("number").columns.tolist()
        config["y"] = next((c for c in numeric if c != config["x"]), cols[-1])
    if config.get("color") and config["color"] not in df.columns:
        config["color"] = None

    build = {"line": px.line, "bar": px.bar, "scatter": px.scatter}.get(config["chart_type"], px.line)
    fig = build(df, x=config["x"], y=config["y"], color=config.get("color"),
                title=config.get("title", ""), template="plotly_white")
    fig.update_layout(
        xaxis_title=config["x"].replace("_", " ").title(),
        yaxis_title=config["y"].replace("_", " ").title(),
        legend_title="Ticker" if config.get("color") == "ticker" else "",
        font=dict(family="DM Mono, monospace", size=11),
    )
    return fig