import logging
import streamlit as st
from db.snowflake import execute_sql_cached
from agents.chart_agent import generate_sql, analyse_and_chart
from agents.prompts import EXAMPLE_PROMPTS

logger = logging.getLogger(__name__)


def render_chat():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_prompt" not in st.session_state:
        st.session_state.pending_prompt = None

    # Chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                if "insight" in msg and msg["insight"]:
                    safe = msg["insight"].replace("$", r"\$")
                    st.write(safe)
                if "chart" in msg:
                    st.plotly_chart(msg["chart"], use_container_width=True)
                if "sql" in msg:
                    with st.expander("Generated SQL"):
                        st.code(msg["sql"], language="sql")
                if "text" in msg:
                    st.write(msg["text"])
            else:
                st.write(msg["content"])

    # Suggestions — always visible so user can re-run or try another prompt.
    # Shown above the chat input with a light separator if there is history.
    if st.session_state.messages:
        st.markdown("---")
    st.markdown("**Ask a question or choose a prompt:**")
    st.write("")
    cols = st.columns(len(EXAMPLE_PROMPTS))
    for col, p in zip(cols, EXAMPLE_PROMPTS):
        with col:
            if st.button(p, key=f"suggestion_{p[:40]}", use_container_width=True):
                st.session_state.pending_prompt = p
                st.rerun()

    # Chat input
    prompt = st.chat_input("Type here...")
    if st.session_state.pending_prompt:
        prompt = st.session_state.pending_prompt
        st.session_state.pending_prompt = None

    if prompt:
        with st.chat_message("user"):
            st.write(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        logger.info("User prompt: %.200s", prompt)

        with st.chat_message("assistant"):
            sql = None
            try:
                with st.spinner("Generating SQL…"):
                    # Build full conversation history so Claude understands
                    # prior queries and doesn't repeat or confuse them.
                    # Assistant turns include the SQL that was generated
                    # so Claude has full context when writing the next query.
                    claude_messages = []
                    for m in st.session_state.messages:
                        if m["role"] == "user":
                            claude_messages.append({
                                "role": "user",
                                "content": m["content"]
                            })
                        elif m["role"] == "assistant" and "sql" in m:
                            claude_messages.append({
                                "role": "assistant",
                                "content": f"I generated this SQL:\n```sql\n{m['sql']}\n```"
                            })
                    sql = generate_sql(claude_messages)

                with st.spinner("Querying Snowflake…"):
                    df = execute_sql_cached(sql)

                if df.empty:
                    st.write("The query returned no results. Try rephrasing your request.")
                    st.session_state.messages.append({
                        "role": "assistant", "content": "No results.",
                        "text": "The query returned no results.", "sql": sql
                    })
                else:
                    with st.spinner("Analysing data…"):
                        config, fig = analyse_and_chart(df, prompt)

                    insight = config.get("insight", "")
                    if insight:
                        safe_insight = insight.replace("$", r"\$")
                        st.write(safe_insight)

                    st.plotly_chart(fig, use_container_width=True)

                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")

                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": prompt,
                        "insight": insight,
                        "chart": fig,
                        "sql": sql,
                    })

            except Exception as e:
                logger.error("Prompt error: %s", e)
                st.error(f"Something went wrong: {str(e)}")
                with st.expander("Generated SQL"):
                    st.code(sql or "SQL not generated", language="sql")