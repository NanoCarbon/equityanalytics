import logging
import streamlit as st
from streamlit.db.snowflake import execute_sql_cached
from agents.chart_agent import generate_sql, generate_chart
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
                if "chart" in msg:
                    st.plotly_chart(msg["chart"], use_container_width=True)
                if "sql" in msg:
                    with st.expander("Generated SQL"):
                        st.code(msg["sql"], language="sql")
                if "text" in msg:
                    st.write(msg["text"])
            else:
                st.write(msg["content"])

    # Inline prompt suggestions — only shown on empty conversation
    if not st.session_state.messages:
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
                    claude_messages = [{"role": m["role"], "content": m["content"]}
                                       for m in st.session_state.messages if m["role"] == "user"]
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
                    with st.spinner("Building chart…"):
                        fig = generate_chart(df, prompt)
                    st.plotly_chart(fig, use_container_width=True)
                    with st.expander("Generated SQL"):
                        st.code(sql, language="sql")
                    st.session_state.messages.append({
                        "role": "assistant", "content": prompt, "chart": fig, "sql": sql
                    })

            except Exception as e:
                logger.error("Prompt error: %s", e)
                st.error(f"Something went wrong: {str(e)}")
                with st.expander("Generated SQL"):
                    st.code(sql or "SQL not generated", language="sql")
