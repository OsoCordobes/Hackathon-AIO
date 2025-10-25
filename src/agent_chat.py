# agent_chat.py
import streamlit as st
from src.agent import build_agent

st.set_page_config(page_title="Jarvis — Chat Agent", layout="wide")
st.title("Jarvis — Chat Agent")

if "agent" not in st.session_state:
    st.session_state.agent = build_agent()
if "chat" not in st.session_state:
    st.session_state.chat = []

for role, msg in st.session_state.chat:
    with st.chat_message(role):
        st.markdown(msg)

q = st.chat_input("Describe the situation...")
if q:
    st.session_state.chat.append(("user", q))
    with st.chat_message("user"):
        st.markdown(q)
    with st.chat_message("assistant"):
        try:
            out = st.session_state.agent.invoke({"input": q})
            ans = out.get("output", str(out))
        except Exception as e:
            ans = f"Error: {e}"
        st.markdown(ans)
        st.session_state.chat.append(("assistant", ans))
