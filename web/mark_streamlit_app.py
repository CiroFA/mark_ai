import streamlit as st
import sys
import os

# Add the project root to the path to allow importing ask_mark
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.ask_mark import main as ask_mark_main  # Assumiamo che ask_mark.py abbia una funzione main(question)

# Page configuration
st.set_page_config(page_title="Mark – AI Investment Assistant")

# Title and instructions
st.title("💼 Mark – AI Investment Assistant")
st.markdown("Ask anything about a publicly listed company.")

# User input
user_input = st.text_input("📨 Enter your question:")

# Question processing
if user_input:
    st.markdown("⏳ Processing your question...")
    try:
        result = ask_mark_main(user_input)
        st.success("✅ Answer generated:")
        st.write(result)
    except Exception as e:
        st.error("❌ Error while processing the question.")
        st.exception(e)