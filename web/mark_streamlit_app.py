import streamlit as st
import sys
import os

# Aggiungiamo il path alla cartella del progetto per poter importare ask_mark
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.ask_mark import main as ask_mark_main  # Assumiamo che ask_mark.py abbia una funzione main(question)

# Configurazione della pagina
st.set_page_config(page_title="Mark – AI Investment Assistant")

# Titolo e istruzioni
st.title("💼 Mark – AI Investment Assistant")
st.markdown("Chiedi qualsiasi cosa su un'azienda quotata in borsa.\n"
            "Esempio: *'Qual è stato il Net Income di Meta nel 2022?'*")

# Input utente
user_input = st.text_input("📨 Inserisci la tua domanda:")

# Elaborazione della domanda
if user_input:
    st.markdown("⏳ Sto elaborando la risposta...")
    try:
        result = ask_mark_main(user_input)
        st.success("✅ Risposta generata:")
        st.write(result)
    except Exception as e:
        st.error("❌ Errore durante l'elaborazione della domanda.")
        st.exception(e)