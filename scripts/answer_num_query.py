import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from openai import OpenAI
from dotenv import load_dotenv
from scripts.db.parse_num_query import parse_numerical_question
from scripts.db.value_query import answer_value_query
from scripts.db.parse_avg_query import parse_avg_question
from scripts.db.avg_query import answer_avg_query

# Carica l'API Key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def ask_mark(user_question: str) -> str:
    # Step 1: Classificazione intelligente del tipo di domanda
    classification_prompt = f"""
Hai ricevuto questa domanda da un utente: \"{user_question}\"

Hai a disposizione due funzioni che puoi utilizzare per rispondere:

1. answer_value_query → serve per rispondere a domande in cui l’utente vuole ottenere un valore preciso, l’ultima osservazione disponibile o un singolo dato (es. Net Debt di Apple, beta di Tesla, utili di Microsoft).

2. answer_avg_query → serve per domande in cui l’utente vuole calcolare una media su un certo intervallo temporale (es. prezzo medio nel 2023, ricavi medi degli ultimi 5 anni, media del cash flow di Meta).

Rispondi solo con il nome della funzione più appropriata tra:
- answer_value_query
- answer_avg_query
"""

    try:
        classification_response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Sei un assistente esperto che deve scegliere la funzione corretta da chiamare per una domanda."},
                {"role": "user", "content": classification_prompt}
            ],
            temperature=0
        )
        chosen_function = classification_response.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ Errore nella classificazione della domanda: {e}"

    # Step 2: Parsing + chiamata funzione corretta
    if chosen_function == "answer_avg_query":
        parsed = parse_avg_question(user_question)
        function_called = "answer_avg_query"
        if not parsed or not all(k in parsed for k in ["company", "table", "column", "period"]):
            return "❌ Non sono riuscito a interpretare la domanda sulla media in modo strutturato."
        result = answer_avg_query(parsed)

    elif chosen_function == "answer_value_query":
        parsed = parse_numerical_question(user_question)
        function_called = "answer_value_query"
        if not parsed or not all(k in parsed for k in ["company", "table", "column", "period"]):
            return "❌ Non sono riuscito a interpretare la domanda in modo strutturato."
        result = answer_value_query(parsed)

    else:
        return f"❌ La funzione scelta dal classificatore non è valida: {chosen_function}"

    if result is None:
        return "❌ Nessun dato trovato nel database per la metrica richiesta."

    # Step 2: Costruzione prompt per risposta finale
    prompt = f"""
L'utente ha chiesto: \"{user_question}\"

Il database ha restituito il valore: {result}
- Tabella: {parsed['table']}
- Colonna: {parsed['column']}
- Azienda (ticker): {parsed['company']}
- Periodo: {parsed['period']}
- Funzione SQL eseguita: {function_called}

Scrivi una risposta dettagliata, professionale e chiara che spieghi all’utente cosa rappresenta questo valore, cosa significa nel contesto della domanda, e quale può essere la sua rilevanza per un analista o un investitore.
""".strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Sei un assistente esperto di finanza aziendale."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5
        )
        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"❌ Errore durante la generazione della risposta: {e}"

if __name__ == "__main__":
    print("💬 Inserisci la tua domanda per Mark:")
    user_input = input("> ")
    response = ask_mark(user_input)
    print("\n🧠 Risposta di Mark:")
    print(response)