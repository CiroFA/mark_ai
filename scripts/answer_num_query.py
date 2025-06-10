"""
answer_num_query.py

Risponde a domande numeriche restituendo un dizionario strutturato
compatibile con la nuova pipeline di Mark.

Schema di output
----------------
{
    "result": <float|int>,
    "company": <str>,
    "table": <str>,
    "column": <str>,
    "period": <str>,
    "function_used": "answer_value_query" | "answer_avg_query"
}
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any

from dotenv import load_dotenv
import os
from openai import OpenAI

# DB helpers
from scripts.db.parse_num_query import parse_numerical_question
from scripts.db.value_query import answer_value_query
from scripts.db.parse_avg_query import parse_avg_question
from scripts.db.avg_query import answer_avg_query

# --------------------------------------------------------------------------- #
#  OpenAI setup
# --------------------------------------------------------------------------- #

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MODEL_NAME = "gpt-4o-mini"
TEMPERATURE = 0.0

_CHOOSE_FUNC_SYSTEM = (
    "Sei un assistente che deve decidere quale funzione chiamare per rispondere "
    "a una domanda finanziaria.\n\n"
    "• Usa 'answer_value_query' se l'utente chiede un singolo valore puntuale, "
    "come 'net debt di Apple nel 2023'.\n"
    "• Usa 'answer_avg_query' se l'utente chiede una media su un intervallo, "
    "come 'ricavi medi di Apple negli ultimi 5 anni'.\n\n"
    "Rispondi SOLO con una delle due stringhe: 'answer_value_query' oppure "
    "'answer_avg_query'. Nessuna spiegazione."
)


# --------------------------------------------------------------------------- #
#  Internal helpers
# --------------------------------------------------------------------------- #

def _choose_function(question: str) -> str:
    """
    Decide se usare answer_value_query o answer_avg_query.

    Parameters
    ----------
    question : str
        Domanda grezza dell'utente.

    Returns
    -------
    str
        'answer_value_query' o 'answer_avg_query'
    """
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            temperature=TEMPERATURE,
            messages=[
                {"role": "system", "content": _CHOOSE_FUNC_SYSTEM},
                {"role": "user", "content": question.strip()}
            ]
        )
        choice = response.choices[0].message.content.strip()
        if choice not in {"answer_value_query", "answer_avg_query"}:
            raise ValueError(f"Scelta non valida: {choice}")
        return choice
    except Exception as exc:  # noqa: BLE001
        logging.error("❌ Errore nella scelta della funzione: %s", exc, exc_info=True)
        # Fallback euristico rapido
        q_low = question.lower()
        if any(w in q_low for w in ["media", "average", "mean"]):
            return "answer_avg_query"
        return "answer_value_query"


# --------------------------------------------------------------------------- #
#  Public API
# --------------------------------------------------------------------------- #

def answer_question(question: str) -> Dict[str, Any]:
    """
    Elabora la domanda numerica e restituisce un dizionario strutturato.

    Parameters
    ----------
    question : str
        Domanda originale dell'utente.

    Returns
    -------
    dict
        Dizionario secondo lo schema definito in testa al file.
    """
    chosen = _choose_function(question)

    if chosen == "answer_avg_query":
        parsed = parse_avg_question(question)
        if not parsed:
            raise ValueError("Impossibile interpretare la domanda sulla media.")
        result = answer_avg_query(parsed)

    else:  # answer_value_query
        parsed = parse_numerical_question(question)
        if not parsed:
            raise ValueError("Impossibile interpretare la domanda numerica.")
        result = answer_value_query(parsed)

    if result is None:
        raise ValueError("Nessun dato trovato nel database.")

    parsed["result"] = result
    parsed["function_used"] = chosen
    return parsed


# --------------------------------------------------------------------------- #
#  CLI per debug rapido
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    try:
        q = input("Domanda> ").strip()
        print(answer_question(q))
    except Exception as err:
        print(f"Errore: {err}")