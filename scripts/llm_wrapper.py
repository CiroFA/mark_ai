"""
llm_wrapper.py

Converte un payload (numerical, text o hybrid) in una risposta
chiara e professionale usando GPT-4o.

Funzione principale
-------------------
format_answer(question: str,
              answer_type: str,
              answer_payload: dict) -> str
"""

from __future__ import annotations

import json
import logging
import os
from typing import Dict, Any, List

from openai import OpenAI 
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
#  Environment
# --------------------------------------------------------------------------- #

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MODEL_NAME = "gpt-4o-mini"
TEMPERATURE = 0.2
MAX_TOKENS = 512

# --------------------------------------------------------------------------- #
#  Prompt helpers
# --------------------------------------------------------------------------- #


def _numerical_system() -> str:
    return (
        "Sei un analista finanziario. Devi produrre una risposta sintetica ma "
        "completa basata su un singolo valore numerico estratto da un database. "
        "Spiega brevemente cosa rappresenta il numero e perché è rilevante."
    )


def _text_system() -> str:
    return (
        "Sei un analista finanziario. Puoi rispondere solo usando i contenuti testuali forniti dall’utente, che provengono da documenti SEC (10-K, 10-Q). "
        "Non devi aggiungere informazioni inventate o che non sono presenti nei testi forniti. Se l’informazione non è nei chunk, devi dirlo esplicitamente."
    )


def _hybrid_system() -> str:
    return (
        "Sei un analista finanziario senior. Devi integrare un valore numerico "
        "e un estratto testuale in un'unica risposta chiara e coesa. "
        "Spiega prima il dato numerico, poi aggiungi il contesto testuale."
    )


def _build_messages(question: str, answer_type: str, payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Costruisce la lista di messaggi per ChatCompletion."""
    if answer_type == "numerical":
        system_prompt = _numerical_system()
        user_content = json.dumps(
            {
                "question": question,
                "numerical_result": payload
            },
            ensure_ascii=False,
            indent=2
        )

    elif answer_type == "text":
        system_prompt = _text_system()
        user_content = json.dumps(
            {
                "question": question,
                "text_chunks": payload
            },
            ensure_ascii=False,
            indent=2
        )

    else:  # hybrid
        system_prompt = _hybrid_system()
        user_content = json.dumps(
            {
                "question": question,
                "numerical_result": payload["numerical"],
                "text_chunks": payload["text"]
            },
            ensure_ascii=False,
            indent=2
        )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content}
    ]


# --------------------------------------------------------------------------- #
#  Public API
# --------------------------------------------------------------------------- #


def format_answer(
    question: str,
    answer_type: str,
    answer_payload: Dict[str, Any]
) -> str:
    """
    Converte il risultato grezzo in risposta umana.

    Parameters
    ----------
    question : str
        La domanda originale dell’utente.
    answer_type : str
        'numerical', 'text' o 'hybrid'.
    answer_payload : dict
        I dati ritornati dagli answer module.

    Returns
    -------
    str
        La risposta generata da GPT-4o.
    """
    try:
        msgs = _build_messages(question, answer_type, answer_payload)

        response = client.chat.completions.create(
            model=MODEL_NAME,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            messages=msgs
        )
        return response.choices[0].message.content.strip()

    except Exception as exc:  # noqa: BLE001
        logging.error("❌ llm_wrapper failure: %s", exc, exc_info=True)
        # Fallback: restituisce payload grezzo
        return f"(Errore LLM)\n\n{json.dumps(answer_payload, ensure_ascii=False, indent=2)}"


# --------------------------------------------------------------------------- #
#  CLI di test rapido
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    demo_question = "Quanto è stato il net debt di Apple nel 2023 e cosa dice il 10-K in merito?"
    demo_payload = {
        "numerical": {
            "result": 5.23,
            "company": "AAPL",
            "table": "fundamentals",
            "column": "netDebt",
            "period": "FY2023",
            "function_used": "answer_value_query"
        },
        "text": {
            "chunks": ["Nel 10-K 2023 Apple sottolinea che…"],
            "source_docs": ["10-K_2023_AAPL.html"]
        }
    }
    print(format_answer(demo_question, "hybrid", demo_payload))
