

"""
classify_question.py

Classifies an incoming user question as 'numerical', 'text', or 'hybrid'
and routes it to the appropriate answer module.

Returned object schema
----------------------
{
    "type": "<numerical | text | hybrid>",
    "result": {
        # for numerical
        "result": ...,
        "company": ...,
        "table": ...,
        "column": ...,
        "period": ...,
        "function_used": ...,

        # for text
        "chunks": [...],
        "source_docs": [...],

        # for hybrid
        "numerical": { ... },   # same as numerical schema
        "text": { ... }         # same as text schema
    }
}
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Dict, Any

from dotenv import load_dotenv
import os
from openai import OpenAI         # ← nuova API

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Local imports (package‑relative)
from .answer_num_query import answer_question as answer_num_question
from .answer_text_query import answer_question as answer_text_question

# --------------------------------------------------------------------------- #
#  Configuration
# --------------------------------------------------------------------------- #

MODEL_NAME = "gpt-4o-mini"  # or the model ID you prefer
TEMPERATURE = 0.0           # deterministic classification
MAX_TOKENS = 64

# --------------------------------------------------------------------------- #
#  Prompt template
# --------------------------------------------------------------------------- #

_SYSTEM_PROMPT = (
    "You are an assistant that receives a user's financial question. "
    "Your ONLY task is to decide whether the question requires:\n"
    "• A **numerical** answer that must be fetched from a SQL database (label: numerical)\n"
    "• A **textual** answer that must be found in SEC filings such as 10‑K, 10‑Q (label: text)\n"
    "• **Both** a numerical value **and** textual context from SEC filings (label: hybrid)\n\n"
    "Respond ONLY with a JSON dictionary having exactly one key 'type' whose value "
    "is either 'numerical', 'text', or 'hybrid'.\n"
    "Output example: {\"type\": \"numerical\"}\n\n"
    "Do NOT add any other keys. Do NOT add explanations."
)


# --------------------------------------------------------------------------- #
#  Core function
# --------------------------------------------------------------------------- #

def _classify(question: str) -> str:
    """
    Runs the classification prompt and returns one of:
    'numerical', 'text', or 'hybrid'.

    Parameters
    ----------
    question : str
        Raw user question.

    Returns
    -------
    str
        The label predicted by the model.
    """
    try:
        msgs = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": question.strip()}
        ]
        response = client.chat.completions.create(
            model=MODEL_NAME,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            messages=msgs
        )
        raw_content: str = response.choices[0].message.content.strip()
        prediction: Dict[str, str] = json.loads(raw_content)
        label = prediction.get("type", "").lower()
        if label not in {"numerical", "text", "hybrid"}:
            raise ValueError(f"Invalid label '{label}' returned by model.")
        return label
    except Exception as exc:  # noqa: BLE001
        logging.error("❌ Classification failed: %s", exc, exc_info=True)
        # Fallback to 'text' so we at least try retrieval
        return "text"


def classify_question(question: str) -> Dict[str, Any]:
    """
    Classify the question and route it to the appropriate answer module.

    Parameters
    ----------
    question : str
        The user question.

    Returns
    -------
    dict
        Structured result ready for downstream processing.
    """
    label = _classify(question)
    print(f"📊 Classificazione della domanda: {label}")

    if label == "numerical":
        numerical_result = answer_num_question(question)
        return {"type": "numerical", "result": numerical_result}

    if label == "text":
        text_result = answer_text_question(question)
        print("📘 answer_text_query.answer_question è stata invocata")
        return {"type": "text", "result": text_result}

    # Hybrid: get both
    numerical_result = answer_num_question(question)
    text_result = answer_text_question(question)
    print("🔀 Esecuzione combinata: numerico + testuale")
    return {
        "type": "hybrid",
        "result": {
            "numerical": numerical_result,
            "text": text_result,
        },
    }


# --------------------------------------------------------------------------- #
#  CLI helper
# --------------------------------------------------------------------------- #

def _cli() -> None:
    """
    Allow quick testing from the command line:
        python classify_question.py "Your question here"
    """
    if len(sys.argv) < 2:
        print("Usage: python classify_question.py \"Your question here\"")
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    output = classify_question(question)
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    _cli()