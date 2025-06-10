

"""
ask_mark.py

Entry‑point script for the Mark assistant.  
Usage examples
--------------
Interactive mode:
    $ python ask_mark.py
    (then type your question and press Enter)

One‑liner:
    $ python ask_mark.py "Qual è il net income di Apple nel 2023?"

The script:
1. Acquisisce la domanda dell’utente (CLI o `input()`).
2. La passa a `classify_question.classify_question`.
3. Passa il risultato a `llm_wrapper.format_answer`.
4. Stampa la risposta formattata.

Assumes:
- OPENAI_API_KEY is defined in a .env file at project root.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, Any

from dotenv import load_dotenv

# Local imports
from scripts.classify_question import classify_question
from scripts.llm_wrapper import format_answer

# --------------------------------------------------------------------------- #
#  Environment
# --------------------------------------------------------------------------- #

load_dotenv()  # ensure OPENAI_API_KEY is available to any downstream module

# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #


def _read_question_from_cli() -> str:
    """
    Returns the user's question.

    • If arguments were passed -> join them as the question.
    • Otherwise, prompt via input().

    Returns
    -------
    str
        The user's question as a single string.
    """
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()

    # Interactive prompt
    try:
        return input("Domanda> ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nInterrotto.")
        sys.exit(0)


def _pretty_dump(obj: Dict[str, Any]) -> str:
    """Nicely format a dict for debug prints."""
    return json.dumps(obj, indent=2, ensure_ascii=False)


# --------------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------------- #


def main() -> None:
    """
    Full orchestrator:
    - Collect question
    - Classify and route
    - Format final answer
    - Print result
    """
    question = _read_question_from_cli()
    if not question:
        print("Nessuna domanda fornita.")
        sys.exit(1)

    # 1) Classify and retrieve raw result
    classification_output = classify_question(question)

    # Debug (optional): uncomment next line
    # print("DEBUG raw output:", _pretty_dump(classification_output), file=sys.stderr)

    # 2) Format human‑readable answer
    final_answer: str = format_answer(
        question=question,
        answer_type=classification_output["type"],
        answer_payload=classification_output["result"],
    )

    # 3) Output to user
    print(final_answer)


if __name__ == "__main__":
    main()