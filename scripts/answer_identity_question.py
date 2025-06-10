"""
answer_identity_question.py

Risponde a domande sull'identità e sul funzionamento di Mark, l'assistente
finanziario AI.  Questo modulo è "senza LLM": restituisce risposte rapide
hard‑coded, così non consumiamo token quando l'utente chiede cose come
«Chi sei?» o «Cosa puoi fare?».

Schema di utilizzo
------------------
>>> from scripts.answer_identity_question import answer_question
>>> answer_question("Chi sei?")
"Sono Mark, un assistente AI specializzato ..."

La funzione restituisce SEMPRE una stringa già pronta da stampare.
"""

from __future__ import annotations

import re
from typing import Dict

# --------------------------------------------------------------------------- #
#  Mappa delle domande conosciute → risposta
# --------------------------------------------------------------------------- #

_KNOWN_PATTERNS: Dict[str, str] = {
    r"\bchi\s+sei\b": (
        "Sono Mark, un assistente AI specializzato in finanza aziendale. "
        "Ti aiuto a ottenere velocemente dati numerici (utile, ricavi, ROE, ecc.) "
        "e riassunti dai documenti SEC come 10‑K e 10‑Q."
    ),
    r"\bcosa\s+(puoi|sai)\s+fare\b": (
        "Posso fornirti valori finanziari aggiornati dalle fonti di mercato "
        "(es. Yahoo Finance) e riassumere informazioni chiave dai documenti ufficiali "
        "delle aziende quotate. Gestisco domande numeriche, testuali o ibride."
    ),
    r"\bcome\s+(funzioni|lavori)\b": (
        "Ricevo la tua domanda, la classifico come numerica, testuale o ibrida, "
        "recupero i dati o i testi necessari e poi uso un modello linguistico "
        "per generare una risposta chiara e professionale."
    ),
    r"\baggiornato\b": (
        "Aggiorno i dati numerici tramite la libreria yfinance e i documenti SEC "
        "appena vengono pubblicati. La frequenza di aggiornamento dipende dalle "
        "fonti ufficiali, ma in genere i dati di mercato sono pressoché in tempo reale."
    ),
    r"\bquali\s+informazioni\s+mi\s+puoi\s+dare\b": (
        "Posso darti valori di bilancio (utile, ricavi, EPS, dividendi), "
        "indicatori di mercato (beta, capitalizzazione) e riassunti di sezioni "
        "rilevanti dei documenti 10‑K / 10‑Q."
    ),
}

_DEFAULT_RESPONSE = (
    "Sono Mark, il tuo assistente AI per l'analisi finanziaria. "
    "Posso aiutarti con dati numerici su aziende quotate e riassunti dai loro documenti ufficiali. "
    "Chiedimi pure!"
)

# --------------------------------------------------------------------------- #
#  Funzione pubblica
# --------------------------------------------------------------------------- #


def answer_question(question: str) -> str:
    """
    Restituisce una risposta testuale a domande sull'identità/funzionalità.

    Parameters
    ----------
    question : str
        La domanda dell'utente.

    Returns
    -------
    str
        Risposta pronta da mostrare.
    """
    q = question.lower()
    for pattern, reply in _KNOWN_PATTERNS.items():
        if re.search(pattern, q):
            return reply
    return _DEFAULT_RESPONSE


# --------------------------------------------------------------------------- #
#  CLI di debug rapido
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import sys
    user_q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Chi sei?"
    print(answer_question(user_q))
