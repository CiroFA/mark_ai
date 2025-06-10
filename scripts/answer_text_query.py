"""
answer_text_query.py

Estrae i chunk di testo più rilevanti dai documenti SEC (10-K, 10-Q),
senza invocare direttamente un LLM.  
Restituisce un dizionario che verrà passato a `llm_wrapper.format_answer`.

API pubblico
------------
answer_question(query: str) -> dict
    {
        "chunks": [
            {
                "filename": "AAPL_10-K_2023_part-17.txt",
                "text":      "…contenuto selezionato…"
            },
            ...
        ],
        "source_docs": ["AAPL_10-K_2023_part-17.txt", ...]
    }
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Dict, Any

# Import relativo: retriever è nello stesso package "scripts"
from .retriever import retrieve_chunks_by_company

# --------------------------------------------------------------------------- #
#  Token counter (fallback a word-based se tiktoken non disponibile)
# --------------------------------------------------------------------------- #

try:
    import tiktoken

    _enc = tiktoken.encoding_for_model("gpt-4")

    def count_tokens(text: str) -> int:  # noqa: D401
        """Conta token con tiktoken."""
        return len(_enc.encode(text))

except ImportError:  # pragma: no cover

    def count_tokens(text: str) -> int:  # noqa: D401
        """Fallback: conta parole."""
        return len(text.split())


_TOKEN_BUDGET = 2000          # limite hard per tutti i chunk
_MAX_CHUNKS   = 6             # cap sul numero di chunk ritornati

# --------------------------------------------------------------------------- #
#  Funzione principale
# --------------------------------------------------------------------------- #


def _read_chunk_file(fname: str) -> str:
    """
    Legge il contenuto del file chunk.

    Parameters
    ----------
    fname : str
        Nome file relativo dentro data/chunks/.

    Returns
    -------
    str
        Testo del chunk o stringa vuota se il file non esiste.
    """
    base_dir = Path(__file__).resolve().parent.parent  # project root
    path = base_dir / "data" / "chunks" / fname
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def answer_question(query: str) -> Dict[str, Any]:
    """
    Recupera i chunk di testo rilevanti per `query`.

    Returns
    -------
    dict
        Dizionario conforme allo schema richiesto da llm_wrapper.
    """
    # 1. Retrieve metadati chunks (semantica, niente filtro ticker)
    meta_chunks = retrieve_chunks_by_company(
        query,
        tickers=None,
        total_k=_MAX_CHUNKS
    )

    if not meta_chunks:  # nessun match
        return {
            "chunks": [],
            "source_docs": [],
        }

    # 2. Carica testo dei chunk e rispetta limite token
    selected_chunks: List[Dict[str, str]] = []
    total_tokens = 0

    for meta in meta_chunks:
        text = _read_chunk_file(meta["filename"])
        if not text:
            continue

        new_token_count = count_tokens(text)
        if total_tokens + new_token_count > _TOKEN_BUDGET:
            break

        selected_chunks.append(
            {
                "filename": meta["filename"],
                "text": text.strip(),
            }
        )
        total_tokens += new_token_count
        # --------------------------------------------------------------- #
    # DEBUG: quali chunk sono stati scelti?
    # --------------------------------------------------------------- #
    print("🟢 Chunk selezionati:")
    for c in selected_chunks:
        token_len = count_tokens(c["text"])
        # cerca il punteggio 'score' nella lista originale meta_chunks
        score = next(
            (m.get("score") for m in meta_chunks if m["filename"] == c["filename"]),
            None
        )
        if score is not None:
            print(f"  • {c['filename']}  |  score={score:.4f}  |  tokens={token_len}")
        else:
            print(f"  • {c['filename']}  |  tokens={token_len}")
    print("-" * 60)
    
    return {
        "chunks": selected_chunks,
        "source_docs": [c["filename"] for c in selected_chunks],
    }


# Alias legacy (se altrove era usato ask_mark)
ask_mark = answer_question