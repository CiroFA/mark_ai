"""
retriever.py

Recupera, tramite FAISS, i chunk di documento più affini a una query.
Aggiunge nel risultato il campo «score» (distanza FAISS) e permette
un filtro opzionale per ticker.

API principale
--------------
retrieve_chunks_by_company(query, tickers=None, total_k=8, per_company_k=2)
    → List[dict]  # metadati + score
"""
from __future__ import annotations

import os
import json
from pathlib import Path
from typing import List, Dict, Any

import faiss
import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

# --------------------------------------------------------------------------- #
#  Percorsi
# --------------------------------------------------------------------------- #

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

INDEX_PATH = ROOT_DIR / "data" / "index" / "company_index.faiss"
METADATA_PATH = ROOT_DIR / "data" / "index" / "metadata.json"

# Modello embedding: override con variabile d’ambiente se serve
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

# --------------------------------------------------------------------------- #
#  Inizializzazione OpenAI e caricamento FAISS + metadati
# --------------------------------------------------------------------------- #

load_dotenv()  # carica eventuale .env a livello progetto
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Carica index e metadati una sola volta (module-level cache)
_index = faiss.read_index(str(INDEX_PATH))
with METADATA_PATH.open(encoding="utf-8") as f:
    _metadata: list[dict[str, Any]] = json.load(f)


# --------------------------------------------------------------------------- #
#  Helper
# --------------------------------------------------------------------------- #

def _get_query_embedding(query: str) -> np.ndarray:
    """Restituisce l’embedding (np.ndarray shape 1×d) della query."""
    resp = client.embeddings.create(
        input=[query],
        model=EMBEDDING_MODEL,
    )
    return np.asarray(resp.data[0].embedding, dtype="float32").reshape(1, -1)


# --------------------------------------------------------------------------- #
#  API pubblica
# --------------------------------------------------------------------------- #

def retrieve_chunks_by_company(
    query: str,
    tickers: list[str] | None = None,
    total_k: int = 8,
    per_company_k: int = 2,
    company_hint: str | None = None,   # riservato per filtri futuri
) -> List[Dict[str, Any]]:
    """
    Restituisce i metadati dei chunk più simili alla *query*.

    Parameters
    ----------
    query : str
        Testo della domanda dell’utente.
    tickers : list[str] | None
        Se fornito, limita i risultati ai file che contengono
        uno dei ticker indicati nel filename.
    total_k : int
        Numero massimo di chunk totali che verranno ritornati.
    per_company_k : int
        Limite di chunk per singola azienda se `tickers` è valorizzato.
    company_hint : str | None
        Placeholder per eventuali filtri aggiuntivi (non usato ora).

    Returns
    -------
    list[dict]
        Ogni dict proviene da `metadata.json` e contiene in più la chiave
        ``score`` (distanza FAISS; più bassa ⇒ match migliore).
    """
    query_vec = _get_query_embedding(query)
    distances, indices = _index.search(query_vec, 50)

    results: list[dict[str, Any]] = []
    tickers_found = {t.upper(): 0 for t in tickers} if tickers else {}

    for dist, idx in zip(distances[0], indices[0]):
        if idx >= len(_metadata):
            continue

        meta = _metadata[idx]
        fname = meta["filename"]
        score = float(dist)

        # Filtro ticker (case-insensitive)
        if tickers and not any(t.upper() in fname.upper() for t in tickers):
            continue

        if tickers:
            for t in tickers:
                up = t.upper()
                if up in fname.upper() and tickers_found[up] < per_company_k:
                    results.append({**meta, "score": score})
                    tickers_found[up] += 1
                    break
        else:
            results.append({**meta, "score": score})

        if len(results) >= total_k:
            break

    return results