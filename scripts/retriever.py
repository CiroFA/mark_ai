import os
import json
import faiss
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

# 📁 Percorsi
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

INDEX_PATH = os.path.join(ROOT_DIR, "data", "index", "company_index.faiss")
METADATA_PATH = os.path.join(ROOT_DIR, "data", "index", "metadata.json")
EMBEDDING_MODEL = "text-embedding-3-small"

# 🔐 API Key
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# 📦 Carica FAISS e metadati
index = faiss.read_index(INDEX_PATH)
with open(METADATA_PATH, "r", encoding="utf-8") as f:
    metadata = json.load(f)

def get_query_embedding(query):
    response = client.embeddings.create(
        input=[query],
        model=EMBEDDING_MODEL
    )
    return np.array(response.data[0].embedding, dtype="float32").reshape(1, -1)

def retrieve_chunks_by_company(query, tickers=None, total_k=8, per_company_k=2):
    query_vector = get_query_embedding(query)
    distances, indices = index.search(query_vector, 50)

    results = []
    tickers_found = {t: 0 for t in tickers} if tickers else {}

    for dist, idx in zip(distances[0], indices[0]):
        if idx >= len(metadata):
            continue
        meta = metadata[idx]
        fname = meta["filename"]
        with open(os.path.join(ROOT_DIR, meta["path"]), "r", encoding="utf-8") as f:
            text = f.read()

        chunk = {
            "filename": fname,
            "distance": round(dist, 4),
            "text": text.strip()
        }

        # ✅ Almeno per_company_k per ciascun ticker se specificato
        matched = False
        if tickers:
            for t in tickers:
                if fname.startswith(t) and tickers_found[t] < per_company_k:
                    results.append(chunk)
                    tickers_found[t] += 1
                    matched = True
                    break
        if matched:
            continue

        # ✅ Completa fino a total_k
        if len(results) < total_k:
            results.append(chunk)

        if len(results) >= total_k and (not tickers or all(v >= per_company_k for v in tickers_found.values())):
            break

    return results
