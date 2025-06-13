#!/usr/bin/env python3
"""
Script ottimizzato per indicizzare oltre un milione di chunk senza esaurire la RAM.
- Elabora i chunk in batch (configurabile via BATCH_SIZE) invece di caricarli tutti in memoria.
- Aggiunge gli embedding al passo, salvando periodicamente l'indice FAISS e i metadati.
- Supporta ripresa automatica se viene interrotto.
"""

import os
import json
import faiss
import numpy as np
from tqdm import tqdm
from openai import OpenAI
from dotenv import load_dotenv

# 📁 Percorsi coerenti con la struttura del progetto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

CHUNKS_FOLDER = os.path.join(ROOT_DIR, "data", "chunks")
INDEX_FOLDER = os.path.join(ROOT_DIR, "data", "index")
os.makedirs(INDEX_FOLDER, exist_ok=True)

INDEX_PATH = os.path.join(INDEX_FOLDER, "company_index.faiss")
METADATA_PATH = os.path.join(INDEX_FOLDER, "metadata.json")
EMBEDDING_MODEL = "text-embedding-3-small"

# ⚙️ Config
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Puoi cambiare con variabile d'ambiente
SAVE_EVERY = int(os.getenv("SAVE_EVERY", "10000"))  # Salva l'indice ogni N chunk

# 🔐 Carica la chiave API da .env
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if api_key is None:
    raise ValueError("❌ OPENAI_API_KEY non trovata.")
client = OpenAI(api_key=api_key)

# 🧠 Funzione per calcolare batch di embedding

def get_embeddings(texts):
    response = client.embeddings.create(input=texts, model=EMBEDDING_MODEL)
    return [item.embedding for item in response.data]

# 📦 Carica indice e metadati se esistono
index = None
metadata = []
existing_files = set()

if os.path.exists(INDEX_PATH) and os.path.exists(METADATA_PATH):
    try:
        index = faiss.read_index(INDEX_PATH)
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        existing_files = {m["filename"] for m in metadata}
        print(f"✅ Indice FAISS caricato con {index.ntotal} embedding già presenti.")
    except Exception as e:
        print(f"⚠️ Errore nel caricamento di FAISS/metadati: {e}. Verrà creato un nuovo indice.")

# 🔍 Scorri i file senza caricarli tutti
all_files = sorted([f for f in os.listdir(CHUNKS_FOLDER) if f.endswith(".txt") and f not in existing_files])
print(f"📄 Nuovi file da indicizzare: {len(all_files)}")

batch_texts = []
batch_meta = []
processed = 0

for file in tqdm(all_files, desc="Indicizzazione", unit="file"):
    path = os.path.join(CHUNKS_FOLDER, file)
    with open(path, "r", encoding="utf-8") as f:
        text = f.read().strip()
    if not text:
        continue

    batch_texts.append(text)
    batch_meta.append({
        "filename": file,
        "length": len(text),
        "path": os.path.relpath(path, ROOT_DIR),
    })

    if len(batch_texts) >= BATCH_SIZE:
        # 🚀 Elabora il batch
        embeddings_np = np.array(get_embeddings(batch_texts)).astype("float32")

        # Inizializza indice se necessario
        if index is None:
            dimension = embeddings_np.shape[1]
            index = faiss.IndexFlatL2(dimension)
            print(f"🧱 Creato nuovo indice FAISS con dimensione {dimension}")
        else:
            assert index.d == embeddings_np.shape[1], "❌ Dimensione embedding incoerente!"

        index.add(embeddings_np)
        metadata.extend(batch_meta)

        batch_texts.clear()
        batch_meta.clear()
        processed += embeddings_np.shape[0]

        # 💾 Salvataggio periodico
        if processed % SAVE_EVERY == 0:
            faiss.write_index(index, INDEX_PATH)
            with open(METADATA_PATH, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            print(f"💾 Salvataggio intermedio a {processed} chunk…")

# Elabora gli eventuali restanti
if batch_texts:
    embeddings_np = np.array(get_embeddings(batch_texts)).astype("float32")
    if index is None:
        dimension = embeddings_np.shape[1]
        index = faiss.IndexFlatL2(dimension)
    index.add(embeddings_np)
    metadata.extend(batch_meta)

# 🔚 Salva risultato finale
if index is not None:
    faiss.write_index(index, INDEX_PATH)
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    print(f"✅ Indicizzazione completata. Totale chunk indicizzati: {index.ntotal}")
else:
    print("ℹ️ Nessun nuovo chunk da indicizzare.")
