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

# 🔐 Carica la chiave API da .env
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if api_key is None:
    raise ValueError("❌ OPENAI_API_KEY non trovata.")
client = OpenAI(api_key=api_key)

# 🧠 Funzione per calcolare l'embedding
def get_embedding(text):
    response = client.embeddings.create(
        input=[text],
        model=EMBEDDING_MODEL
    )
    return response.data[0].embedding

# 📦 Tenta di caricare indice e metadati esistenti
index = None
metadata = []
existing_files = set()

if os.path.exists(INDEX_PATH) and os.path.exists(METADATA_PATH):
    try:
        index = faiss.read_index(INDEX_PATH)
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        existing_files = {m["filename"] for m in metadata}
        print(f"✅ Indice FAISS caricato correttamente con {index.ntotal} embedding.")
    except Exception as e:
        print(f"⚠️ Errore nel caricamento di FAISS o metadati: {e}")
        print("➡️ Verrà creato un nuovo indice.")
else:
    print("📂 Nessun indice esistente trovato. Ne verrà creato uno nuovo.")

# 🔍 Trova nuovi chunk da indicizzare
new_chunks = []
new_metadata = []

for file in sorted(os.listdir(CHUNKS_FOLDER)):
    if not file.endswith(".txt") or file in existing_files:
        continue

    path = os.path.join(CHUNKS_FOLDER, file)
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    if text.strip():
        new_chunks.append(text)
        new_metadata.append({
            "filename": file,
            "length": len(text),
            "path": path
        })

print(f"📄 Chunk totali trovati: {len(os.listdir(CHUNKS_FOLDER))}")
print(f"🆕 Nuovi da indicizzare: {len(new_chunks)}")

# 🔁 Calcola e aggiungi nuovi embedding
if new_chunks:
    embeddings = [get_embedding(chunk) for chunk in tqdm(new_chunks)]
    embeddings_np = np.array(embeddings).astype("float32")

    if index is None:
        dimension = embeddings_np.shape[1]
        index = faiss.IndexFlatL2(dimension)
        print(f"🧱 Creato nuovo indice FAISS con dimensione {dimension}")
    else:
        assert index.d == embeddings_np.shape[1], "❌ Dimensione embedding non coerente!"

    index.add(embeddings_np)
    faiss.write_index(index, INDEX_PATH)

    metadata.extend(new_metadata)
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"✅ Aggiunti {len(new_chunks)} nuovi embedding. Totale ora: {index.ntotal}")
else:
    print("✅ Nessun nuovo chunk da indicizzare. Indice invariato.")
