import os
from retriever import retrieve_chunks_by_company
from llm_wrapper import build_prompt, call_llm

try:
    import tiktoken
    enc = tiktoken.encoding_for_model("gpt-4")

    def count_tokens(text):
        return len(enc.encode(text))
except ImportError:
    def count_tokens(text):
        return len(text.split())

def ask_mark(query):
    # 🔍 Recupero chunk senza filtrare su ticker (solo semantica)
    chunks = retrieve_chunks_by_company(query, tickers=None, total_k=6)

    # 📥 Carica il testo associato a ciascun chunk
    for chunk in chunks:
        chunk_path = os.path.join(os.path.dirname(__file__), '../data/chunks', chunk["filename"])
        if os.path.exists(chunk_path):
            with open(chunk_path, "r", encoding="utf-8") as f:
                chunk["text"] = f.read()
        else:
            chunk["text"] = ""

    if not chunks:
        return "Non ho trovato informazioni rilevanti nei documenti disponibili."

    prompt = build_prompt(query, chunks)
    return call_llm(prompt)

# 🧪 Test manuale
if __name__ == "__main__":
    question = input("Scrivi la tua domanda: ")
    print(ask_mark(question))
