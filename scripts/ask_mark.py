from retriever import retrieve_chunks_by_company
from llm_wrapper import build_prompt, call_llm

def ask_mark(query):
    # 🧠 Analisi base per Apple + Microsoft
    tickers = []
    q = query.lower()
    if "apple" in q:
        tickers.append("AAPL")
    if "microsoft" in q:
        tickers.append("MSFT")
    if "google" in q or "alphabet" in q:
        tickers.append("GOOGL")
    if "amazon" in q:
        tickers.append("AMZN")

    chunks = retrieve_chunks_by_company(query, tickers=tickers if tickers else None, total_k=8, per_company_k=2)

    if not chunks:
        return "⚠️ Nessun documento rilevante trovato. Prova a riformulare la domanda."

    min_distance = min(c["distance"] for c in chunks)
    print(f"📊 Distanza minima: {min_distance}")

    if min_distance > 1.6:
        return "❌ I documenti disponibili non sono sufficientemente rilevanti per fornire una risposta affidabile. — Mark"
    if 1.0 < min_distance <= 1.6:
        disclaimer = "⚠️ I documenti trovati sono moderatamente rilevanti. La risposta potrebbe non essere completamente accurata.\n\n"
    else:
        disclaimer = ""

    prompt = build_prompt(query, chunks)
    response = call_llm(prompt)
    return disclaimer + response

# ▶️ Test manuale
if __name__ == "__main__":
    query = input("Scrivi una domanda per Mark: ")
    answer = ask_mark(query)
    print("\n📢 Risposta:\n")
    print(answer)
