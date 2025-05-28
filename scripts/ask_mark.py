# ✅ Versione standard di ask_mark.py con supporto a company_hint, fallback semantico e confronto tra aziende

import csv
import os
from retriever import retrieve_chunks_by_company, find_closest_general_topic
from llm_wrapper import build_prompt, call_llm

try:
    import tiktoken
    enc = tiktoken.encoding_for_model("gpt-4")
    def count_tokens(text):
        return len(enc.encode(text))
except ImportError:
    def count_tokens(text):
        return len(text.split())

def load_company_tickers(csv_path):
    company_tickers = {}
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ticker = row['ticker']
            company_tickers[ticker.lower()] = ticker
    return company_tickers

COMPANY_TICKERS = load_company_tickers(
    os.path.join(os.path.dirname(__file__), '../data/documents_raw/company_list.csv')
)

def ask_mark(query):
    tickers = []
    q = query.lower()
    for name in COMPANY_TICKERS:
        if name in q and COMPANY_TICKERS[name] not in tickers:
            tickers.append(COMPANY_TICKERS[name])

    # 🔁 Gestione multi-azienda per confronto
    if len(tickers) > 1:
        context_chunks = []
        all_files = []
        for t in tickers:
            company_chunks = retrieve_chunks_by_company(query, tickers=[t], total_k=2, per_company_k=2)
            if company_chunks:
                max_chars_per_company = 1000
                texts = []
                char_count = 0
                for c in company_chunks:
                    chunk_text = f"- Contenuto da {c['filename']}\n{c['text']}\n"
                    all_files.append(c['filename'])
                    if char_count + len(chunk_text) > max_chars_per_company:
                        break
                    texts.append(chunk_text)
                    char_count += len(chunk_text)
                formatted = f"\nInformazioni su {t}:\n" + "\n".join(texts)
                context_chunks.append(formatted)
        if not context_chunks:
            return "❌ Nessun contenuto utile trovato per nessuna delle aziende menzionate. — Mark"

        context_text = "\n\n".join(context_chunks)
        prompt = (
            "Rispondi alla seguente domanda confrontando le aziende indicate. "
            "Specifica sempre il nome dell’azienda a cui si riferiscono le informazioni. "
            "Alla fine della risposta, riporta sempre da quale documento (es. TSLA_10-K_2023...) "
            "hai tratto le informazioni, se possibile.\n\n"
            f"Contesto rilevante:\n{context_text}\n\nDomanda: {query}\nRisposta:"
        )

        print(f"🔢 Token stimati: {count_tokens(prompt)}")
        response = call_llm(prompt)
        return response + "\n\nIndice di affidabilità: Moderata\n\n— Mark, AI Investment Assistant"

    company_hint = tickers[0] if tickers else None
    chunks = retrieve_chunks_by_company(query, tickers=tickers if tickers else None, total_k=8, per_company_k=2, company_hint=company_hint)
    min_distance = min([c["distance"] for c in chunks], default=99)

    if min_distance < 0.9:
        quality = "Alta"
    elif min_distance < 1.2:
        quality = "Moderata"
    else:
        quality = "Bassa"

    fallback_used = False

    if not chunks or min_distance > 1.2:
        topic, similarity = find_closest_general_topic(query)
        if similarity > 0.5:
            fallback_used = True
            print(f"[INFO] Fallback attivato: tema '{topic}' con similarità {similarity:.2f}")
            alt_chunks = retrieve_chunks_by_company(topic, tickers=None, total_k=8, per_company_k=2, company_hint=company_hint)
            if alt_chunks:
                alt_prompt = build_prompt(
                    f"La domanda originale era: '{query}'. Offri una risposta approssimativa basata su questo tema correlato: '{topic}'. "
                    "Specifica sempre il nome dell’azienda a cui si riferiscono le informazioni. "
                    "Alla fine della risposta, riporta sempre da quale documento (es. TSLA_10-K_2023...) hai tratto le informazioni, se possibile.",
                    alt_chunks
                )
                print(f"🔢 Token stimati (fallback): {count_tokens(alt_prompt)}")
                alt_response = call_llm(alt_prompt)
                return (
                    f"\n⚠️ Nessun documento strettamente rilevante trovato. Risposta basata su tema correlato ('{topic}').\n\n"
                    + alt_response
                    + f"\n\nIndice di affidabilità: {quality}\n\n— Mark, AI Investment Assistant"
                )
        return "❌ Nessun contenuto utile trovato, e non ci sono argomenti correlati abbastanza simili. — Mark"

    print(f"📊 Distanza minima: {min_distance:.4f}")
    disclaimer = ""
    if min_distance > 1.6:
        return "❌ I documenti disponibili non sono sufficientemente rilevanti per fornire una risposta affidabile. — Mark"
    if 1.0 < min_distance <= 1.6:
        disclaimer = "⚠️ I documenti trovati sono moderatamente rilevanti. La risposta potrebbe non essere completamente accurata.\n\n"

    prompt = build_prompt(query, chunks)
    print(f"🔢 Token stimati: {count_tokens(prompt)}")
    response = call_llm(prompt)

    if fallback_used:
        disclaimer += "\n⚠️ Questa risposta è stata generata tramite fallback semantico.\n"

    return disclaimer + response + f"\n\nIndice di affidabilità: {quality}\n\n— Mark, AI Investment Assistant"

if __name__ == "__main__":
    query = input("Scrivi una domanda per Mark: ")
    answer = ask_mark(query)
    print("\n📢 Risposta:\n")
    print(answer)
