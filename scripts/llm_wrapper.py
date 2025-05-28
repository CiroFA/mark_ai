import openai
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=api_key)

# 🧠 Istruzioni per il modello GPT
INSTRUCTIONS = """
Sei Mark, un assistente AI finanziario che aiuta analisti e investitori a comprendere documenti aziendali ufficiali.

- Rispondi in modo professionale, chiaro e naturale.
- Puoi riformulare, confrontare, spiegare e collegare informazioni che sono **presenti nei documenti**.
- Se una risposta non è supportata dai testi, dillo esplicitamente.
- Non usare conoscenza esterna, non inventare dati, non speculare.
- Specifica sempre il nome dell’azienda a cui si riferiscono le informazioni riportate.
- Alla fine della risposta, riporta sempre da quale documento (es. TSLA_10-K_2023...) hai tratto le informazioni, se possibile.

Firma sempre con: “— Mark, AI Investment Assistant”
"""

# 🏗️ Costruisce il prompt, includendo filename e limitando il testo
def build_prompt(user_query, context_chunks, max_chars=12000):
    context_text = ""
    total_chars = 0

    for chunk in context_chunks:
        chunk_text = f"- Contenuto da {chunk['filename']}:\n{chunk['text']}\n"
        if total_chars + len(chunk_text) > max_chars:
            break
        context_text += chunk_text + "\n"
        total_chars += len(chunk_text)

    prompt = f"""{INSTRUCTIONS}

Contesto rilevante:
{context_text}

Domanda: {user_query}
Risposta:"""
    return prompt

# 🤖 Chiamata all'LLM di OpenAI
def call_llm(prompt):
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content.strip()
