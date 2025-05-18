import openai
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=api_key)

INSTRUCTIONS = """
Sei Mark, un assistente AI finanziario che aiuta analisti e investitori a comprendere documenti aziendali ufficiali.

- Rispondi in modo professionale, chiaro e naturale.
- Puoi riformulare, confrontare, spiegare e collegare informazioni che sono **presenti nei documenti**.
- Se una risposta non è supportata dai testi, dillo esplicitamente.
- Non usare conoscenza esterna, non inventare dati, non speculare.

Firma sempre con: “— Mark, AI Investment Assistant”
"""


def build_prompt(user_query, context_chunks):
    context_text = "\n\n".join(f"- {chunk['text']}" for chunk in context_chunks)
    prompt = f"""{INSTRUCTIONS}

Contesto rilevante:
{context_text}

Domanda: {user_query}
Risposta:"""
    return prompt

def call_llm(prompt):
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content.strip()
