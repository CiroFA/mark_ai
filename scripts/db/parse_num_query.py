import os
import ast
from typing import Dict, Any
from openai import OpenAI
from dotenv import load_dotenv
from .metric_metadata import METRIC_METADATA

# Carica le variabili d'ambiente per l'API Key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def build_metric_options_string() -> str:
    """
    Costruisce una lista dettagliata delle metriche disponibili in formato leggibile per GPT,
    includendo descrizioni, filtrando solo le tabelle numeriche compatibili.
    """
    lines = []
    allowed_tables = {"balance_sheet", "cashflow", "financials", "history", "info"}
    for key, meta in METRIC_METADATA.items():
        if meta["table"] not in allowed_tables:
            continue
        table = meta["table"]
        column = meta["column"]
        description = meta.get("description", "")
        lines.append(f"- {key}")
        lines.append(f"  table: {table}")
        lines.append(f"  column: {column}")
        if description:
            lines.append(f"  description: {description}")
        lines.append("")  # aggiunge una riga vuota tra blocchi
    return "\n".join(lines)

def parse_numerical_question(question: str) -> Dict[str, Any]:
    """
    Usa ChatGPT per interpretare una domanda e restituire una struttura pronta
    per essere passata a value_query.py (company, table, column, period).
    """
    metric_options = build_metric_options_string()

    prompt = f"""
Sei un assistente che deve tradurre domande finanziarie in una struttura precisa per interrogare un database SQL.

Ecco l'elenco completo delle metriche disponibili, con la tabella e la colonna corrispondente:
{metric_options}

L'utente ha scritto la seguente domanda:
\"\"\"{question}\"\"\"

Il tuo compito è:
1. Capire a quale azienda si riferisce la domanda (simbolo ticker, ad esempio: AAPL, MSFT, TSLA, ecc.)
2. Capire a quale metrica si riferisce tra quelle fornite sopra
3. Restituire un dizionario Python nel formato seguente:
{{
  "company": "AAPL",
  "table": "financials",
  "column": "Net_Income",
  "period": "2022"
}}

Il campo "period" è sempre obbligatorio. Se l'utente non specifica un anno o un intervallo temporale chiaro, usa il valore "latest".
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": (
                 "Sei un assistente esperto di database finanziari. "
                 "Il tuo compito è selezionare con la massima accuratezza la colonna più rilevante tra quelle fornite, in base alla metrica richiesta. "
                 "Non devi scegliere una colonna che contiene informazioni correlate, ma una che corrisponde direttamente e semanticamente al termine richiesto. "
                 "Ad esempio, se la domanda riguarda il rapporto P/E, scegli solo colonne come 'trailingPE' o 'forwardPE', non 'Net_Income'. "
                 "Il tuo obiettivo è minimizzare ambiguità e restituire un dizionario Python esatto e coerente con il contenuto del database."
                )},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        content = response.choices[0].message.content
        print("🧾 Output grezzo GPT:", content)
        lines = content.strip().splitlines()

        # Estrai solo il blocco che inizia con "{" e termina con "}"
        start_index = next((i for i, line in enumerate(lines) if line.strip().startswith("{")), None)
        end_index = next((i for i, line in reversed(list(enumerate(lines))) if line.strip().endswith("}")), None)

        if start_index is not None and end_index is not None and start_index <= end_index:
            cleaned = "\n".join(lines[start_index:end_index+1])
            parsed = ast.literal_eval(cleaned)
            return parsed
        else:
            print("❌ Impossibile estrarre un dizionario valido dal testo GPT.")
            return {}
    except Exception as e:
        print("Errore nel parsing GPT:", e)
        return {}
