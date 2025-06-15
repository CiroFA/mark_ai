import os
import ast
import json
import re
from typing import Dict, Any
from openai import OpenAI
from dotenv import load_dotenv
from .metric_metadata import METRIC_METADATA
from .table_metadata import TABLE_METADATA

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

def build_table_time_mapping() -> str:
    """
    Restituisce una stringa con la mappatura tabella → campo temporale
    (o 'None' se la tabella non ha un campo data). Serve per il prompt LLM.
    """
    lines = []
    for table, meta in TABLE_METADATA.items():
        time_field = meta.get("time_field")
        lines.append(f"- {table}: {time_field or 'None'}")
    return "\n".join(lines)

def parse_numerical_question(question: str) -> Dict[str, Any]:
    """
    Usa ChatGPT per interpretare una domanda e restituire una struttura pronta
    per essere passata a value_query.py (company, table, column, period).
    """
    metric_options = build_metric_options_string()
    time_mapping = build_table_time_mapping()

    prompt = f"""
Sei un assistente che deve tradurre domande finanziarie in una struttura precisa
per interrogare un database SQL.

### Mappatura tabella → campo data
{time_mapping}

### Metriche disponibili
{metric_options}

L'utente ha scritto la seguente domanda:
\"\"\"{question}\"\"\"

Il tuo compito è:
1. Identificare il ticker (es. AAPL, MSFT…).
2. Scegliere la metrica più pertinente tra quelle sopra.
3. Estrarre eventuali riferimenti temporali:
   • anno intero  → "time_type":"year",  "time_value":"2023"
   • mese/anno    → "time_type":"month", "time_value":"2023-12"
   • data precisa → "time_type":"date",  "time_value":"2023-12-15"
   • intervallo   → "time_type":"range", "time_value":{{"from":"2020","to":"2024"}}
   • se assente   → "time_type":"latest", "time_value":null
4. Restituire **esattamente** il dizionario Python (niente testo attorno) con
   le 7 chiavi seguenti. Esempio formale (solo come guida):

   ```python
   {{
       "company":    "AAPL",
       "table":      "financials",
       "column":     "Net_Income",
       "time_field": "date",                 # oppure null
       "time_type":  "latest",               # oppure year / month / date / range
       "time_value": None                    # oppure "2023", "2023-12", ...
   }}
   ```

Nota: se la tabella indicata non ha campo data (`time_field = null`) allora il
periodo DEVE essere `"latest"`.
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

            # Rimuovi eventuali backtick / code‑fence residui
            cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)

            # Prova prima a caricarlo come JSON puro
            try:
                parsed = json.loads(cleaned)
            except json.JSONDecodeError:
                # Fallback: sostituisci JSON null/true/false con Python None/True/False
                cleaned_py = re.sub(r'\bnull\b', 'None', cleaned)
                cleaned_py = re.sub(r'\btrue\b', 'True', cleaned_py, flags=re.IGNORECASE)
                cleaned_py = re.sub(r'\bfalse\b', 'False', cleaned_py, flags=re.IGNORECASE)
                parsed = ast.literal_eval(cleaned_py)

            # parsed = ast.literal_eval(cleaned)

            # Post‑processing aggiuntivo
            table = parsed.get("table")
            parsed["time_field"] = TABLE_METADATA.get(table, {}).get("time_field")

            # Normalizza: se il parser non ha messo time_type/value li settiamo
            parsed.setdefault("time_type", "latest")
            if parsed["time_type"] == "latest":
                parsed["time_value"] = None

            # Coerenza tra table statica e periodo
            if parsed["time_field"] is None and parsed["time_type"] != "latest":
                raise ValueError(f"La tabella {table} non contiene dati storici: "
                                 "il periodo deve essere 'latest'.")
            return parsed
        else:
            print("❌ Impossibile estrarre un dizionario valido dal testo GPT.")
            return {}
    except Exception as e:
        print("Errore nel parsing GPT:", e)
        return {}
