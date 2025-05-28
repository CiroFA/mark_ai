import os
import ast
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from .metric_metadata import METRIC_METADATA

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def build_metric_prompt() -> str:
    lines = []
    for key, meta in METRIC_METADATA.items():
        if meta["table"] in {"balance_sheet", "cashflow", "financials", "history"}:
            lines.append(f"- {key}")
            lines.append(f"  table: {meta['table']}")
            lines.append(f"  column: {meta['column']}")
            if meta.get("description"):
                lines.append(f"  description: {meta['description']}")
            lines.append("")
    return "\n".join(lines)

def parse_avg_question(question: str) -> dict:
    CURRENT_YEAR = datetime.now().year
    prompt = f"""
L'utente ha fatto la seguente domanda in linguaggio naturale: "{question}"

Il suo intento è calcolare la **media** di una certa metrica finanziaria per una specifica azienda, in un certo periodo.

L’anno attuale è: {CURRENT_YEAR}.

Quando l’utente usa espressioni temporali, comportati così:
- “ultimi N anni” → restituisci N anni più recenti, prima dell’anno attuale (incluso l’anno precedente)
  Esempio: N=3, anno attuale={CURRENT_YEAR} → ["{CURRENT_YEAR - 3}", "{CURRENT_YEAR - 2}", "{CURRENT_YEAR - 1}"]
- “negli ultimi anni” o “recenti anni” → interpreta come ultimi 3 anni
- “quest’anno” o “anno corrente” → ["{CURRENT_YEAR - 1}"]
- “anno scorso” → ["{CURRENT_YEAR - 2}"]
- “dal 2020” → ["2020", ..., "{CURRENT_YEAR - 1}"]
- “tra 2019 e 2022” → ["2019", "2020", "2021", "2022"]
- Se non è presente alcun riferimento temporale, restituisci "latest"

Restituisci un dizionario Python con:
- "company": il ticker (es. "AAPL")
- "table": il nome della tabella SQL
- "column": il nome della colonna
- "period": una lista di anni stringa o "latest"

Queste sono le metriche disponibili:
{build_metric_prompt()}

Restituisci solo un dizionario Python valido. Nessuna spiegazione, nessun testo extra.
""".strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Sei un assistente esperto di finanza aziendale."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        content = response.choices[0].message.content
        print("🧾 Output grezzo GPT:", content)

        # Estrai il blocco di dizionario puro
        lines = content.strip().splitlines()
        start = next((i for i, line in enumerate(lines) if line.strip().startswith("{")), None)
        end = next((i for i, line in reversed(list(enumerate(lines))) if line.strip().endswith("}")), None)

        if start is not None and end is not None and start <= end:
            cleaned = "\n".join(lines[start:end+1])
            print("🔍 Dizionario estratto per parsing:", cleaned)
            parsed_dict = ast.literal_eval(cleaned)
            print("🔎 Dizionario dopo parsing:", parsed_dict)
            if isinstance(parsed_dict.get("period"), list):
                print("🕵️ Period originale:", parsed_dict["period"])
                parsed_dict["period"] = [
                    str(year).strip() for year in parsed_dict["period"]
                    if str(year).strip().isdigit()
                ]
                print("✅ Period trasformato:", parsed_dict["period"])
            return parsed_dict
        else:
            print("❌ Nessun dizionario riconosciuto.")
            return {}
    except Exception as e:
        print("❌ Errore nel parsing GPT:", e)
        return {}