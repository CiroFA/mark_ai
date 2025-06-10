# scripts/db/generate_table_metadata.py
import os
import pymysql
import time
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MYSQL_USER     = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST     = os.getenv("MYSQL_HOST")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT"))
MYSQL_DB       = os.getenv("MYSQL_DB")

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "table_metadata.py")

# ─────────────────────────────────────────────────────────────────────────────
def get_all_tables():
    conn = pymysql.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB
    )
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        return [row[0] for row in cur.fetchall()]

# ─────────────────────────────────────────────────────────────────────────────
def ask_gpt_table_meta(table: str) -> dict | None:
    """
    Chiede a GPT di identificare la colonna temporale principale per la tabella.
    """
    prompt = f"""
You are a senior data architect.  
In a financial MySQL database, we have a table named `{table}`.

**Task**: Identify the column that uniquely represents the time dimension
for this table and classify its type.  
Allowed `time_type` values:

- "date"           → YYYY-MM-DD
- "year"           → 2023
- "period_string"  → "FY2023", "FY2022", "TTM"
- "offset_string"  → "0", "-1m", "-2m"
- "none"           → no time dimension

Return **only** a valid Python dictionary like:
{{
  "time_field": <str|None>,
  "time_type": <str>
}}
    """.strip()

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}]
        )
        txt = resp.choices[0].message.content.strip()
        # estrai il blocco { ... }
        start = txt.find("{")
        end   = txt.rfind("}")
        if start != -1 and end != -1:
            return eval(txt[start:end+1])
    except Exception as e:
        print(f"GPT error for table {table}: {e}")
    return None

# ─────────────────────────────────────────────────────────────────────────────
def generate_table_metadata():
    print("🔍 Scanning tables...")
    tables = get_all_tables()
    meta   = {}

    for tbl in tables:
        print(f"🧠 GPT-4o → {tbl}")
        entry = ask_gpt_table_meta(tbl)
        if entry:
            meta[tbl] = {
                "time_field": entry["time_field"],
                "time_type":  entry["time_type"]
            }
        time.sleep(0.8)   # antirate-limit

    print("✏️  Writing TABLE_METADATA…")
    with open(OUTPUT_FILE, "w") as f:
        f.write("TABLE_METADATA = {\n")
        for tbl, cfg in meta.items():
            f.write(f'    "{tbl}": {{\n')
            f.write(f'        "time_field": {repr(cfg["time_field"])},\n')
            f.write(f'        "time_type": {repr(cfg["time_type"])},\n')
            f.write("    },\n")
        f.write("}\n")
    print(f"✅ Done → {OUTPUT_FILE}")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    generate_table_metadata()