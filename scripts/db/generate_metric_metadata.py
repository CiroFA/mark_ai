import os
import pymysql
import time
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB")

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "metric_metadata.py")

def get_all_columns():
    connection = pymysql.connect(
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB
    )
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = %s
        """, (MYSQL_DB,))
        return cursor.fetchall()

def ask_gpt_metadata(table: str, column: str):
    prompt = f"""
You are an expert in financial data from Yahoo Finance. 
The column is named `{column}` and belongs to the `{table}` table.
Please provide a short and precise English description of what this metric likely represents, 
using a maximum of 120 characters. No aliases.

Return only a Python dictionary in this format:
{{
  "description": "..."
}}
    """.strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        content = response.choices[0].message.content
        lines = content.strip().splitlines()
        start = next((i for i, line in enumerate(lines) if line.strip().startswith("{")), None)
        end = next((i for i, line in reversed(list(enumerate(lines))) if line.strip().endswith("}")), None)
        if start is not None and end is not None:
            block = "\n".join(lines[start:end+1])
            return eval(block)
        else:
            return None
    except Exception as e:
        print(f"❌ GPT error for {table}.{column}: {e}")
        return None

def to_snake_case(s):
    return s.lower().replace(" ", "_").replace("-", "_")

def generate_metadata():
    print("📊 Scanning database columns...")
    columns = get_all_columns()
    metadata = {}

    EXCLUDED_COLUMNS = {
        "company_id", "balance_sheet_id", "cashflow_id", "dividends_id",
        "financials_id", "history_id", "info_id", "recommendations_id",
        "splits_id", "sustainability_id"
    }

    for table, column in columns:
        if table.lower() == "officers" or column.lower() in EXCLUDED_COLUMNS:
            continue

        key = to_snake_case(column)
        print(f"🧠 Generating for {table}.{column}...")
        result = ask_gpt_metadata(table, column)
        if result:
            metadata[key] = {
                "table": table,
                "column": column,
                "description": result["description"]
            }
        time.sleep(0.8)  # evita rate limit

    with open(OUTPUT_FILE, "w") as f:
        f.write("METRIC_METADATA = {\n")
        for key, entry in metadata.items():
            f.write(f'    "{key}": {{\n')
            f.write(f'        "table": "{entry["table"]}",\n')
            f.write(f'        "column": "{entry["column"]}",\n')
            f.write(f'        "description": "{entry["description"]}"\n')
            f.write("    },\n")
        f.write("}\n")
    print(f"✅ Metadata written to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_metadata()
