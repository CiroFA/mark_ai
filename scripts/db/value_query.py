import pymysql
import os
from dotenv import load_dotenv
from .table_metadata import TABLE_METADATA

load_dotenv()
MYSQL_USER     = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST     = os.getenv("MYSQL_HOST")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT"))
MYSQL_DB       = os.getenv("MYSQL_DB")

def answer_value_query(parsed: dict):
    """
    Esegue una query SQL a partire da:
    {
        "company": "AAPL",
        "table": "financials",
        "column": "Net_Income",
        "time_field": "fiscal_year",   # può essere None
        "time_type":  "year",
        "time_value": "2023"           # può essere 'latest' o None
    }
    """
    company    = parsed.get("company")
    table      = parsed.get("table")
    column     = parsed.get("column")
    time_field = parsed.get("time_field") or TABLE_METADATA.get(table, {}).get("time_field")
    time_value = parsed.get("time_value")

    if not (company and table and column):
        print("❌ Dati insufficienti per costruire la query.")
        return None

    try:
        connection = pymysql.connect(
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB
        )

        with connection.cursor() as cursor:
            if time_field is None:
                # Nessuna dimensione temporale (info, sustainability)
                query = f"""
                    SELECT {column}
                    FROM {table}
                    WHERE company_id = (
                        SELECT company_id FROM info WHERE symbol = %s
                    )
                    LIMIT 1
                """
                cursor.execute(query, (company.upper(),))

            else:
                # Tabella con dimensione temporale
                if time_value not in (None, "latest"):
                    tv = str(time_value).strip()          # normalizza a stringa senza spazi
                    if parsed.get("time_type") == "date" and len(tv) == 4 and tv.isdigit():
                        time_value = f"{tv}-12-31"

                if time_value in (None, "latest"):
                    query = f"""
                        SELECT {column}
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        ORDER BY {time_field} DESC
                        LIMIT 1
                    """
                    cursor.execute(query, (company.upper(),))
                else:
                    query = f"""
                        SELECT {column}
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        AND {time_field} = %s
                        LIMIT 1
                    """
                    cursor.execute(query, (company.upper(), time_value))

            result = cursor.fetchone()
            return result[0] if result else None

    except Exception as e:
        print("❌ Errore durante l'esecuzione della query:", e)
        return None

    finally:
        if "connection" in locals() and connection:
            connection.close()