import pymysql
import os
from dotenv import load_dotenv
from .table_metadata import TABLE_METADATA
from .date_utils import bounds
import datetime

load_dotenv()
MYSQL_USER     = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST     = os.getenv("MYSQL_HOST")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT"))
MYSQL_DB       = os.getenv("MYSQL_DB")

class ValueNotInDatabase(Exception):
    pass

def answer_value_query(parsed: dict):
    """
    Esegue una query SQL a partire da:
    {
        "company": "AAPL",
        "table": "financials",
        "column": "Net_Income",
        "time_field": "date",       # può essere None
        "time_type":  "year",
        "time_value": "2023"        # può essere None se latest
    }
    """
    company    = parsed.get("company")
    table      = parsed.get("table")
    column     = parsed.get("column")
    time_field = parsed.get("time_field") or TABLE_METADATA.get(table, {}).get("time_field")
    time_type  = parsed.get("time_type", "latest")
    time_value = parsed.get("time_value")

    if not (company and table and column):
        print("❌ Dati insufficienti per costruire la query.")
        return None

    # Se la tabella non contiene un campo temporale, accettiamo solo 'latest'
    if time_field is None and time_type != "latest":
        raise ValueError(f"La tabella {table} non contiene dati storici: "
                         "il periodo deve essere 'latest'.")

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
                # Tabella statica (info, recommendations, splits, sustainability)
                sql = f"""
                    SELECT {column}
                    FROM {table}
                    WHERE company_id = (
                        SELECT company_id FROM info WHERE symbol = %s
                    )
                """
                cursor.execute(sql, (company.upper(),))

            else:
                start, end = bounds(time_type, time_value)
                if start is None:
                    # Caso 'latest'
                    sql = f"""
                        SELECT {column}
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        ORDER BY {time_field} DESC
                        LIMIT 1
                    """
                    cursor.execute(sql, (company.upper(),))
                else:
                    sql = f"""
                        SELECT {column}
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                          AND {time_field} BETWEEN %s AND %s
                        ORDER BY {time_field} DESC
                        LIMIT 1
                    """
                    # PyMySQL converte automaticamente datetime.date in stringa
                    cursor.execute(sql, (company.upper(), start, end))

            result = cursor.fetchone()
            if result is None:
                raise ValueNotInDatabase("No data found in the database.")
            return result[0]

    except Exception as e:
        if isinstance(e, ValueNotInDatabase):
            raise
        print("❌ Errore durante l'esecuzione della query:", e)
        return None

    finally:
        if "connection" in locals() and connection:
            connection.close()