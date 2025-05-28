import pymysql
import os
from dotenv import load_dotenv
from .metric_map import METRIC_MAP

# Carica le credenziali dal file .env
load_dotenv()
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB")

def answer_value_query(parsed: dict):
    """
    Esegue una query SQL per ottenere il valore richiesto a partire da un dizionario strutturato:
    {
        "company": "AAPL",
        "table": "financials",
        "column": "Net_Income",
        "period": "2022"
    }
    """
    company = parsed.get("company")
    table = parsed.get("table")
    column = parsed.get("column")
    period = parsed.get("period")

    if not (company and table and column and period):
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
            # Controlla se la colonna 'period' esiste nella tabella
            check_column_query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = %s AND table_name = %s AND column_name = 'period'
            """
            cursor.execute(check_column_query, (MYSQL_DB, table))
            period_exists = cursor.fetchone()[0] > 0

            if period == "latest":
                if period_exists:
                    query = f"""
                        SELECT {column}
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        ORDER BY period DESC
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
                    AND period LIKE %s
                    LIMIT 1
                """
                cursor.execute(query, (company.upper(), f"%{period}%"))

            result = cursor.fetchone()
            return result[0] if result else None

    except Exception as e:
        print("❌ Errore durante l'esecuzione della query:", e)
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()