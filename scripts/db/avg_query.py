import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB")

non_date_tables = {"info", "officers", "recommendations", "splits", "sustainability"}

def answer_avg_query(parsed: dict):
    """
    Calcola la media della colonna specificata per una determinata azienda e periodo.
    """
    company = parsed.get("company")
    table = parsed.get("table")
    column = parsed.get("column")
    period = parsed.get("period")

    if not (company and table and column and period):
        print("❌ Dati insufficienti per la media.")
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
            if table.strip().lower() in non_date_tables:
                print(f"❌ La tabella '{table}' non supporta query temporali.")
                return None

            date_column = "period" if table.strip().lower() == "cashflow" else "date"

            if period == "latest":
                query = f"""
                    SELECT AVG({column})
                    FROM {table}
                    WHERE company_id = (
                        SELECT company_id FROM info WHERE symbol = %s
                    )
                    AND {date_column} IS NOT NULL
                """
                cursor.execute(query, (company.upper(),))

            elif isinstance(period, list):
                if date_column == "date":
                    like_clauses = " OR ".join([f"{date_column} LIKE %s" for _ in period])
                    query = f"""
                        SELECT AVG({column})
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        AND ({like_clauses})
                    """
                    like_values = [f"%{p}%" for p in period]
                    cursor.execute(query, (company.upper(), *like_values))
                else:
                    like_clauses = " OR ".join([f"{date_column} LIKE %s" for _ in period])
                    query = f"""
                        SELECT AVG({column})
                        FROM {table}
                        WHERE company_id = (
                            SELECT company_id FROM info WHERE symbol = %s
                        )
                        AND ({like_clauses})
                    """
                    like_values = [f"%{p}%" for p in period]
                    cursor.execute(query, (company.upper(), *like_values))

            else:
                query = f"""
                    SELECT AVG({column})
                    FROM {table}
                    WHERE company_id = (
                        SELECT company_id FROM info WHERE symbol = %s
                    )
                    AND {date_column} LIKE %s
                """
                cursor.execute(query, (company.upper(), f"%{period}%"))

            result = cursor.fetchone()
            return result[0] if result and result[0] is not None else None

    except Exception as e:
        print("❌ Errore media query:", e)
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()