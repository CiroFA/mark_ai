"""
update_mark_db.py

End‑to‑end ETL script that downloads data from Yahoo Finance via *yfinance*
and upserts it into a MySQL database.  For every ticker in `tickers` the
script populates:

    • info + officers
    • price history & corporate actions
    • annual balance‑sheet, cash‑flow, financial statements
    • recommendations summary
    • stock‑splits
    • sustainability / ESG snapshot

Each loader follows a uniform pattern:

    1.  Download raw data with yfinance
    2.  Normalise / pivot as needed
    3.  Resolve `company_id`
    4.  (optional) Add missing columns with ALTER TABLE
    5.  Upsert inside a transaction
    6.  Structured logging (inserted / updated / skipped)

The helper `normalize_name()` converts any label to `snake_case` so that
schema additions stay consistent across tables.
"""
import os
import logging
from dotenv import load_dotenv
import yfinance as yf
from sqlalchemy import create_engine, text, inspect, MetaData
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.types import Text, Boolean, Integer, Float, String
import pandas as pd
import re

# Helper: normalize raw field names to snake_case (handles CamelCase and symbols)
def normalize_name(raw: str) -> str:
    # Insert underscores before capitals (CamelCase → snake_case)
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', raw)
    # Replace non‑alphanumeric chars with underscores, lowercase, and trim
    s = re.sub(r"[^0-9A-Za-z]+", "_", s).lower().strip("_")
    return s

# Load environment variables from .env file
load_dotenv()

# Logging configuration: create a directory for logs if it doesn't exist, and set up the log file with timestamp
log_dir = os.path.join(os.path.dirname(__file__), "logs", "mark_db_logs")
os.makedirs(log_dir, exist_ok=True)
log_filename = f"update_mark_db_{datetime.now().strftime('%Y-%m-%d')}.log"
log_path = os.path.join(log_dir, log_filename)

logging.basicConfig(
    filename=log_path,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

# Database connection parameters loaded from environment variables
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DB")

logging.info(f"✅ Connected to database: {MYSQL_DB}")

# Create SQLAlchemy engine and inspector for database interaction
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)
inspector = inspect(engine)

# Load tickers from external CSV file
import pandas as pd

csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'company_list.csv'))
df_companies = pd.read_csv(csv_path)
tickers = df_companies["ticker"].dropna().unique().tolist()

# Function to insert or update data in the 'info' table
# This function retrieves stock information from yfinance, dynamically updates the database schema if new fields are found,
# and performs an upsert operation (insert or update) for the given ticker.
# 
# Note: This function currently handles only the 'info' table, but the structure and approach can be replicated for other tables
# such as 'history', 'financials', etc., by creating similar functions following this pattern.
def insert_info_data(ticker):
    # Retrieve stock data from yfinance
    stock = yf.Ticker(ticker)
    info = stock.info

    # Load current table metadata from the database
    metadata = MetaData()
    metadata.reflect(bind=engine)
    info_table = metadata.tables["info"]

    # Get existing columns in the 'info' table
    columns = info_table.columns.keys()

    # Identify any new fields in the retrieved info that are not yet in the table schema
    extra_fields = [key for key in info.keys() if key not in columns]

    # Prepare to add new columns for compatible extra fields
    added_columns = []
    skipped_columns = {}
    for field in extra_fields:
        value = info.get(field)
        col_name = field
        # Skip fields with complex data types that are not supported (list, dict)
        if isinstance(value, (list, dict)):
            skipped_columns[col_name] = f"type {type(value).__name__}"
            continue
        # Skip string fields that are excessively long (over 10,000 characters)
        if isinstance(value, str) and len(value) > 10000:
            skipped_columns[col_name] = f"text too long ({len(value)} characters)"
            continue
        # Determine SQLAlchemy column type based on Python data type
        if isinstance(value, str):
            col_type = Text()
        elif isinstance(value, bool):
            col_type = Boolean()
        elif isinstance(value, int):
            col_type = Integer()
        elif isinstance(value, float):
            col_type = Float()
        elif value is None:
            col_type = String(500)
        else:
            skipped_columns[col_name] = f"unsupported type: {type(value)}"
            continue

        # Attempt to alter the table by adding the new column
        try:
            with engine.begin() as conn:
                alter_stmt = text(f"ALTER TABLE info ADD COLUMN `{col_name}` {col_type.compile(dialect=engine.dialect)}")
                conn.execute(alter_stmt)
                added_columns.append((col_name, col_type.__class__.__name__))
        except SQLAlchemyError as e:
            skipped_columns[col_name] = f"SQL error: {e}"

    # Log information about added columns and skipped fields
    if added_columns:
        logging.info(f"➕ {len(added_columns)} new columns added to 'info' for {ticker}")
    if skipped_columns:
        logging.warning(
            f"⚠️ {len(skipped_columns)} fields ignored for {ticker} in 'info' due to unsupported types or length"
        )

    # Force reload of metadata to reflect the newly added columns
    metadata.clear()  # Clear cached metadata to avoid stale schema info
    metadata.reflect(bind=engine)  # Reload updated table schema
    updated_table = metadata.tables["info"]

    # Verify that the new columns are present in the refreshed metadata
    updated_columns = updated_table.columns.keys()
    for col_name, _ in added_columns:
        if col_name not in updated_columns:
            logging.error(f"❌ Column '{col_name}' was not detected in metadata after addition.")
            # Force another refresh and re-check
            metadata.reflect(bind=engine)
            updated_table = metadata.tables["info"]
            updated_columns = updated_table.columns.keys()
            if col_name not in updated_columns:
                logging.error(f"❌ Column '{col_name}' still not present. Aborting operation.")
                return

    # Prepare data dictionary for insert or update, including all known columns except the primary key 'company_id'
    insert_data = {col: info.get(col, None) for col in updated_columns if col != "company_id"}

    # Perform upsert operation: update if symbol exists, otherwise insert new row
    with engine.begin() as conn:
        try:
            existing_symbol = conn.execute(
                text(f"SELECT company_id FROM info WHERE symbol = :symbol"), {"symbol": insert_data["symbol"]}
            ).fetchone()

            if existing_symbol:
                update_stmt = (
                    updated_table.update()
                    .where(updated_table.c.symbol == insert_data["symbol"])
                    .values(**insert_data)
                )
                conn.execute(update_stmt)
                logging.info(f"🔁 1 row updated in 'info' for {ticker}")
            else:
                stmt = mysql_insert(updated_table).values(**insert_data)
                conn.execute(stmt)
                logging.info(f"➕ 1 new row inserted into 'info' for {ticker}")
            
            # Recupera il company_id aggiornato
            company_id_row = conn.execute(
                text("SELECT company_id FROM info WHERE symbol = :symbol"),
                {"symbol": ticker}
            ).fetchone()

            if company_id_row:
                insert_officers_data(ticker, company_id_row[0])
            else:
                logging.warning(f"⚠️ Could not find company_id for {ticker} to insert officers")
        except SQLAlchemyError as e:
            logging.error(f"❌ Error during upsert for {ticker}: {e}")


# Function to insert or update officers data in the 'officers' table
# This function retrieves officers data from yfinance and performs an upsert operation for each officer.
# It checks if the officer already exists in the database based on company_id, name, and fiscal_year.
# If the officer exists, it updates the record; otherwise, it inserts a new record.
def insert_officers_data(ticker, company_id):
    stock = yf.Ticker(ticker)
    officers = stock.info.get("companyOfficers", [])

    if not officers:
        logging.warning(f"⚠️ No officers data found for {ticker}; skipping 'officers' insert")
        return

    metadata = MetaData()
    metadata.reflect(bind=engine)
    officers_table = metadata.tables["officers"]

    inserted_count = 0
    updated_count = 0

    with engine.begin() as conn:
        for officer in officers:
            name = officer.get("name")
            title = officer.get("title")
            age = officer.get("age")
            year_born = officer.get("yearBorn")
            fiscal_year = officer.get("fiscalYear")
            total_pay = officer.get("totalPay")
            exercised_value = officer.get("exercisedValue")
            unexercised_value = officer.get("unexercisedValue")

            # Controlla se l'ufficiale esiste già
            result = conn.execute(
                text("""
                    SELECT officer_id FROM officers
                    WHERE company_id = :company_id AND name = :name AND fiscal_year = :fiscal_year
                """),
                {
                    "company_id": company_id,
                    "name": name,
                    "fiscal_year": fiscal_year
                }
            ).fetchone()

            if result:
                update_stmt = officers_table.update().where(
                    officers_table.c.officer_id == result[0]
                ).values(
                    title=title,
                    age=age,
                    year_born=year_born,
                    total_pay=total_pay,
                    exercised_value=exercised_value,
                    unexercised_value=unexercised_value
                )
                conn.execute(update_stmt)
                updated_count += 1
            else:
                insert_stmt = officers_table.insert().values(
                    company_id=company_id,
                    name=name,
                    title=title,
                    age=age,
                    year_born=year_born,
                    fiscal_year=fiscal_year,
                    total_pay=total_pay,
                    exercised_value=exercised_value,
                    unexercised_value=unexercised_value
                )
                conn.execute(insert_stmt)
                inserted_count += 1

    if inserted_count:
        logging.info(f"➕ {inserted_count} new rows inserted into 'officers' for {ticker}")
    if updated_count:
        logging.info(f"🔁 {updated_count} rows updated in 'officers' for {ticker}")

# Function to insert or update daily price and corporate‑action data in the 'history' table
# It downloads the full history from yfinance and performs an upsert per (company_id, date).
def insert_history_data(ticker):
    # ── 1. Download all historical rows ───────────────────────────────────────
    try:
        df = yf.Ticker(ticker).history(period="max", actions=True)
    except Exception as e:
        logging.error(f"❌ Failed to download history for {ticker}: {e}")
        return

    if df.empty:
        logging.warning(f"⚠️ No history data found for {ticker}")
        return

    # ── 2. Normalise dataframe ───────────────────────────────────────────────
    df.index = df.index.tz_localize(None)          # remove timezone info
    df.reset_index(inplace=True)                   # move index to column
    df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
        },
        inplace=True,
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Retain only the columns actually present in the MySQL table
    expected_cols = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dividends",
        "stock_splits",
    ]
    df = df[expected_cols]

    # ── 3. Resolve company_id ────────────────────────────────────────────────
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping history insert")
        return
    company_id = row[0]

    # ── 4. Upsert each row inside a single transaction ───────────────────────
    inserted = 0
    updated = 0
    with engine.begin() as conn:
        for _, rec in df.iterrows():
            # Convert NaN to None for SQL
            clean = {
                k: (None if pd.isna(v) else v)
                for k, v in rec.to_dict().items()
            }
            clean["company_id"] = company_id

            exists = conn.execute(
                text(
                    """
                    SELECT history_id
                    FROM history
                    WHERE company_id = :cid AND date = :d
                    """
                ),
                {"cid": company_id, "d": clean["date"]},
            ).fetchone()

            if exists:
                conn.execute(
                    text(
                        """
                        UPDATE history
                        SET open=:open,
                            high=:high,
                            low=:low,
                            close=:close,
                            volume=:volume,
                            dividends=:dividends,
                            stock_splits=:stock_splits
                        WHERE company_id=:cid AND date=:d
                        """
                    ),
                    {
                        **clean,
                        "cid": company_id,
                        "d": clean["date"],
                    },
                )
                updated += 1
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO history
                        (company_id, date, open, high, low, close, volume, dividends, stock_splits)
                        VALUES (:company_id, :date, :open, :high, :low, :close,
                                :volume, :dividends, :stock_splits)
                        """
                    ),
                    clean,
                )
                inserted += 1

    # ── 5. Logging summary ──────────────────────────────────────────────────
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'history' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'history' for {ticker}")


# Function to insert or update annual balance‑sheet data in the 'balance_sheet' table
# It downloads the full balance sheet from yfinance, pivots it per‑date, and performs an
# upsert per (company_id, date).  Columns not present in the table are counted and reported.
def insert_balance_sheet_data(ticker):
    # ── 1. Download balance‑sheet dataframe (yfinance returns accounts × dates) ─────
    try:
        raw_df = yf.Ticker(ticker).balance_sheet
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve balance_sheet data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if raw_df.empty:
        logging.warning(f"⚠️ No balance‑sheet data found for {ticker}")
        return

    # ── 2. Pivot so that each row is a single date ───────────────────────────
    df = raw_df.T  # rows = dates, cols = account names
    df.index = df.index.tz_localize(None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # ── 3. Fetch company_id ──────────────────────────────────────────────────
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping balance‑sheet insert")
        return
    company_id = row[0]

    balance_table = metadata.tables["balance_sheet"]
    existing_cols = balance_table.columns.keys()
    # Build a normalization map: normalized name → actual column name
    existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 3a. Dynamically add missing columns (similar to insert_info_data) ─────
    # Identify extra fields in the dataframe (normalized names) that are not in the table
    extra_fields = []
    sample_values = {}
    for col in df.columns:
        if col == "date":
            continue
        norm_col = normalize_name(col)
        if norm_col not in existing_ci:
            extra_fields.append(norm_col)
            # grab first non‑NaN sample to infer type
            sample_series = df[col].dropna()
            sample_values[norm_col] = sample_series.iloc[0] if not sample_series.empty else None

    added_columns = []
    skipped_columns = {}

    for norm_col in extra_fields:
        value = sample_values.get(norm_col)
        # skip lists/dicts
        if isinstance(value, (list, dict)):
            skipped_columns[norm_col] = f"type {type(value).__name__}"
            continue
        if isinstance(value, str) and len(value) > 10000:
            skipped_columns[norm_col] = f"text too long ({len(value)} characters)"
            continue

        # map python type → SQLAlchemy column type
        if isinstance(value, str):
            col_type = Text()
        elif isinstance(value, bool):
            col_type = Boolean()
        elif isinstance(value, int):
            col_type = Integer()
        elif isinstance(value, float):
            col_type = Float()
        elif value is None:
            col_type = String(500)
        else:
            skipped_columns[norm_col] = f"unsupported type: {type(value)}"
            continue

        try:
            with engine.begin() as conn:
                alter_stmt = text(
                    f"ALTER TABLE balance_sheet ADD COLUMN `{norm_col}` {col_type.compile(dialect=engine.dialect)}"
                )
                conn.execute(alter_stmt)
                added_columns.append((norm_col, col_type.__class__.__name__))
        except SQLAlchemyError as e:
            skipped_columns[norm_col] = f"SQL error: {e}"

    if added_columns:
        logging.info(f"➕ {len(added_columns)} new columns added to 'balance_sheet' for {ticker}")
    if skipped_columns:
        logging.warning(f"⚠️ {len(skipped_columns)} fields ignored for {ticker} in 'balance_sheet' due to unsupported types or length")

    # Reload metadata to include newly added columns
    if added_columns:
        metadata.clear()
        metadata.reflect(bind=engine)
        balance_table = metadata.tables["balance_sheet"]
        existing_cols = balance_table.columns.keys()
        existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 4. Iterate rows and upsert ───────────────────────────────────────────
    inserted = 0
    updated = 0

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            date_val = rec["date"]
            row_data = {
                "company_id": company_id,
                "date": date_val,
            }

            # Map each account name → table column (normalize to snake_case)
            for field, value in rec.items():
                if field == "date":
                    continue
                col_name = normalize_name(field)
                target_col = existing_ci.get(col_name)
                if target_col:
                    row_data[target_col] = None if pd.isna(value) else value

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT balance_sheet_id
                    FROM balance_sheet
                    WHERE company_id=:cid AND date=:d
                    """
                ),
                {"cid": company_id, "d": date_val},
            ).fetchone()

            if exists:
                upd_vals = {k: v for k, v in row_data.items() if k not in ("company_id", "date")}
                if upd_vals:  # avoid empty update
                    conn.execute(
                        balance_table.update()
                        .where(balance_table.c.company_id == company_id)
                        .where(balance_table.c.date == date_val)
                        .values(**upd_vals)
                    )
                updated += 1
            else:
                conn.execute(balance_table.insert().values(**row_data))
                inserted += 1

    # ── 5. Logging ───────────────────────────────────────────────────────────
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'balance_sheet' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'balance_sheet' for {ticker}")


# Function to insert or update annual cash‑flow data in the 'cashflow' table
# It downloads the full cash‑flow statement from yfinance, pivots it per‑date,
# and performs an upsert per (company_id, period).  Fields lacking a matching
# column are counted and reported.
def insert_cashflow_data(ticker):
    # ── 1. Download raw cash‑flow dataframe (yfinance: accounts × dates) ─────
    try:
        raw_df = yf.Ticker(ticker).cashflow
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve cashflow data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if raw_df.empty:
        logging.warning(f"⚠️ No cash‑flow data found for {ticker}")
        return

    # ── 2. Pivot so each row corresponds to a single period (date) ───────────
    df = raw_df.T
    df.index = df.index.tz_localize(None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "period"}, inplace=True)
    df["period"] = pd.to_datetime(df["period"]).dt.date

    # ── 3. Retrieve company_id ───────────────────────────────────────────────
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping cash‑flow insert")
        return
    company_id = row[0]

    cashflow_table = metadata.tables["cashflow"]
    existing_cols = cashflow_table.columns.keys()
    # Build normalised‑name → real column name map
    existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 3a. Dynamically add missing columns ──────────────────────────────────
    extra_fields = []
    sample_values = {}
    for col in df.columns:
        if col == "period":
            continue
        norm_col = normalize_name(col)
        if norm_col not in existing_ci:
            extra_fields.append(norm_col)
            sample_series = df[col].dropna()
            sample_values[norm_col] = sample_series.iloc[0] if not sample_series.empty else None

    added_columns = []
    skipped_cols = {}

    for norm_col in extra_fields:
        val = sample_values.get(norm_col)
        if isinstance(val, (list, dict)):
            skipped_cols[norm_col] = f"type {type(val).__name__}"
            continue
        if isinstance(val, str) and len(val) > 10000:
            skipped_cols[norm_col] = "text too long"
            continue

        if isinstance(val, str):
            col_type = Text()
        elif isinstance(val, bool):
            col_type = Boolean()
        elif isinstance(val, int):
            col_type = Integer()
        elif isinstance(val, float):
            col_type = Float()
        else:
            col_type = String(500)

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE cashflow ADD COLUMN `{norm_col}` {col_type.compile(dialect=engine.dialect)}"
                    )
                )
                added_columns.append(norm_col)
        except SQLAlchemyError:
            skipped_cols[norm_col] = "SQL error"

    if added_columns:
        logging.info(f"➕ {len(added_columns)} new columns added to 'cashflow' for {ticker}")
    if skipped_cols:
        logging.warning(f"⚠️ {len(skipped_cols)} fields ignored for {ticker} in 'cashflow' due to unsupported types or length")

    if added_columns:
        metadata.clear()
        metadata.reflect(bind=engine)
        cashflow_table = metadata.tables["cashflow"]
        existing_cols = cashflow_table.columns.keys()
        existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 4. Iterate rows and upsert ───────────────────────────────────────────
    inserted = 0
    updated = 0

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            period_date = rec["period"]
            row_data = {
                "company_id": company_id,
                "period": period_date,
            }

            for field, value in rec.items():
                if field == "period":
                    continue
                col_name = normalize_name(field)
                target_col = existing_ci.get(col_name)
                if target_col:
                    row_data[target_col] = None if pd.isna(value) else value

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT cashflow_id
                    FROM cashflow
                    WHERE company_id = :cid AND period = :p
                    """
                ),
                {"cid": company_id, "p": period_date},
            ).fetchone()

            if exists:
                upd_vals = {k: v for k, v in row_data.items() if k not in ("company_id", "period")}
                if upd_vals:  # avoid empty update
                    conn.execute(
                        cashflow_table.update()
                        .where(cashflow_table.c.company_id == company_id)
                        .where(cashflow_table.c.period == period_date)
                        .values(**upd_vals)
                    )
                updated += 1
            else:
                conn.execute(cashflow_table.insert().values(**row_data))
                inserted += 1

    # ── 5. Logging summary ──────────────────────────────────────────────────
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'cashflow' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'cashflow' for {ticker}")


# Function to insert or update annual financial‑statement data in the 'financials' table
# It downloads the full statement from yfinance, pivots it per‑date, and performs
# an upsert per (company_id, date).  Fields not present in the table are counted.
def insert_financials_data(ticker):
    # ── 1. Download financials dataframe (rows = accounts, cols = dates) ─────
    try:
        raw_df = yf.Ticker(ticker).financials
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve financials data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if raw_df.empty:
        logging.warning(f"⚠️ No financials data found for {ticker}")
        return

    # ── 2. Pivot: one row per date ───────────────────────────────────────────
    df = raw_df.T
    df.index = df.index.tz_localize(None)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # ── 3. Resolve company_id ────────────────────────────────────────────────
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping financials insert")
        return
    company_id = row[0]

    financials_table = metadata.tables["financials"]
    existing_cols = financials_table.columns.keys()
    # Build a normalization map so we can match regardless of case/format
    existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 3a. Dynamically add missing columns ──────────────────────────────────
    extra_fields = []
    sample_values = {}
    for col in df.columns:
        if col == "date":
            continue
        norm_col = normalize_name(col)
        if norm_col not in existing_ci:
            extra_fields.append(norm_col)
            sample_series = df[col].dropna()
            sample_values[norm_col] = sample_series.iloc[0] if not sample_series.empty else None

    added_columns = []
    skipped_cols = {}

    for norm_col in extra_fields:
        val = sample_values.get(norm_col)
        if isinstance(val, (list, dict)):
            skipped_cols[norm_col] = f"type {type(val).__name__}"
            continue
        if isinstance(val, str) and len(val) > 10000:
            skipped_cols[norm_col] = f"text too long"
            continue

        if isinstance(val, str):
            col_type = Text()
        elif isinstance(val, bool):
            col_type = Boolean()
        elif isinstance(val, int):
            col_type = Integer()
        elif isinstance(val, float):
            col_type = Float()
        else:
            col_type = String(500)

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE financials ADD COLUMN `{norm_col}` {col_type.compile(dialect=engine.dialect)}"
                    )
                )
                added_columns.append(norm_col)
        except SQLAlchemyError:
            skipped_cols[norm_col] = "SQL error"

    if added_columns:
        logging.info(f"➕ {len(added_columns)} new columns added to 'financials' for {ticker}")
    if skipped_cols:
        logging.warning(f"⚠️ {len(skipped_cols)} fields ignored for {ticker} in 'financials' due to unsupported types or length")

    if added_columns:
        metadata.clear()
        metadata.reflect(bind=engine)
        financials_table = metadata.tables["financials"]
        existing_cols = financials_table.columns.keys()
        existing_ci = {normalize_name(c): c for c in existing_cols}

    # ── 4. Iterate rows and upsert ───────────────────────────────────────────
    inserted = 0
    updated = 0

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            date_val = rec["date"]
            row_data = {
                "company_id": company_id,
                "date": date_val,
            }

            for field, value in rec.items():
                if field == "date":
                    continue
                col_name = normalize_name(field)
                target_col = existing_ci.get(col_name)
                if target_col:
                    row_data[target_col] = None if pd.isna(value) else value

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT financials_id
                    FROM financials
                    WHERE company_id = :cid AND date = :d
                    """
                ),
                {"cid": company_id, "d": date_val},
            ).fetchone()

            if exists:
                upd_vals = {k: v for k, v in row_data.items() if k not in ("company_id", "date")}
                if upd_vals:
                    conn.execute(
                        financials_table.update()
                        .where(financials_table.c.company_id == company_id)
                        .where(financials_table.c.date == date_val)
                        .values(**upd_vals)
                    )
                updated += 1
            else:
                conn.execute(financials_table.insert().values(**row_data))
                inserted += 1

    # ── 5. Logging summary ──────────────────────────────────────────────────
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'financials' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'financials' for {ticker}")


# Function to insert or update dividend data in the 'dividends' table
# It pulls the dividend series from yfinance and upserts per (company_id, datetime).
def insert_dividends_data(ticker):
    # 1. Download dividend series (index = dates, values = dividends)
    try:
        ser = yf.Ticker(ticker).dividends
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve dividends data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if ser.empty:
        logging.warning(f"⚠️ No dividend data found for {ticker}")
        return

    # 2. Convert to DataFrame with explicit columns
    df = ser.to_frame(name="dividend")
    df.index = df.index.tz_localize(None)  # drop timezone info
    df.reset_index(inplace=True)              # first column now holds the dates
    # Ensure the first column is named 'datetime' regardless of its original name
    df.columns = ["datetime", "dividend"]
    df["datetime"] = pd.to_datetime(df["datetime"])  # ensure datetime type

    # 3. Resolve company_id
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping dividends insert")
        return
    company_id = row[0]

    div_table = metadata.tables["dividends"]
    existing_cols = div_table.columns.keys()

    # 4. Iterate rows and upsert
    inserted = 0
    updated = 0
    total_missing = 0  # unlikely, but keep for consistency

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            dt_val = rec["datetime"]
            row_data = {
                "company_id": company_id,
                "datetime": dt_val,
            }

            # Only one data column: dividend
            if "dividend" in existing_cols:
                row_data["dividend"] = None if pd.isna(rec["dividend"]) else float(rec["dividend"])
            else:
                total_missing += 1  # dividend column missing (should not happen)

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT dividend_id
                    FROM dividends
                    WHERE company_id = :cid AND datetime = :dt
                    """
                ),
                {"cid": company_id, "dt": dt_val},
            ).fetchone()

            if exists:
                conn.execute(
                    div_table.update()
                    .where(div_table.c.company_id == company_id)
                    .where(div_table.c.datetime == dt_val)
                    .values(dividend=row_data.get("dividend"))
                )
                updated += 1
            else:
                conn.execute(div_table.insert().values(**row_data))
                inserted += 1

    # 5. Logging summary
    if total_missing:
        logging.warning(f"⚠️ {total_missing} fields ignored for {ticker} in 'dividends' due to absent columns")
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'dividends' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'dividends' for {ticker}")


# Function to insert or update recommendation‑summary data in the 'recommendations' table
# It uses yfinance.recommendations_summary (monthly consensus) and upserts per (company_id, period).
def insert_recommendations_data(ticker):
    # 1. Download recommendation summary
    try:
        df = yf.Ticker(ticker).recommendations_summary
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve recommendations_summary data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if df is None or df.empty:
        logging.warning(f"⚠️ No recommendations data found for {ticker}")
        return

    # Expected columns
    expected_cols = ["period", "strongBuy", "buy", "hold", "sell", "strongSell"]
    df = df[expected_cols]
    df["period"] = df["period"].astype(str)

    # 2. Resolve company_id
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping recommendations insert")
        return
    company_id = row[0]

    rec_table = metadata.tables["recommendations"]
    existing_cols = rec_table.columns.keys()

    # 3. Iterate rows and upsert
    inserted = 0
    updated = 0
    total_missing = 0

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            period_val = rec["period"]
            row_data = {
                "company_id": company_id,
                "period": period_val,
            }

            row_missing = 0
            for field in expected_cols[1:]:
                if field in existing_cols:
                    val = rec[field]
                    row_data[field] = None if pd.isna(val) else int(val)
                else:
                    row_missing += 1
            total_missing += row_missing

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT recommendation_id
                    FROM recommendations
                    WHERE company_id = :cid AND period = :p
                    """
                ),
                {"cid": company_id, "p": period_val},
            ).fetchone()

            if exists:
                upd_vals = {k: v for k, v in row_data.items() if k not in ("company_id", "period")}
                if upd_vals:
                    conn.execute(
                        rec_table.update()
                        .where(rec_table.c.company_id == company_id)
                        .where(rec_table.c.period == period_val)
                        .values(**upd_vals)
                    )
                updated += 1
            else:
                conn.execute(rec_table.insert().values(**row_data))
                inserted += 1

    # 4. Logging summary
    if total_missing:
        logging.warning(f"⚠️ {total_missing} fields ignored for {ticker} in 'recommendations' due to absent columns")
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'recommendations' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'recommendations' for {ticker}")


# Function to insert or update stock‑split data in the 'splits' table
# It pulls the split series from yfinance and upserts per (company_id, date).
def insert_splits_data(ticker):
    # 1. Download split series (index = dates, values = split ratios)
    try:
        ser = yf.Ticker(ticker).splits
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve splits data for {ticker}: {e} (possibly missing or unsupported on Yahoo Finance)")
        return

    if ser.empty:
        logging.warning(f"⚠️ No splits data found for {ticker}")
        return

    # 2. Convert to DataFrame with explicit columns
    df = ser.to_frame(name="split_ratio")
    df.index = df.index.tz_localize(None)  # remove timezone
    df.reset_index(inplace=True)
    df.columns = ["date", "split_ratio"]
    df["date"] = pd.to_datetime(df["date"])  # keep full datetime

    # 3. Resolve company_id
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping splits insert")
        return
    company_id = row[0]

    splits_table = metadata.tables["splits"]
    existing_cols = splits_table.columns.keys()

    # 4. Iterate rows and upsert
    inserted = 0
    updated = 0
    total_missing = 0  # keep for consistency

    with engine.begin() as conn:
        for _, rec in df.iterrows():
            date_val = rec["date"]
            row_data = {
                "company_id": company_id,
                "date": date_val,
            }

            if "split_ratio" in existing_cols:
                row_data["split_ratio"] = None if pd.isna(rec["split_ratio"]) else float(rec["split_ratio"])
            else:
                total_missing += 1

            # Upsert logic
            exists = conn.execute(
                text(
                    """
                    SELECT split_id
                    FROM splits
                    WHERE company_id = :cid AND date = :d
                    """
                ),
                {"cid": company_id, "d": date_val},
            ).fetchone()

            if exists:
                conn.execute(
                    splits_table.update()
                    .where(splits_table.c.company_id == company_id)
                    .where(splits_table.c.date == date_val)
                    .values(split_ratio=row_data.get("split_ratio"))
                )
                updated += 1
            else:
                conn.execute(splits_table.insert().values(**row_data))
                inserted += 1

    # 5. Logging summary
    if total_missing:
        logging.warning(f"⚠️ {total_missing} fields ignored for {ticker} in 'splits' due to absent columns")
    if inserted:
        logging.info(f"➕ {inserted} new rows inserted into 'splits' for {ticker}")
    if updated:
        logging.info(f"🔁 {updated} rows updated in 'splits' for {ticker}")


# Function to insert or update ESG sustainability data in the 'sustainability' table
# It extracts the 'esgScores' object from yfinance.info and upserts per (company_id, rating_year, rating_month).
def insert_sustainability_data(ticker):
    stock = yf.Ticker(ticker)

    try:
        # Try the newer .sustainability DataFrame first
        esg_df = stock.sustainability
    except Exception as e:
        logging.warning(f"⚠️ Failed to retrieve sustainability data for {ticker}: {e} (likely due to missing ESG endpoint on Yahoo Finance)")
        return
    if esg_df is not None and not esg_df.empty:
        # Most yfinance versions: single unnamed or 'Value' column
        if esg_df.shape[1] == 1:
            esg = esg_df.iloc[:, 0].to_dict()
        else:
            # Rare case: already flattened
            esg = esg_df.iloc[0].to_dict()
    else:
        esg = stock.info.get("esgScores") or {}

    if not esg:
        logging.warning(f"⚠️ No sustainability data found for {ticker}")
        return

    # 1. Dynamically flatten the ESG dict
    flat_esg = {}

    for k, v in esg.items():
        if isinstance(v, dict) and {"min", "avg", "max"} <= set(v.keys()):
            # For nested performance objects, keep snake_case names
            base = normalize_name(k)  # e.g. peerEsgScorePerformance → peer_esg_score_performance
            for subk in ("min", "avg", "max"):
                flat_esg[f"{base}_{subk}"] = v.get(subk)
        else:
            # Keep original CamelCase key to match table columns (e.g. maxAge, totalEsg)
            flat_esg[k] = v

    # 2. Resolve company_id
    metadata = MetaData()
    metadata.reflect(bind=engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT company_id FROM info WHERE symbol = :sym"),
            {"sym": ticker},
        ).fetchone()
    if not row:
        logging.warning(f"⚠️ No company_id found for {ticker}; skipping sustainability insert")
        return
    company_id = row[0]

    sust_table = metadata.tables["sustainability"]
    existing_cols = sust_table.columns.keys()

    # ── 3a. Dynamically add missing columns (like insert_info_data) ────────────
    extra_fields = []
    sample_values = {}

    for col, val in flat_esg.items():
        if col not in existing_cols:
            extra_fields.append(col)
            sample_values[col] = val

    added_columns = []
    skipped_columns_dyn = {}

    for col in extra_fields:
        val = sample_values[col]
        if isinstance(val, (list, dict)):
            skipped_columns_dyn[col] = f"type {type(val).__name__}"
            continue
        if isinstance(val, str) and len(val) > 10_000:
            skipped_columns_dyn[col] = f"text too long ({len(val)} characters)"
            continue

        # Map python -> SQLAlchemy
        if isinstance(val, str):
            col_type = Text()
        elif isinstance(val, bool):
            col_type = Boolean()
        elif isinstance(val, int):
            col_type = Integer()
        elif isinstance(val, float):
            col_type = Float()
        elif val is None:
            col_type = String(500)
        else:
            skipped_columns_dyn[col] = f"unsupported type {type(val)}"
            continue

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE sustainability ADD COLUMN `{col}` {col_type.compile(dialect=engine.dialect)}"
                    )
                )
                added_columns.append((col, col_type.__class__.__name__))
        except SQLAlchemyError as e:
            skipped_columns_dyn[col] = f"SQL error: {e}"

    if added_columns:
        logging.info(f"➕ {len(added_columns)} new columns added to 'sustainability' for {ticker}")
    if skipped_columns_dyn:
        logging.warning(f"⚠️ {len(skipped_columns_dyn)} fields ignored for {ticker} in 'sustainability' due to unsupported types or length")

    if added_columns:
        # Refresh metadata and column list
        metadata.clear()
        metadata.reflect(bind=engine)
        sust_table = metadata.tables["sustainability"]
        existing_cols = sust_table.columns.keys()

    # 3b. Build row with only existing columns (after dynamic schema), count any still missing
    row_data = {"company_id": company_id}
    missing_fields = 0
    for col, val in flat_esg.items():
        if col in existing_cols:
            row_data[col] = None if pd.isna(val) else val
        else:
            missing_fields += 1

    # ── Upsert using only company_id (one ESG snapshot per company) ───────────
    inserted = 0
    updated = 0
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT sustainability_id FROM sustainability WHERE company_id = :cid"),
            {"cid": company_id},
        ).fetchone()

        if exists:
            upd_vals = {k: v for k, v in row_data.items() if k != "company_id"}
            conn.execute(
                sust_table.update()
                .where(sust_table.c.company_id == company_id)
                .values(**upd_vals)
            )
            updated = 1
        else:
            conn.execute(sust_table.insert().values(**row_data))
            inserted = 1

    # 5. Logging
    if inserted:
        logging.info(f"➕ 1 new row inserted into 'sustainability' for {ticker}")
    if updated:
        logging.info(f"🔁 1 row updated in 'sustainability' for {ticker}")

# Execute the update process for each ticker in the list
for ticker in tickers:
    print(f"➡️ Processing: {ticker}")
    insert_info_data(ticker)
    insert_history_data(ticker)
    insert_balance_sheet_data(ticker)
    insert_cashflow_data(ticker)
    insert_financials_data(ticker)
    insert_dividends_data(ticker)
    insert_recommendations_data(ticker)
    insert_splits_data(ticker)
    insert_sustainability_data(ticker)
