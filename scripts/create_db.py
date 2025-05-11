# Import libraries
import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey
)
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read database configuration
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB")

# Debug: print loaded values
print("USER:", MYSQL_USER)
print("PASSWORD:", MYSQL_PASSWORD)
print("HOST:", MYSQL_HOST)
print("PORT:", MYSQL_PORT)
print("DB:", MYSQL_DB)

# Check for missing variables
if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB]):
    raise ValueError("Missing environment variables in .env file.")

# Build connection string
db_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# Create SQLAlchemy engine
try:
    engine = create_engine(db_url)
    metadata = MetaData()

    # Define tables
    companies = Table(
        "companies", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(10), unique=True, nullable=False),
        Column("name", String(255), nullable=False),
        Column("sector", String(100))
    )

    market_data = Table(
        "market_data", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(10), ForeignKey("companies.ticker")),
        Column("date", Date, nullable=False),
        Column("open", Float),
        Column("close", Float),
        Column("volume", Integer)
    )

    fundamentals = Table(
        "fundamentals", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("ticker", String(10), ForeignKey("companies.ticker")),
        Column("pe_ratio", Float),
        Column("roe", Float),
        Column("eps", Float),
        Column("profit_margin", Float),
        Column("updated_at", Date)
    )

    # Create tables in the database
    metadata.create_all(engine)

    print("✅ MySQL connection successfully created.")
    print("✅ Tables created successfully.")

except OperationalError as e:
    print("MySQL connection error:")
    print(e)
