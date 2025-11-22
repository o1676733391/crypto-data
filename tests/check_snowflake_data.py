"""Check if data was inserted into Snowflake"""
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

ctx = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role=os.getenv('SNOWFLAKE_ROLE'),
)

cur = ctx.cursor()

# Check tables
print("=== Checking Snowflake Tables ===\n")

cur.execute("SHOW TABLES")
tables = cur.fetchall()
print(f"Tables in {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}:")
for table in tables:
    print(f"  - {table[1]}")

print("\n" + "="*50 + "\n")

# Check MARKET_TICKS
print("MARKET_TICKS table:")
cur.execute("SELECT COUNT(*) FROM MARKET_TICKS")
count = cur.fetchone()[0]
print(f"  Total rows: {count}")

if count > 0:
    cur.execute("SELECT SYMBOL, EXCHANGE_TS, LAST_PRICE, INGESTED_AT FROM MARKET_TICKS ORDER BY INGESTED_AT DESC LIMIT 5")
    rows = cur.fetchall()
    print("\n  Latest 5 rows:")
    for row in rows:
        print(f"    {row[0]}: ${row[2]} @ {row[1]} (ingested: {row[3]})")

print("\n" + "="*50 + "\n")

# Check TECHNICAL_INDICATORS
print("TECHNICAL_INDICATORS table:")
cur.execute("SELECT COUNT(*) FROM TECHNICAL_INDICATORS")
count = cur.fetchone()[0]
print(f"  Total rows: {count}")

if count > 0:
    cur.execute("SELECT SYMBOL, EXCHANGE_TS, RSI_14, MACD, CREATED_AT FROM TECHNICAL_INDICATORS ORDER BY CREATED_AT DESC LIMIT 5")
    rows = cur.fetchall()
    print("\n  Latest 5 rows:")
    for row in rows:
        rsi_str = f"{row[2]:.2f}" if row[2] is not None else "N/A"
        macd_str = f"{row[3]:.4f}" if row[3] is not None else "N/A"
        print(f"    {row[0]}: RSI={rsi_str}, MACD={macd_str} @ {row[1]} (created: {row[4]})")

cur.close()
ctx.close()
