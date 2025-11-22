import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

print("Testing Snowflake connection...")
print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"User: {os.getenv('SNOWFLAKE_USER')}")
print(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
print(f"Database: {os.getenv('SNOWFLAKE_DATABASE')}")
print(f"Schema: {os.getenv('SNOWFLAKE_SCHEMA')}")
print(f"Role: {os.getenv('SNOWFLAKE_ROLE')}")
print()

try:
    ctx = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE'),
    )
    print("✓ Connection successful!")
    
    # Test query
    cur = ctx.cursor()
    cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    result = cur.fetchone()
    print(f"\nSnowflake Version: {result[0]}")
    print(f"Current User: {result[1]}")
    print(f"Current Role: {result[2]}")
    print(f"Current Database: {result[3]}")
    print(f"Current Schema: {result[4]}")
    
    # Check if tables exist
    cur.execute("SHOW TABLES LIKE 'MARKET_TICKS'")
    market_ticks = cur.fetchone()
    print(f"\nMARKET_TICKS table exists: {market_ticks is not None}")
    
    cur.execute("SHOW TABLES LIKE 'TECHNICAL_INDICATORS'")
    tech_indicators = cur.fetchone()
    print(f"TECHNICAL_INDICATORS table exists: {tech_indicators is not None}")
    
    cur.close()
    ctx.close()
    
except Exception as e:
    print(f"✗ Connection failed!")
    print(f"Error: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()
