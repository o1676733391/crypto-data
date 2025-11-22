"""
Interactive script to test and fix Snowflake credentials
"""
import snowflake.connector
from getpass import getpass

print("=== Snowflake Credentials Fixer ===\n")
print("Current credentials from .env:")
print("Account: hkvparm-ec66514")
print("User: CRYPTO_INGEST")
print("Password: v212011V@")
print("Warehouse: CRYPTO_WH")
print("Database: CRYPTO")
print("Schema: PUBLIC")
print("Role: ACCOUNTADMIN")
print("\n" + "="*50 + "\n")

print("Let's test different variations:\n")

# Test 1: Original credentials
print("Test 1: Trying original credentials...")
try:
    ctx = snowflake.connector.connect(
        account="hkvparm-ec66514",
        user="CRYPTO_INGEST",
        password="v212011V@",
        warehouse="CRYPTO_WH",
        database="CRYPTO",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
    )
    print("✓ SUCCESS! Original credentials work!")
    ctx.close()
    exit(0)
except Exception as e:
    print(f"✗ Failed: {str(e)}\n")

# Test 2: Try lowercase username
print("Test 2: Trying lowercase username...")
try:
    ctx = snowflake.connector.connect(
        account="hkvparm-ec66514",
        user="crypto_ingest",
        password="v212011V@",
        warehouse="CRYPTO_WH",
        database="CRYPTO",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
    )
    print("✓ SUCCESS! Lowercase username works!")
    print("\nUpdate your .env file:")
    print("SNOWFLAKE_USER=crypto_ingest")
    ctx.close()
    exit(0)
except Exception as e:
    print(f"✗ Failed: {str(e)}\n")

# Test 3: Try different account format
print("Test 3: Trying account without region...")
try:
    ctx = snowflake.connector.connect(
        account="hkvparm",
        user="CRYPTO_INGEST",
        password="v212011V@",
        warehouse="CRYPTO_WH",
        database="CRYPTO",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
    )
    print("✓ SUCCESS! Account format 'hkvparm' works!")
    print("\nUpdate your .env file:")
    print("SNOWFLAKE_ACCOUNT=hkvparm")
    ctx.close()
    exit(0)
except Exception as e:
    print(f"✗ Failed: {str(e)}\n")

# Test 4: Try with full account URL format
print("Test 4: Trying full URL format...")
try:
    ctx = snowflake.connector.connect(
        account="hkvparm-ec66514.snowflakecomputing.com",
        user="CRYPTO_INGEST",
        password="v212011V@",
        warehouse="CRYPTO_WH",
        database="CRYPTO",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
    )
    print("✓ SUCCESS! Full URL format works!")
    print("\nUpdate your .env file:")
    print("SNOWFLAKE_ACCOUNT=hkvparm-ec66514.snowflakecomputing.com")
    ctx.close()
    exit(0)
except Exception as e:
    print(f"✗ Failed: {str(e)}\n")

print("\n" + "="*50)
print("\nNone of the automatic fixes worked.")
print("\nPlease manually enter your Snowflake credentials:\n")

account = input("Account identifier (e.g., abc12345 or abc12345.us-east-1): ").strip()
user = input("Username: ").strip()
password = getpass("Password: ")
warehouse = input("Warehouse (default: CRYPTO_WH): ").strip() or "CRYPTO_WH"
database = input("Database (default: CRYPTO): ").strip() or "CRYPTO"
schema = input("Schema (default: PUBLIC): ").strip() or "PUBLIC"
role = input("Role (default: ACCOUNTADMIN): ").strip() or "ACCOUNTADMIN"

print("\nTesting your credentials...")
try:
    ctx = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
    print("\n✓ SUCCESS! Your credentials work!\n")
    print("Update your .env file with these values:")
    print(f"SNOWFLAKE_ACCOUNT={account}")
    print(f"SNOWFLAKE_USER={user}")
    print(f"SNOWFLAKE_PASSWORD={password}")
    print(f"SNOWFLAKE_WAREHOUSE={warehouse}")
    print(f"SNOWFLAKE_DATABASE={database}")
    print(f"SNOWFLAKE_SCHEMA={schema}")
    print(f"SNOWFLAKE_ROLE={role}")
    ctx.close()
except Exception as e:
    print(f"\n✗ Still failed: {str(e)}")
    print("\nPlease check your Snowflake account and verify:")
    print("1. Username and password are correct")
    print("2. Account identifier is in the right format")
    print("3. User has necessary permissions")
    print("4. Network access is allowed")
