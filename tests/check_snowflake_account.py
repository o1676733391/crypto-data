"""
Quick Snowflake account format checker
"""
import snowflake.connector

# Common Snowflake account formats to try
account_formats = [
    "hkvparm-ec66514",  # Original
    "ec66514.us-east-1",  # Region format
    "ec66514.eu-west-1",
    "ec66514.ap-southeast-1",
    "hkvparm.ec66514",  # Org.account format
    "HKVPARM-EC66514",  # Uppercase
]

username = input("Enter your Snowflake username: ").strip()
password = input("Enter your Snowflake password: ").strip()

print(f"\nTrying different account formats with username: {username}\n")

for account in account_formats:
    print(f"Testing: {account}...", end=" ")
    try:
        ctx = snowflake.connector.connect(
            account=account,
            user=username,
            password=password,
            login_timeout=5
        )
        print("✓ SUCCESS!")
        print(f"\nWorking credentials:")
        print(f"SNOWFLAKE_ACCOUNT={account}")
        print(f"SNOWFLAKE_USER={username}")
        print(f"SNOWFLAKE_PASSWORD={password}")
        
        # Get more info
        cur = ctx.cursor()
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION()")
        result = cur.fetchone()
        print(f"\nSnowflake Account: {result[0]}")
        print(f"Snowflake Region: {result[1]}")
        
        ctx.close()
        exit(0)
    except Exception as e:
        error_str = str(e)
        if "404" in error_str:
            print("✗ (Account not found)")
        elif "locked" in error_str.lower():
            print("✗ (Account locked)")
        elif "password" in error_str.lower():
            print("✗ (Invalid credentials)")
        else:
            print(f"✗ ({error_str[:50]}...)")

print("\n\nNo working format found. Please check your Snowflake account details:")
print("1. Log into Snowflake web UI")
print("2. Look at the URL: https://<ACCOUNT_IDENTIFIER>.snowflakecomputing.com")
print("3. The ACCOUNT_IDENTIFIER is what you need")
