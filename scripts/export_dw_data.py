import os
import sys
import pandas as pd
import snowflake.connector
from datetime import datetime

# Add parent dir to path to import src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_settings

def export_data():
    print("Connecting to Snowflake...")
    settings = get_settings()
    
    # Create export directory
    export_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_export")
    os.makedirs(export_dir, exist_ok=True)
    
    try:
        # Support key-pair authentication for MFA accounts
        pk_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
        pk_pass = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        connect_kwargs = dict(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            role=settings.snowflake_role,
        )
        if pk_path:
            try:
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.backends import default_backend
            except ImportError:
                raise RuntimeError("cryptography library is required for private-key auth. Install 'cryptography'.")
            with open(pk_path, "rb") as f:
                key_data = f.read()
            password_bytes = pk_pass.encode() if pk_pass else None
            pkey = serialization.load_pem_private_key(key_data, password=password_bytes, backend=default_backend())
            private_key_bytes = pkey.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            connect_kwargs["private_key"] = private_key_bytes
        else:
            connect_kwargs["password"] = settings.snowflake_password

        ctx = snowflake.connector.connect(**connect_kwargs)
        
        tables = ["MARKET_TICKS", "PROTOCOL_TVL"]
        
        for table in tables:
            print(f"Exporting {table}...")
            # Using LIMIT for safety, remove or increase if you want full dump
            # If warehouse is suspended, this query will auto-resume it.
            query = f"SELECT * FROM {table}"
            
            # Read into DataFrame
            df = pd.read_sql(query, ctx)
            
            # Save to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table}_{timestamp}.csv"
            filepath = os.path.join(export_dir, filename)
            
            df.to_csv(filepath, index=False)
            print(f"Saved {len(df)} rows to {filepath}")
            
        ctx.close()
        print("\nExport complete!")
        
    except Exception as e:
        print(f"Export failed: {e}")
        if "suspended" in str(e).lower():
            print("Note: If your ACCOUNT is suspended, you cannot export data via API.")
            print("If only the WAREHOUSE is suspended, this script should have auto-resumed it.")

if __name__ == "__main__":
    export_data()
