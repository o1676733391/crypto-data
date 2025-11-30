import asyncio
import os
import sys
from datetime import datetime
import snowflake.connector
from supabase import create_client

# Add parent dir to path to import src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_settings

async def measure_snowflake():
    print("Connecting to Snowflake...")
    settings = get_settings()
    try:
        ctx = snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            role=settings.snowflake_role,
        )
        cursor = ctx.cursor()
        
        # 1. Total Rows
        cursor.execute("SELECT COUNT(*) FROM MARKET_TICKS")
        market_ticks_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM PROTOCOL_TVL")
        protocol_tvl_count = cursor.fetchone()[0]
        
        # 2. Distinct Symbols/Protocols
        cursor.execute("SELECT COUNT(DISTINCT SYMBOL) FROM MARKET_TICKS")
        market_symbols_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT PROTOCOL_SLUG) FROM PROTOCOL_TVL")
        protocol_count = cursor.fetchone()[0]
        
        # 3. Storage Usage (Estimate)
        # Snowflake doesn't give direct bytes per table easily without admin views, 
        # but we can try to query information_schema if available.
        try:
            cursor.execute(f"SELECT SUM(BYTES) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{settings.snowflake_schema}'")
            total_bytes = cursor.fetchone()[0]
        except:
            total_bytes = 0
            
        print("\n--- Snowflake Metrics ---")
        print(f"MARKET_TICKS Rows: {market_ticks_count}")
        print(f"PROTOCOL_TVL Rows: {protocol_tvl_count}")
        print(f"Tracked Crypto Symbols: {market_symbols_count}")
        print(f"Tracked DeFi Protocols: {protocol_count}")
        if total_bytes:
            print(f"Total Storage: {total_bytes / 1024 / 1024:.2f} MB")
            
        ctx.close()
        return {
            "market_ticks": market_ticks_count,
            "protocol_tvl": protocol_tvl_count,
            "crypto_symbols": market_symbols_count,
            "defi_protocols": protocol_count,
            "storage_mb": total_bytes / 1024 / 1024 if total_bytes else 0
        }
    except Exception as e:
        print(f"Snowflake connection failed: {e}")
        return None

async def measure_supabase():
    print("\nConnecting to Supabase...")
    settings = get_settings()
    try:
        supabase = create_client(str(settings.supabase_url), settings.supabase_service_role_key)
        
        # Supabase API count
        res = supabase.table(settings.supabase_market_ticks_table).select("id", count="exact").execute()
        count = res.count
        
        print("\n--- Supabase Metrics ---")
        print(f"Real-time Market Ticks: {count}")
        return count
    except Exception as e:
        print(f"Supabase connection failed: {e}")
        return None

async def main():
    await measure_snowflake()
    await measure_supabase()

if __name__ == "__main__":
    asyncio.run(main())
