"""Test Supabase connection and operations"""
import asyncio
from dotenv import load_dotenv
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.supabase_client import SupabaseWriter

load_dotenv()

async def test_supabase():
    print("Testing Supabase connection...")
    print(f"URL: {os.getenv('SUPABASE_URL')}")
    print(f"Market Ticks Table: {os.getenv('SUPABASE_MARKET_TICKS_TABLE')}")
    print(f"Technicals Table: {os.getenv('SUPABASE_TECHNICALS_TABLE')}")
    print()

    try:
        writer = SupabaseWriter()
        print("✓ SupabaseWriter initialized successfully!")
        
        # Test with sample data
        from datetime import datetime, timezone
        sample_market = {
            "symbol": "TESTUSDT",
            "exchange": "TEST",
            "window_interval": "1m",
            "exchange_ts": datetime.now(tz=timezone.utc).isoformat(),
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 102.0,
            "last_price": 102.5,
            "price_change_pct_1h": 2.5,
            "price_change_pct_24h": 5.0,
            "volume_24h_quote": 1000000.0,
            "volume_change_pct_24h": None,
            "market_cap": None,
            "circulating_supply": None,
            "bid_ask_spread": 0.01,
            "bid_depth": {"levels": [], "base_total": 0, "quote_total": 0},
            "ask_depth": {"levels": [], "base_total": 0, "quote_total": 0},
            "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        
        sample_technical = {
            "symbol": "TESTUSDT",
            "window_interval": "1m",
            "exchange_ts": datetime.now(tz=timezone.utc).isoformat(),
            "rolling_return_1h": 0.025,
            "rolling_return_24h": 0.05,
            "rolling_volatility_24h": 0.15,
            "high_low_range_24h": 10.0,
            "moving_average_7": 101.0,
            "moving_average_30": 100.0,
            "rsi_14": 55.0,
            "macd": 0.5,
            "macd_signal": 0.3,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        
        print("\nAttempting test insert...")
        await writer.insert_pair(sample_market, sample_technical)
        print("✓ Test data inserted successfully!")
        
    except Exception as e:
        print(f"✗ Supabase test failed!")
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_supabase())
