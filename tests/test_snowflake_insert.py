"""Test Snowflake insert with PARSE_JSON"""
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.snowflake_client import SnowflakeWriter

async def test_insert():
    print("Testing Snowflake insert with JSON columns...")
    
    writer = SnowflakeWriter()
    
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
        "bid_depth": {
            "levels": [
                {"price": 100.5, "quantity": 10.0},
                {"price": 100.4, "quantity": 15.0}
            ],
            "base_total": 25.0,
            "quote_total": 2512.5
        },
        "ask_depth": {
            "levels": [
                {"price": 100.6, "quantity": 8.0},
                {"price": 100.7, "quantity": 12.0}
            ],
            "base_total": 20.0,
            "quote_total": 2012.0
        },
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
    
    try:
        print("\n1. Testing market_tick insert...")
        await writer.insert_market_tick(sample_market)
        print("✓ Market tick inserted successfully!")
        
        print("\n2. Testing technical indicators insert...")
        await writer.insert_technicals(sample_technical)
        print("✓ Technical indicators inserted successfully!")
        
        print("\n3. Testing batch insert...")
        await writer.insert_pair(sample_market, sample_technical)
        print("✓ Batch insert successful!")
        
        print("\n✓ All Snowflake tests passed!")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        writer.close()

if __name__ == "__main__":
    asyncio.run(test_insert())
