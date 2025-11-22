"""Test Binance API fetcher"""
import asyncio
from dotenv import load_dotenv
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.fetcher import BinanceDataSource, fetch_market_data

load_dotenv()

async def test_binance():
    print("Testing Binance API connection...")
    print()
    
    try:
        client = BinanceDataSource()
        print("✓ BinanceDataSource initialized")
        
        # Test single ticker endpoint (actually used by the app)
        print("\n1. Testing single ticker...")
        ticker = await client.get_ticker_single("BTCUSDT")
        print(f"✓ Got ticker data for BTCUSDT")
        print(f"   Last Price = {ticker.get('lastPrice')}, 24h Change = {ticker.get('priceChangePercent')}%")
        
        # Test order book
        print("\n2. Testing order book...")
        orderbook = await client.get_order_book("BTCUSDT")
        print(f"✓ Got order book with {len(orderbook.get('bids', []))} bids and {len(orderbook.get('asks', []))} asks")
        if orderbook.get('bids'):
            print(f"   Best bid: {orderbook['bids'][0]}")
        if orderbook.get('asks'):
            print(f"   Best ask: {orderbook['asks'][0]}")
        
        # Test klines
        print("\n3. Testing klines...")
        klines = await client.get_recent_klines("BTCUSDT", interval="1m", limit=5)
        print(f"✓ Got {len(klines)} klines")
        if klines:
            latest = klines[-1]
            print(f"   Latest: Open={latest[1]}, High={latest[2]}, Low={latest[3]}, Close={latest[4]}")
        
        # Test full snapshot
        print("\n4. Testing full snapshot...")
        snapshot = await client.fetch_symbol_snapshot("ETHUSDT")
        print(f"✓ Got full snapshot for {snapshot['symbol']}")
        print(f"   Ticker: ✓")
        print(f"   Order Book: ✓")
        print(f"   Klines: {len(snapshot['klines'])} candles")
        
        # Test multi-symbol fetch
        print("\n5. Testing multi-symbol fetch...")
        test_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        snapshots = await fetch_market_data(test_symbols)
        print(f"✓ Got snapshots for {len(snapshots)} symbols")
        for snap in snapshots:
            print(f"   {snap['symbol']}: ✓")
        
        await client.close()
        print("\n✓ All Binance tests passed!")
        
    except Exception as e:
        print(f"\n✗ Binance test failed!")
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_binance())
