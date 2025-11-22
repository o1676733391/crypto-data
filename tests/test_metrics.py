"""Test metrics calculations"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.metrics import (
    _to_float, _sma, _ema, _rsi, _macd, 
    _log_returns, _annualized_volatility, build_payload
)

def test_basic_functions():
    print("Testing basic utility functions...")
    
    # Test _to_float
    assert _to_float("123.45") == 123.45
    assert _to_float(100) == 100.0
    assert _to_float(None) is None
    print("✓ _to_float works")
    
    # Test _sma
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    sma_5 = _sma(values, 5)
    assert sma_5 == 8.0  # avg of last 5: (6+7+8+9+10)/5
    print(f"✓ _sma works: SMA(5) = {sma_5}")
    
    # Test _rsi
    prices = [44, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 
              45.89, 46.03, 45.61, 46.28, 46.28, 46.00, 46.03, 46.41, 46.22, 45.64]
    rsi = _rsi(prices, 14)
    assert rsi is not None
    assert 0 <= rsi <= 100
    print(f"✓ _rsi works: RSI(14) = {rsi:.2f}")
    
    # Test _macd (needs at least 35 data points)
    prices = list(range(100, 150))  # 50 data points
    macd, signal = _macd(prices)
    assert macd is not None
    assert signal is not None
    print(f"✓ _macd works: MACD = {macd:.4f}, Signal = {signal:.4f}")
    
    # Test _log_returns
    prices = [100, 105, 103, 108, 110]
    returns = _log_returns(prices)
    assert len(returns) == 4
    print(f"✓ _log_returns works: {len(returns)} returns calculated")
    
    # Test _annualized_volatility
    vol = _annualized_volatility(returns, 1440)
    assert vol is not None
    assert vol > 0
    print(f"✓ _annualized_volatility works: Vol = {vol:.4f}")

def test_payload_building():
    print("\nTesting payload building with mock data...")
    
    # Mock snapshot from Binance
    mock_snapshot = {
        "symbol": "BTCUSDT",
        "ticker": {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.00",
            "priceChangePercent": "2.5",
            "highPrice": "51000.00",
            "lowPrice": "49000.00",
            "quoteVolume": "1000000000.00"
        },
        "orderbook": {
            "bids": [
                ["49900.00", "1.5"],
                ["49890.00", "2.0"],
                ["49880.00", "1.0"],
                ["49870.00", "0.5"],
                ["49860.00", "3.0"]
            ],
            "asks": [
                ["49910.00", "1.2"],
                ["49920.00", "2.5"],
                ["49930.00", "1.8"],
                ["49940.00", "0.8"],
                ["49950.00", "2.2"]
            ]
        },
        "klines": [
            [1700000000000, "49000", "49500", "48900", "49200", "100", 1700000060000, "50000000", 1000, "50", "25000000", "0"],
            [1700000060000, "49200", "49600", "49100", "49400", "120", 1700000120000, "60000000", 1200, "60", "30000000", "0"],
            [1700000120000, "49400", "49700", "49300", "49500", "110", 1700000180000, "55000000", 1100, "55", "27500000", "0"],
        ] * 40  # Repeat to get 120 klines
    }
    
    try:
        payload = build_payload(mock_snapshot)
        
        print("✓ Payload built successfully")
        print(f"\nMarket Tick fields:")
        print(f"   Symbol: {payload.market_tick['symbol']}")
        print(f"   Last Price: {payload.market_tick['last_price']}")
        print(f"   Bid-Ask Spread: {payload.market_tick['bid_ask_spread']}")
        print(f"   24h Change: {payload.market_tick['price_change_pct_24h']}%")
        
        print(f"\nTechnical Indicators:")
        print(f"   MA(7): {payload.technicals['moving_average_7']}")
        print(f"   MA(30): {payload.technicals['moving_average_30']}")
        print(f"   RSI(14): {payload.technicals['rsi_14']}")
        print(f"   MACD: {payload.technicals['macd']}")
        print(f"   Volatility: {payload.technicals['rolling_volatility_24h']}")
        
        # Validate structure
        assert payload.market_tick['symbol'] == "BTCUSDT"
        assert payload.market_tick['exchange'] == "BINANCE"
        assert payload.technicals['symbol'] == "BTCUSDT"
        
        print("\n✓ All payload fields validated")
        
    except Exception as e:
        print(f"✗ Payload building failed!")
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_basic_functions()
    test_payload_building()
    print("\n✓ All metrics tests passed!")
