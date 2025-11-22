"""Test script to verify Snowflake aggregation tables and data."""
import asyncio
import logging
from datetime import datetime, timedelta
from src.aggregator import SnowflakeAggregator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_aggregation():
    """Test the aggregation pipeline."""
    aggregator = SnowflakeAggregator()
    
    try:
        # Create tables
        logger.info("Creating aggregation tables...")
        await aggregator.create_aggregation_tables()
        
        # Create views
        logger.info("Creating analytical views...")
        await aggregator.create_analytical_views()
        
        # Run aggregation
        logger.info("Running aggregation pipeline...")
        await aggregator.run_full_aggregation_pipeline()
        
        # Query results
        conn = aggregator._get_connection()
        cursor = conn.cursor()
        
        # Check 1-minute candles
        cursor.execute("""
            SELECT symbol, COUNT(*) as candle_count, 
                   MIN(candle_time) as first_candle, 
                   MAX(candle_time) as last_candle
            FROM CANDLES_1MIN
            GROUP BY symbol
            ORDER BY symbol
        """)
        logger.info("\n=== 1-MINUTE CANDLES ===")
        for row in cursor.fetchall():
            logger.info(f"Symbol: {row[0]}, Count: {row[1]}, First: {row[2]}, Last: {row[3]}")
        
        # Check 5-minute candles
        cursor.execute("""
            SELECT symbol, COUNT(*) as candle_count,
                   MIN(candle_time) as first_candle,
                   MAX(candle_time) as last_candle
            FROM CANDLES_5MIN
            GROUP BY symbol
            ORDER BY symbol
        """)
        logger.info("\n=== 5-MINUTE CANDLES ===")
        for row in cursor.fetchall():
            logger.info(f"Symbol: {row[0]}, Count: {row[1]}, First: {row[2]}, Last: {row[3]}")
        
        # Check 1-hour candles
        cursor.execute("""
            SELECT symbol, COUNT(*) as candle_count,
                   MIN(candle_time) as first_candle,
                   MAX(candle_time) as last_candle
            FROM CANDLES_1HOUR
            GROUP BY symbol
            ORDER BY symbol
        """)
        logger.info("\n=== 1-HOUR CANDLES ===")
        for row in cursor.fetchall():
            logger.info(f"Symbol: {row[0]}, Count: {row[1]}, First: {row[2]}, Last: {row[3]}")
        
        # Check daily candles
        cursor.execute("""
            SELECT symbol, COUNT(*) as candle_count,
                   MIN(candle_time) as first_candle,
                   MAX(candle_time) as last_candle
            FROM CANDLES_DAILY
            GROUP BY symbol
            ORDER BY symbol
        """)
        logger.info("\n=== DAILY CANDLES ===")
        for row in cursor.fetchall():
            logger.info(f"Symbol: {row[0]}, Count: {row[1]}, First: {row[2]}, Last: {row[3]}")
        
        # Check latest prices view
        cursor.execute("""
            SELECT symbol, last_price, volume_24h, seconds_ago
            FROM VW_LATEST_PRICES
            ORDER BY symbol
        """)
        logger.info("\n=== LATEST PRICES VIEW ===")
        for row in cursor.fetchall():
            logger.info(f"Symbol: {row[0]}, Price: ${row[1]:,.2f}, Volume 24h: {row[2]:,.0f}, Age: {row[3]}s")
        
        # Show sample 5-minute candles
        cursor.execute("""
            SELECT symbol, candle_time, open, high, low, close, volume, trade_count
            FROM CANDLES_5MIN
            WHERE candle_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
            ORDER BY symbol, candle_time DESC
            LIMIT 20
        """)
        logger.info("\n=== RECENT 5-MIN CANDLES (Last hour) ===")
        for row in cursor.fetchall():
            logger.info(f"{row[0]} @ {row[1]}: O:{row[2]:.2f} H:{row[3]:.2f} L:{row[4]:.2f} C:{row[5]:.2f} V:{row[6]:.2f} Trades:{row[7]}")
        
        cursor.close()
        logger.info("\n✅ Aggregation test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Aggregation test failed: {e}", exc_info=True)
    finally:
        aggregator.close()


if __name__ == "__main__":
    asyncio.run(test_aggregation())
