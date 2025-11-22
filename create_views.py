"""
Script to create analytical views in Snowflake
Run this once to set up all DeFi analytical views
"""
import asyncio
import logging
from src.defi_snowflake_client import DefiSnowflakeWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def main():
    """Create all analytical views"""
    logger.info("Starting analytical views creation...")
    
    try:
        # Initialize Snowflake client
        writer = DefiSnowflakeWriter()
        
        # Create all views
        writer.create_analytical_views()
        
        logger.info("=" * 60)
        logger.info("✅ All analytical views created successfully!")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Available views:")
        logger.info("  1. VW_TOP_PROTOCOLS_24H - Top protocols ranked by TVL")
        logger.info("  2. VW_CHAIN_DOMINANCE - Chain market share & L1/L2 split")
        logger.info("  3. VW_CATEGORY_PERFORMANCE - Category-level metrics")
        logger.info("  4. VW_PROTOCOL_MOVERS - Biggest gainers/losers")
        logger.info("  5. VW_MARKET_SUMMARY - Overall market health metrics")
        logger.info("")
        logger.info("Usage example:")
        logger.info("  SELECT * FROM VW_TOP_PROTOCOLS_24H LIMIT 10;")
        logger.info("  SELECT * FROM VW_CHAIN_DOMINANCE;")
        logger.info("")
        
        # Close connection
        writer.close()
        
    except Exception as e:
        logger.error(f"❌ Error creating views: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
