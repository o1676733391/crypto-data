"""Test DefiLlama integration"""
import asyncio
import logging
from src.defillama_client import DefiLlamaClient
from src.defi_snowflake_client import DefiSnowflakeWriter
from src.defi_service import DefiIngestionService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_defillama_api():
    """Test DefiLlama API calls"""
    client = DefiLlamaClient()
    
    try:
        # Test 1: Get top protocols
        logger.info("\n=== TEST 1: Top 10 Protocols by TVL ===")
        protocols = await client.get_top_protocols(10)
        
        for i, protocol in enumerate(protocols, 1):
            logger.info(
                f"{i}. {protocol.get('name')} - "
                f"TVL: ${protocol.get('tvl', 0)/1e9:.2f}B - "
                f"Chain: {protocol.get('chain', 'Multi')} - "
                f"Category: {protocol.get('category', 'N/A')}"
            )
        
        # Test 2: Get chains TVL
        logger.info("\n=== TEST 2: Top Chains by TVL ===")
        chains = await client.get_chains_tvl()
        
        # Handle both list and dict formats
        if isinstance(chains, list):
            sorted_chains = sorted(
                chains,
                key=lambda x: x.get('tvl', 0) if isinstance(x, dict) else 0,
                reverse=True
            )[:10]
            # Convert to (name, data) tuples for consistency
            sorted_chains = [(c.get('name', 'Unknown'), c) for c in sorted_chains]
        else:
            sorted_chains = sorted(
                [(name, data) for name, data in chains.items() if isinstance(data, dict)],
                key=lambda x: x[1].get('tvl', 0),
                reverse=True
            )[:10]
        
        for i, (chain_name, chain_data) in enumerate(sorted_chains, 1):
            logger.info(
                f"{i}. {chain_name} - "
                f"TVL: ${chain_data.get('tvl', 0)/1e9:.2f}B"
            )
        
        # Test 3: Get specific protocol details
        logger.info("\n=== TEST 3: Uniswap Protocol Details ===")
        uniswap = await client.get_protocol_tvl("uniswap")
        
        if uniswap:
            logger.info(f"Name: {uniswap.get('name')}")
            logger.info(f"Symbol: {uniswap.get('symbol')}")
            
            # Handle TVL being list or dict with chainTvls
            tvl_value = uniswap.get('tvl', 0)
            if isinstance(tvl_value, list):
                # TVL is historical data, get latest
                if tvl_value:
                    latest_tvl = tvl_value[-1].get('totalLiquidityUSD', 0) if isinstance(tvl_value[-1], dict) else 0
                    logger.info(f"Current TVL: ${latest_tvl/1e9:.2f}B")
            else:
                logger.info(f"Current TVL: ${tvl_value/1e9:.2f}B")
                
            logger.info(f"Category: {uniswap.get('category')}")
            logger.info(f"Chains: {uniswap.get('chains', [])}")
            
            # Historical data points
            tvl_history = uniswap.get('tvl', [])
            if isinstance(tvl_history, list) and tvl_history:
                logger.info(f"Historical data points: {len(tvl_history)}")
        
        logger.info("\n✅ DefiLlama API tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)
    finally:
        await client.close()


async def test_snowflake_storage():
    """Test storing DeFi data in Snowflake"""
    logger.info("\n=== TEST 4: Snowflake DeFi Storage ===")
    
    client = DefiLlamaClient()
    writer = DefiSnowflakeWriter()
    
    try:
        # Fetch sample data
        protocols = await client.get_top_protocols(5)
        
        # Calculate market share
        total_tvl = sum(p.get('tvl', 0) for p in protocols)
        
        # Store protocols
        for protocol in protocols:
            tvl = protocol.get('tvl', 0)
            protocol['marketShare'] = (tvl / total_tvl * 100) if total_tvl > 0 else 0
            protocol['change_1d'] = protocol.get('change_1d', 0)
            protocol['change_7d'] = protocol.get('change_7d', 0)
            protocol['change_1m'] = protocol.get('change_1m', 0)
            
            await writer.insert_protocol_tvl(protocol)
            logger.info(f"Stored: {protocol.get('name')} - TVL ${tvl/1e9:.2f}B")
        
        logger.info("✅ Snowflake storage test completed!")
        
    except Exception as e:
        logger.error(f"❌ Storage test failed: {e}", exc_info=True)
    finally:
        await client.close()
        writer.close()


async def test_full_integration():
    """Test complete DeFi ingestion service"""
    logger.info("\n=== TEST 5: Full DeFi Ingestion Service ===")
    
    service = DefiIngestionService(top_n_protocols=20)
    
    try:
        # Manual fetch (don't start background loop)
        await service.manual_fetch()
        
        logger.info("✅ Full integration test completed!")
        
    except Exception as e:
        logger.error(f"❌ Integration test failed: {e}", exc_info=True)
    finally:
        await service.stop()


async def main():
    """Run all tests"""
    await test_defillama_api()
    await test_snowflake_storage()
    await test_full_integration()


if __name__ == "__main__":
    asyncio.run(main())
