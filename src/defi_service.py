"""
DeFi data ingestion service
Fetches TVL, protocol data from DefiLlama and stores in Snowflake
"""
import logging
import asyncio
from typing import List, Dict, Any, Union
from datetime import datetime

from .defillama_client import DefiLlamaClient
from .defi_snowflake_client import DefiSnowflakeWriter

logger = logging.getLogger(__name__)


class DefiIngestionService:
    """Service to ingest DeFi protocol and chain data"""
    
    def __init__(self, top_n_protocols: int = 100):
        self.defillama = DefiLlamaClient()
        self.snowflake = DefiSnowflakeWriter()
        self.top_n = top_n_protocols
        self._stop_event = asyncio.Event()
        self._task = None
    
    async def start(self, interval_minutes: int = 60):
        """
        Start DeFi data ingestion loop.
        
        Args:
            interval_minutes: How often to fetch data (default: 60 minutes for free tier)
        """
        if self._task and not self._task.done():
            logger.warning("DeFi ingestion service already running")
            return
        
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(interval_minutes))
        logger.info(f"DeFi ingestion service started (interval: {interval_minutes}min)")
    
    async def stop(self):
        """Stop DeFi ingestion service"""
        self._stop_event.set()
        if self._task:
            await self._task
        await self.defillama.close()
        self.snowflake.close()
        logger.info("DeFi ingestion service stopped")
    
    async def _run_loop(self, interval_minutes: int):
        """Main ingestion loop"""
        # Initial delay to let main service start first
        await asyncio.sleep(10)
        
        while not self._stop_event.is_set():
            try:
                logger.info("Starting DeFi data fetch cycle...")
                await self.fetch_and_store_all()
                logger.info("DeFi data fetch cycle completed")
            except Exception as e:
                logger.error(f"DeFi ingestion error: {e}", exc_info=True)
            
            # Wait for next cycle
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=interval_minutes * 60
                )
            except asyncio.TimeoutError:
                continue
    
    async def fetch_and_store_all(self):
        """Fetch all DeFi data and store in Snowflake"""
        # Fetch protocols and chains in parallel
        protocols_task = self.defillama.get_top_protocols(self.top_n)
        chains_task = self.defillama.get_chains_tvl()
        
        protocols, chains_data = await asyncio.gather(
            protocols_task,
            chains_task,
            return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(protocols, Exception):
            logger.error(f"Failed to fetch protocols: {protocols}")
            protocols = []
        
        if isinstance(chains_data, Exception):
            logger.error(f"Failed to fetch chains: {chains_data}")
            chains_data = {}
        
        # Process and store protocols
        if protocols:
            await self._store_protocols(protocols)
        
        # Process and store chains
        if chains_data:
            await self._store_chains(chains_data)
    
    async def _store_protocols(self, protocols: List[Dict[str, Any]]):
        """Process and store protocol data"""
        logger.info(f"Processing {len(protocols)} protocols...")
        
        # Calculate total TVL for market share
        total_tvl = sum(p.get('tvl', 0) for p in protocols)
        
        # Enrich protocols with calculated fields
        for protocol in protocols:
            tvl = protocol.get('tvl', 0)
            protocol['marketShare'] = (tvl / total_tvl * 100) if total_tvl > 0 else 0
            
            # Calculate changes if data available
            tvl_prev_day = protocol.get('tvlPrevDay', tvl)
            tvl_prev_week = protocol.get('tvlPrevWeek', tvl)
            tvl_prev_month = protocol.get('tvlPrevMonth', tvl)
            
            protocol['change_1d'] = ((tvl - tvl_prev_day) / tvl_prev_day * 100) if tvl_prev_day else 0
            protocol['change_7d'] = ((tvl - tvl_prev_week) / tvl_prev_week * 100) if tvl_prev_week else 0
            protocol['change_1m'] = ((tvl - tvl_prev_month) / tvl_prev_month * 100) if tvl_prev_month else 0
        
        # Store in Snowflake
        await self.snowflake.bulk_insert_protocols(protocols)
        logger.info(f"Stored {len(protocols)} protocols in Snowflake")
    
    async def _store_chains(self, chains_data: Any):
        """Process and store chain data"""
        chains = []
        
        # Handle both list and dict formats
        if isinstance(chains_data, list):
            # New API format returns a list
            for chain_info in chains_data:
                if isinstance(chain_info, dict):
                    chain_entry = {
                        'name': chain_info.get('name'),
                        'tvl': chain_info.get('tvl', 0),
                        'tvlPrevDay': chain_info.get('tvlPrevDay'),
                        'tvlPrevWeek': chain_info.get('tvlPrevWeek'),
                        'tokenSymbol': chain_info.get('tokenSymbol'),
                        'cmcId': chain_info.get('cmcId'),
                        'gecko_id': chain_info.get('gecko_id')
                    }
                    
                    # Calculate changes
                    tvl = chain_entry['tvl']
                    tvl_prev_day = chain_entry.get('tvlPrevDay', tvl)
                    tvl_prev_week = chain_entry.get('tvlPrevWeek', tvl)
                    
                    chain_entry['change_1d'] = ((tvl - tvl_prev_day) / tvl_prev_day * 100) if tvl_prev_day else 0
                    chain_entry['change_7d'] = ((tvl - tvl_prev_week) / tvl_prev_week * 100) if tvl_prev_week else 0
                    
                    chains.append(chain_entry)
        elif isinstance(chains_data, dict):
            # Old API format returns a dict
            for chain_name, chain_info in chains_data.items():
                if isinstance(chain_info, dict):
                    chain_entry = {
                        'name': chain_name,
                        'tvl': chain_info.get('tvl', 0),
                        'tvlPrevDay': chain_info.get('tvlPrevDay'),
                        'tvlPrevWeek': chain_info.get('tvlPrevWeek'),
                        'tokenSymbol': chain_info.get('tokenSymbol'),
                        'cmcId': chain_info.get('cmcId'),
                        'gecko_id': chain_info.get('gecko_id')
                    }
                    
                    # Calculate changes
                    tvl = chain_entry['tvl']
                    tvl_prev_day = chain_entry.get('tvlPrevDay', tvl)
                    tvl_prev_week = chain_entry.get('tvlPrevWeek', tvl)
                    
                    chain_entry['change_1d'] = ((tvl - tvl_prev_day) / tvl_prev_day * 100) if tvl_prev_day else 0
                    chain_entry['change_7d'] = ((tvl - tvl_prev_week) / tvl_prev_week * 100) if tvl_prev_week else 0
                    
                    chains.append(chain_entry)
        
        if chains:
            await self.snowflake.bulk_insert_chains(chains)
            logger.info(f"Stored {len(chains)} chains in Snowflake")
    
    async def get_protocol_snapshot(self, protocol_slug: str) -> Dict[str, Any]:
        """Get comprehensive protocol snapshot (on-demand)"""
        return await self.defillama.get_protocol_snapshot(protocol_slug)
    
    async def manual_fetch(self):
        """Manually trigger a fetch cycle (for testing)"""
        logger.info("Manual DeFi fetch triggered")
        await self.fetch_and_store_all()
