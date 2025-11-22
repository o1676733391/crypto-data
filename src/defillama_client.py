"""
DefiLlama API client for DeFi protocol data
100% Free - No API key required
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import httpx

logger = logging.getLogger(__name__)


class DefiLlamaClient:
    """Client for DefiLlama API - Free DeFi data aggregator"""
    
    BASE_URL = "https://api.llama.fi"
    COINS_URL = "https://coins.llama.fi"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                http2=True,
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
            )
        return self._client
    
    async def close(self):
        """Close HTTP client"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def get_all_protocols(self) -> List[Dict[str, Any]]:
        """
        Get list of all DeFi protocols with current TVL.
        
        Returns:
            List of protocols with: name, symbol, chain, tvl, change_1d, change_7d, etc.
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/protocols")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data)} protocols from DefiLlama")
            return data
        except Exception as e:
            logger.error(f"Error fetching protocols: {e}")
            return []
    
    async def get_protocol_tvl(self, protocol_slug: str) -> Dict[str, Any]:
        """
        Get detailed TVL history for a specific protocol.
        
        Args:
            protocol_slug: Protocol identifier (e.g., "uniswap", "aave", "curve")
        
        Returns:
            Protocol details with historical TVL data
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/protocol/{protocol_slug}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched TVL history for {protocol_slug}")
            return data
        except Exception as e:
            logger.error(f"Error fetching protocol {protocol_slug}: {e}")
            return {}
    
    async def get_chains_tvl(self) -> Dict[str, Any]:
        """
        Get current TVL for all blockchain chains.
        
        Returns:
            Dictionary of chains with current TVL
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/v2/chains")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched TVL for {len(data)} chains")
            return data
        except Exception as e:
            logger.error(f"Error fetching chains: {e}")
            return {}
    
    async def get_chain_tvl_history(self, chain: str) -> List[Dict[str, Any]]:
        """
        Get historical TVL for a specific chain.
        
        Args:
            chain: Chain name (e.g., "Ethereum", "BSC", "Polygon")
        
        Returns:
            List of TVL data points with timestamp and tvl
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/v2/historicalChainTvl/{chain}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched TVL history for {chain}")
            return data
        except Exception as e:
            logger.error(f"Error fetching chain {chain} history: {e}")
            return []
    
    async def get_stablecoins(self) -> Dict[str, Any]:
        """
        Get stablecoin market data including circulating amounts.
        
        Returns:
            Stablecoin data with chains, peggedUSD, etc.
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/stablecoins?includePrices=true")
            response.raise_for_status()
            data = response.json()
            logger.info("Fetched stablecoin data")
            return data
        except Exception as e:
            logger.error(f"Error fetching stablecoins: {e}")
            return {}
    
    async def get_token_prices(self, token_addresses: List[str]) -> Dict[str, Any]:
        """
        Get current prices for multiple tokens.
        
        Args:
            token_addresses: List of token addresses with chain prefix
                            Format: "ethereum:0x..." or "bsc:0x..."
        
        Returns:
            Dictionary mapping addresses to price data
        """
        client = await self._get_client()
        
        if not token_addresses:
            return {}
        
        # Join addresses with comma
        addresses_str = ",".join(token_addresses)
        
        try:
            response = await client.get(
                f"{self.COINS_URL}/prices/current/{addresses_str}"
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched prices for {len(token_addresses)} tokens")
            return data.get("coins", {})
        except Exception as e:
            logger.error(f"Error fetching token prices: {e}")
            return {}
    
    async def get_protocol_treasury(self, protocol: str) -> Dict[str, Any]:
        """
        Get treasury/revenue data for a protocol.
        
        Args:
            protocol: Protocol identifier
        
        Returns:
            Treasury data
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/treasury/{protocol}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched treasury for {protocol}")
            return data
        except Exception as e:
            logger.error(f"Error fetching treasury for {protocol}: {e}")
            return {}
    
    async def get_protocol_fees(self, protocol: str) -> Dict[str, Any]:
        """
        Get fees/revenue data for a protocol.
        
        Args:
            protocol: Protocol identifier
        
        Returns:
            Fees data
        """
        client = await self._get_client()
        
        try:
            response = await client.get(f"{self.BASE_URL}/summary/fees/{protocol}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched fees for {protocol}")
            return data
        except Exception as e:
            logger.error(f"Error fetching fees for {protocol}: {e}")
            return {}
    
    async def get_top_protocols(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get top N protocols by TVL (convenience method).
        
        Args:
            limit: Number of protocols to return
        
        Returns:
            List of top protocols sorted by TVL
        """
        protocols = await self.get_all_protocols()
        
        # Sort by TVL descending
        protocols_sorted = sorted(
            protocols,
            key=lambda x: x.get("tvl") or 0,  # Handle None values
            reverse=True
        )
        
        return protocols_sorted[:limit]
    
    async def get_protocol_snapshot(self, protocol_slug: str) -> Dict[str, Any]:
        """
        Get comprehensive snapshot of a protocol (convenience method).
        
        Combines: basic info, TVL history, fees, treasury
        
        Args:
            protocol_slug: Protocol identifier
        
        Returns:
            Combined protocol data
        """
        import asyncio
        
        # Fetch all data in parallel
        results = await asyncio.gather(
            self.get_protocol_tvl(protocol_slug),
            self.get_protocol_fees(protocol_slug),
            self.get_protocol_treasury(protocol_slug),
            return_exceptions=True
        )
        
        tvl_data, fees_data, treasury_data = results
        
        # Handle exceptions
        tvl_data = tvl_data if not isinstance(tvl_data, Exception) else {}
        fees_data = fees_data if not isinstance(fees_data, Exception) else {}
        treasury_data = treasury_data if not isinstance(treasury_data, Exception) else {}
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "protocol": protocol_slug,
            "tvl": tvl_data,
            "fees": fees_data,
            "treasury": treasury_data
        }
