from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Tuple

import httpx

from .config import get_settings


class BinanceDataSource:
    """Thin async wrapper around Binance REST endpoints used for market stats."""

    def __init__(self, *, timeout: float | None = None) -> None:
        settings = get_settings()
        self._base_url = settings.binance_rest_base.rstrip("/")
        self._timeout = timeout or settings.http_timeout_seconds
        self._client = httpx.AsyncClient(
            base_url=self._base_url, 
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            http2=True  # Enable HTTP/2 for better performance
        )
        self._orderbook_depth = settings.orderbook_depth

    async def close(self) -> None:
        await self._client.aclose()

    async def get_ticker_24hr(self, symbols: List[str]) -> Dict[str, Any]:
        encoded = json.dumps(symbols)
        response = await self._client.get("/api/v3/ticker/24hr", params={"symbols": encoded})
        response.raise_for_status()
        payload = response.json()
        return {entry["symbol"].upper(): entry for entry in payload}

    async def get_order_book(self, symbol: str) -> Dict[str, Any]:
        response = await self._client.get(
            "/api/v3/depth",
            params={"symbol": symbol.upper(), "limit": self._orderbook_depth},
        )
        response.raise_for_status()
        return response.json()

    async def get_recent_klines(
        self,
        symbol: str,
        *,
        interval: str = "1m",
        limit: int = 120,
    ) -> List[List[Any]]:
        response = await self._client.get(
            "/api/v3/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )
        response.raise_for_status()
        return response.json()

    async def fetch_symbol_snapshot(self, symbol: str) -> Dict[str, Any]:
        # Fetch all data in parallel for low latency
        ticker_task = asyncio.create_task(self.get_ticker_single(symbol))
        orderbook_task = asyncio.create_task(self.get_order_book(symbol))
        klines_task = asyncio.create_task(self.get_recent_klines(symbol))
        
        ticker, order, klines = await asyncio.gather(
            ticker_task, orderbook_task, klines_task
        )
        return {
            "symbol": symbol.upper(),
            "ticker": ticker,
            "orderbook": order,
            "klines": klines,
        }

    async def get_ticker_single(self, symbol: str) -> Dict[str, Any]:
        response = await self._client.get(
            "/api/v3/ticker/24hr", params={"symbol": symbol.upper()}
        )
        response.raise_for_status()
        return response.json()

    async def fetch_all_snapshots(self, symbols: List[str]) -> List[Dict[str, Any]]:
        # Fetch all symbols in parallel with slight stagger to avoid overwhelming API
        async def fetch_with_stagger(symbol: str, delay: float) -> Dict[str, Any]:
            await asyncio.sleep(delay)
            return await self.fetch_symbol_snapshot(symbol)
        
        # Stagger requests by 100ms each
        tasks = [fetch_with_stagger(symbol, idx * 0.1) for idx, symbol in enumerate(symbols)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log errors
        snapshots = []
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Error fetching {symbols[idx]}: {result}")
            else:
                snapshots.append(result)
        return snapshots


async def fetch_market_data(symbols: List[str]) -> List[Dict[str, Any]]:
    settings = get_settings()
    client = BinanceDataSource(timeout=settings.http_timeout_seconds)
    try:
        return await client.fetch_all_snapshots(symbols)
    finally:
        await client.close()
