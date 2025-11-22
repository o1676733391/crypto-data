from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from .config import get_settings
from .fetcher import fetch_market_data
from .metrics import build_payload
from .snowflake_client import SnowflakeWriter
from .supabase_client import SupabaseWriter
from .aggregator import SnowflakeAggregator


class IngestionService:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._interval = max(10, self._settings.fetch_interval_seconds)
        self._symbols = self._settings.symbol_list
        self._supabase = SupabaseWriter()
        self._snowflake = SnowflakeWriter()
        self._aggregator = SnowflakeAggregator()
        self._task: Optional[asyncio.Task[None]] = None
        self._aggregation_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._latest_payloads: dict[str, dict] = {}
        self._last_fetch_time: Optional[float] = None
        self._fetch_latencies: List[float] = []  # Track last 100 fetch times

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    def latest_payload(self, symbol: str) -> Optional[dict]:
        return self._latest_payloads.get(symbol.upper())
    
    def get_stats(self) -> dict:
        """Get performance statistics for monitoring"""
        avg_latency = sum(self._fetch_latencies) / len(self._fetch_latencies) if self._fetch_latencies else 0
        return {
            "symbols": self._symbols,
            "fetch_interval_seconds": self._interval,
            "last_fetch_time": self._last_fetch_time,
            "avg_fetch_latency_ms": round(avg_latency * 1000, 2),
            "recent_latencies_ms": [round(l * 1000, 2) for l in self._fetch_latencies[-10:]],
            "tracked_symbols_count": len(self._latest_payloads),
        }

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        
        # Initialize aggregation tables and views
        logging.info("Initializing Snowflake aggregation tables...")
        await self._aggregator.create_aggregation_tables()
        await self._aggregator.create_analytical_views()
        
        # Start main ingestion loop
        self._task = asyncio.create_task(self._run_loop())
        
        # Start background aggregation loop (runs every 1 minute)
        self._aggregation_task = asyncio.create_task(self._aggregation_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task
        if self._aggregation_task:
            await self._aggregation_task
        self._snowflake.close()
        self._aggregator.close()

    async def _run_loop(self) -> None:
        logging.info("Starting ingestion loop for symbols: %s", ",".join(self._symbols))
        while not self._stop_event.is_set():
            started = time.perf_counter()
            try:
                snapshots = await fetch_market_data(self._symbols)
                await self._process_snapshots(snapshots)
                
                # Track latency
                elapsed = time.perf_counter() - started
                self._last_fetch_time = time.time()
                self._fetch_latencies.append(elapsed)
                if len(self._fetch_latencies) > 100:
                    self._fetch_latencies.pop(0)
                
                logging.info("Fetch completed in %.2fms", elapsed * 1000)
            except Exception as exc:  # noqa: BLE001
                logging.exception("Ingestion loop error: %s", exc)
            elapsed = time.perf_counter() - started
            remaining = self._interval - elapsed
            if remaining > 0:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    continue

    async def _process_snapshots(self, snapshots: List[dict]) -> None:
        tasks = []
        for snapshot in snapshots:
            payload = build_payload(snapshot)
            self._latest_payloads[snapshot["symbol"].upper()] = payload.market_tick
            tasks.append(
                asyncio.gather(
                    self._supabase.insert_pair(
                        payload.market_tick, payload.technicals
                    ),
                    self._snowflake.insert_pair(
                        payload.market_tick, payload.technicals
                    ),
                    return_exceptions=True
                )
            )
        results = await asyncio.gather(*tasks)
        
        # Log any errors but continue processing
        for idx, result in enumerate(results):
            for db_idx, db_result in enumerate(result):
                if isinstance(db_result, Exception):
                    db_name = ["Supabase", "Snowflake"][db_idx]
                    logging.error("Failed to insert to %s: %s", db_name, str(db_result))
        
        logging.info("Processed %d snapshots", len(snapshots))
    
    async def _aggregation_loop(self) -> None:
        """Background task that runs aggregation pipeline periodically."""
        logging.info("Starting aggregation loop (runs every 60 seconds)")
        
        # Wait 2 minutes before first aggregation to ensure we have some raw data
        await asyncio.sleep(120)
        
        while not self._stop_event.is_set():
            try:
                logging.info("Running aggregation pipeline...")
                await self._aggregator.run_full_aggregation_pipeline()
                logging.info("Aggregation pipeline completed successfully")
            except Exception as exc:
                logging.exception("Aggregation loop error: %s", exc)
            
            # Wait 60 seconds before next aggregation
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                continue

