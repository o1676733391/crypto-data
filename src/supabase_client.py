from __future__ import annotations

import asyncio
from typing import Any, Dict

from supabase import Client, create_client

from .config import get_settings


class SupabaseWriter:
    def __init__(self) -> None:
        settings = get_settings()
        self._client: Client = create_client(
            str(settings.supabase_url), settings.supabase_service_role_key
        )
        self._market_table = settings.supabase_market_ticks_table
        self._technicals_table = settings.supabase_technicals_table

    def _insert(self, table: str, payload: Dict[str, Any]) -> None:
        self._client.table(table).insert(payload).execute()

    async def insert_market_tick(self, payload: Dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert, self._market_table, payload)

    async def insert_technicals(self, payload: Dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert, self._technicals_table, payload)

    async def insert_pair(self, market_payload: Dict[str, Any], technical_payload: Dict[str, Any]) -> None:
        await asyncio.gather(
            self.insert_market_tick(market_payload),
            self.insert_technicals(technical_payload),
        )
