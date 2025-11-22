from __future__ import annotations

import asyncio
import json
from typing import Any, Dict

import snowflake.connector

from .config import get_settings


CREATE_MARKET_TICKS = """
CREATE TABLE IF NOT EXISTS MARKET_TICKS (
    ID STRING DEFAULT UUID_STRING(),
    SYMBOL STRING,
    EXCHANGE STRING,
    WINDOW_INTERVAL STRING,
    EXCHANGE_TS TIMESTAMP_TZ,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    LAST_PRICE FLOAT,
    PRICE_CHANGE_PCT_1H FLOAT,
    PRICE_CHANGE_PCT_24H FLOAT,
    VOLUME_24H_QUOTE FLOAT,
    VOLUME_CHANGE_PCT_24H FLOAT,
    MARKET_CAP FLOAT,
    CIRCULATING_SUPPLY FLOAT,
    BID_ASK_SPREAD FLOAT,
    BID_DEPTH VARIANT,
    ASK_DEPTH VARIANT,
    INGESTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""

CREATE_TECHNICALS = """
CREATE TABLE IF NOT EXISTS TECHNICAL_INDICATORS (
    ID STRING DEFAULT UUID_STRING(),
    SYMBOL STRING,
    WINDOW_INTERVAL STRING,
    EXCHANGE_TS TIMESTAMP_TZ,
    ROLLING_RETURN_1H FLOAT,
    ROLLING_RETURN_24H FLOAT,
    ROLLING_VOLATILITY_24H FLOAT,
    HIGH_LOW_RANGE_24H FLOAT,
    MOVING_AVERAGE_7 FLOAT,
    MOVING_AVERAGE_30 FLOAT,
    RSI_14 FLOAT,
    MACD FLOAT,
    MACD_SIGNAL FLOAT,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""


class SnowflakeWriter:
    def __init__(self) -> None:
        settings = get_settings()
        self._settings = settings
        self._ctx = None
        # Remove lock for better concurrency - Snowflake handles concurrent writes

    def _get_connection(self):
        if self._ctx is None:
            try:
                self._ctx = snowflake.connector.connect(
                    account=self._settings.snowflake_account,
                    user=self._settings.snowflake_user,
                    password=self._settings.snowflake_password,
                    warehouse=self._settings.snowflake_warehouse,
                    database=self._settings.snowflake_database,
                    schema=self._settings.snowflake_schema,
                    role=self._settings.snowflake_role,
                )
                self._ensure_tables()
                self._connection_error = None
            except Exception as e:
                self._connection_error = str(e)
                raise
        return self._ctx

    def _ensure_tables(self) -> None:
        with self._get_connection().cursor() as cur:
            cur.execute(CREATE_MARKET_TICKS)
            cur.execute(CREATE_TECHNICALS)

    async def insert_market_tick(self, payload: Dict[str, Any]) -> None:
        sql = """
            INSERT INTO MARKET_TICKS (
                SYMBOL, EXCHANGE, WINDOW_INTERVAL, EXCHANGE_TS,
                OPEN, HIGH, LOW, CLOSE, LAST_PRICE,
                PRICE_CHANGE_PCT_1H, PRICE_CHANGE_PCT_24H,
                VOLUME_24H_QUOTE, VOLUME_CHANGE_PCT_24H,
                MARKET_CAP, CIRCULATING_SUPPLY, BID_ASK_SPREAD,
                BID_DEPTH, ASK_DEPTH, INGESTED_AT
            ) SELECT 
                %(symbol)s, %(exchange)s, %(window_interval)s, %(exchange_ts)s,
                %(open)s, %(high)s, %(low)s, %(close)s, %(last_price)s,
                %(price_change_pct_1h)s, %(price_change_pct_24h)s,
                %(volume_24h_quote)s, %(volume_change_pct_24h)s,
                %(market_cap)s, %(circulating_supply)s, %(bid_ask_spread)s,
                PARSE_JSON(%(bid_depth)s), PARSE_JSON(%(ask_depth)s), %(ingested_at)s
        """
        payload_copy = payload.copy()
        payload_copy["bid_depth"] = json.dumps(payload_copy.get("bid_depth"))
        payload_copy["ask_depth"] = json.dumps(payload_copy.get("ask_depth"))
        
        await asyncio.to_thread(self._execute, sql, payload_copy)

    async def insert_technicals(self, payload: Dict[str, Any]) -> None:
        sql = """
            INSERT INTO TECHNICAL_INDICATORS (
                SYMBOL, WINDOW_INTERVAL, EXCHANGE_TS,
                ROLLING_RETURN_1H, ROLLING_RETURN_24H,
                ROLLING_VOLATILITY_24H, HIGH_LOW_RANGE_24H,
                MOVING_AVERAGE_7, MOVING_AVERAGE_30,
                RSI_14, MACD, MACD_SIGNAL, CREATED_AT
            ) VALUES (
                %(symbol)s, %(window_interval)s, %(exchange_ts)s,
                %(rolling_return_1h)s, %(rolling_return_24h)s,
                %(rolling_volatility_24h)s, %(high_low_range_24h)s,
                %(moving_average_7)s, %(moving_average_30)s,
                %(rsi_14)s, %(macd)s, %(macd_signal)s, %(created_at)s
            )
        """
        await asyncio.to_thread(self._execute, sql, payload)

    async def insert_pair(self, market_payload: Dict[str, Any], technical_payload: Dict[str, Any]) -> None:
        await asyncio.gather(
            self.insert_market_tick(market_payload),
            self.insert_technicals(technical_payload),
        )

    def _execute(self, sql: str, params: Dict[str, Any]) -> None:
        with self._get_connection().cursor() as cur:
            cur.execute(sql, params)

    def close(self) -> None:
        if self._ctx:
            self._ctx.close()
