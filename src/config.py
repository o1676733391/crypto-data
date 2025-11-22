from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

from dotenv import dotenv_values
from pydantic import BaseModel, Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[1]
DOTENV_PATH = BASE_DIR / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(DOTENV_PATH),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    supabase_url: HttpUrl = Field(alias="SUPABASE_URL")
    supabase_service_role_key: str = Field(alias="SUPABASE_SERVICE_ROLE_KEY")
    supabase_market_ticks_table: str = Field("market_ticks", alias="SUPABASE_MARKET_TICKS_TABLE")
    supabase_technicals_table: str = Field("technical_indicators", alias="SUPABASE_TECHNICALS_TABLE")

    snowflake_account: str = Field(alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(alias="SNOWFLAKE_USER")
    snowflake_password: str = Field(alias="SNOWFLAKE_PASSWORD")
    snowflake_role: str = Field("ACCOUNTADMIN", alias="SNOWFLAKE_ROLE")
    snowflake_warehouse: str = Field(alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field(alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field(alias="SNOWFLAKE_SCHEMA")

    fetch_interval_seconds: int = Field(60, alias="FETCH_INTERVAL_SECONDS")
    symbols: str = Field("BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT", alias="SYMBOLS")
    binance_rest_base: str = Field("https://api.binance.com", alias="BINANCE_REST_BASE")
    orderbook_depth: int = Field(5, alias="ORDERBOOK_DEPTH")
    http_timeout_seconds: int = Field(10, alias="HTTP_TIMEOUT_SECONDS")

    @property
    def symbol_list(self) -> List[str]:
        return [symbol.strip().upper() for symbol in self.symbols.split(",") if symbol.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
