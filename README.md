# Crypto Data Ingestion Service

Python service that pulls minute-level market data for BTC, ETH, SOL, and BNB from Binance, computes basic technical indicators, and streams the results into both Supabase (for realtime dashboards) and Snowflake (for warehousing and analytics).

## Features

- FastAPI server with background task that fetches market snapshots on a schedule (default 60s).
- Collects ticker stats, order book depth, and recent klines from Binance REST API.
- Derives moving averages, RSI, MACD, short-term returns, volatility approximations.
- Writes structured records to Supabase tables to trigger Realtime subscriptions.
- Replicates the same payloads into Snowflake tables for downstream analytics.
- Exposes health endpoint and most recent snapshot per symbol.

## Prerequisites

- Python 3.11+
- Supabase project (URL + service role key) with tables `market_ticks` and `technical_indicators`.
- Snowflake account with warehouse, database, and schema ready for ingestion.
- Binance public REST access (no API key needed for the ticker/order-book endpoints used).

## Local Setup

1. Create and activate a virtual environment.
   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. Install dependencies.
   ```powershell
   pip install -r requirements.txt
   ```

3. Copy `.env.example` to `.env` and fill in credentials.
   ```powershell
   Copy-Item .env.example .env
   ```

   Required variables:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA`

   Optional overrides: polling interval, symbol list, Binance base URL, etc.

4. Ensure Supabase tables exist (adjust types as needed):

   ```sql
   create table if not exists market_ticks (
       id uuid default gen_random_uuid() primary key,
       symbol text,
       exchange text,
       window_interval text,
       exchange_ts timestamptz,
       open numeric,
       high numeric,
       low numeric,
       close numeric,
       last_price numeric,
       price_change_pct_1h numeric,
       price_change_pct_24h numeric,
       volume_24h_quote numeric,
       volume_change_pct_24h numeric,
       market_cap numeric,
        circulating_supply numeric,
       bid_ask_spread numeric,
       bid_depth jsonb,
       ask_depth jsonb,
       ingested_at timestamptz default now()
   );

   create table if not exists technical_indicators (
       id uuid default gen_random_uuid() primary key,
       symbol text,
       window_interval text,
       exchange_ts timestamptz,
       rolling_return_1h numeric,
       rolling_return_24h numeric,
       rolling_volatility_24h numeric,
       high_low_range_24h numeric,
       moving_average_7 numeric,
       moving_average_30 numeric,
       rsi_14 numeric,
       macd numeric,
       macd_signal numeric,
       created_at timestamptz default now()
   );
   ```

5. Snowflake tables are created automatically on first connection (`MARKET_TICKS`, `TECHNICAL_INDICATORS`). Confirm your user has privileges to create tables in the target schema.

6. Run the server.
   ```powershell
   uvicorn src.server:app --host 0.0.0.0 --port 8000
   ```

7. Monitor endpoints:
   - `GET /health`
   - `GET /latest/{symbol}`

## Operational Notes

- The ingestion loop enforces a minimum 10-second interval to stay within Binance rate limits; adjust `FETCH_INTERVAL_SECONDS` carefully.
- `market_cap`, `circulating_supply`, and `volume_change_pct_24h` fields are left `NULL` until integrated with a fundamentals source (e.g., CoinGecko). Extend `metrics.py` to enrich these values.
- Snowflake inserts use `PARSE_JSON` for depth ladders; cast to structured columns in downstream models.
- Consider enabling Supabase Row-Level Security with service-role inserts and read-only policies for analytics clients.
- For production, add retries/backoff, structured logging, and alerting around `_run_loop` exceptions.
