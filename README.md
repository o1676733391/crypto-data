# Crypto Data Ingestion Service

Python service that pulls minute-level market data for BTC, ETH, SOL, and BNB from Binance, computes basic technical indicators, and streams the results into both Supabase (for realtime dashboards) and Snowflake (for warehousing and analytics).