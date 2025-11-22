"""
Data aggregation module for building OHLCV candles from raw market ticks.
Transforms tick-level data into time-series candles for analytical queries.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import snowflake.connector
from src.config import get_settings

logger = logging.getLogger(__name__)


class SnowflakeAggregator:
    """Handles aggregation of raw market ticks into OHLCV candles at multiple timeframes."""
    
    def __init__(self):
        self._conn = None
        self._settings = get_settings()
    
    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Lazy connection to Snowflake."""
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                user=self._settings.snowflake_user,
                password=self._settings.snowflake_password,
                account=self._settings.snowflake_account,
                warehouse=self._settings.snowflake_warehouse,
                database=self._settings.snowflake_database,
                schema=self._settings.snowflake_schema,
            )
            logger.info("Connected to Snowflake for aggregation")
        return self._conn
    
    async def create_aggregation_tables(self):
        """Create candle aggregation tables if they don't exist."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Base candle table structure
        candle_ddl_template = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            candle_time TIMESTAMP_NTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            open FLOAT NOT NULL,
            high FLOAT NOT NULL,
            low FLOAT NOT NULL,
            close FLOAT NOT NULL,
            volume FLOAT NOT NULL,
            volume_quote FLOAT NOT NULL,
            trade_count INTEGER NOT NULL,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (symbol, candle_time)
        )
        """
        
        tables = [
            "CANDLES_1MIN",
            "CANDLES_5MIN", 
            "CANDLES_15MIN",
            "CANDLES_1HOUR",
            "CANDLES_4HOUR",
            "CANDLES_DAILY"
        ]
        
        for table in tables:
            try:
                cursor.execute(candle_ddl_template.format(table_name=table))
                logger.info(f"Created/verified table: {table}")
            except Exception as e:
                logger.error(f"Error creating table {table}: {e}")
        
        cursor.close()
        logger.info("All aggregation tables created/verified")
    
    async def create_analytical_views(self):
        """Create useful analytical views for common queries."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        views = {
            "VW_LATEST_PRICES": """
                CREATE OR REPLACE VIEW VW_LATEST_PRICES AS
                SELECT 
                    SYMBOL,
                    LAST_PRICE,
                    VOLUME_24H_QUOTE as VOLUME_24H,
                    PRICE_CHANGE_PCT_24H,
                    EXCHANGE_TS as TIMESTAMP,
                    DATEDIFF('second', EXCHANGE_TS, CURRENT_TIMESTAMP()) as SECONDS_AGO
                FROM MARKET_TICKS
                QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY EXCHANGE_TS DESC) = 1
            """,
            
            "VW_HOURLY_VOLATILITY": """
                CREATE OR REPLACE VIEW VW_HOURLY_VOLATILITY AS
                SELECT 
                    symbol,
                    DATE_TRUNC('hour', candle_time) as hour,
                    AVG((high - low) / low * 100) as avg_volatility_pct,
                    MAX(high) as period_high,
                    MIN(low) as period_low,
                    SUM(volume_quote) as total_volume_quote
                FROM CANDLES_1HOUR
                GROUP BY symbol, DATE_TRUNC('hour', candle_time)
            """,
            
            "VW_DAILY_SUMMARY": """
                CREATE OR REPLACE VIEW VW_DAILY_SUMMARY AS
                SELECT 
                    symbol,
                    DATE_TRUNC('day', candle_time) as trading_day,
                    FIRST_VALUE(open) OVER (PARTITION BY symbol, DATE_TRUNC('day', candle_time) ORDER BY candle_time) as day_open,
                    MAX(high) as day_high,
                    MIN(low) as day_low,
                    LAST_VALUE(close) OVER (PARTITION BY symbol, DATE_TRUNC('day', candle_time) ORDER BY candle_time 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as day_close,
                    SUM(volume) as day_volume,
                    SUM(volume_quote) as day_volume_quote,
                    SUM(trade_count) as day_trades
                FROM CANDLES_DAILY
                GROUP BY symbol, DATE_TRUNC('day', candle_time), open, close, candle_time
            """
        }
        
        for view_name, view_sql in views.items():
            try:
                cursor.execute(view_sql)
                logger.info(f"Created/updated view: {view_name}")
            except Exception as e:
                logger.error(f"Error creating view {view_name}: {e}")
        
        cursor.close()
        logger.info("All analytical views created/updated")
    
    async def aggregate_1min_candles(self, lookback_minutes: int = 5):
        """
        Aggregate raw ticks into 1-minute OHLCV candles.
        
        Args:
            lookback_minutes: How many minutes back to aggregate (default 5 for overlap)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        aggregate_sql = """
        MERGE INTO CANDLES_1MIN target
        USING (
            SELECT 
                candle_time,
                SYMBOL,
                MAX(CASE WHEN rn = 1 THEN LAST_PRICE END) as open,
                MAX(LAST_PRICE) as high,
                MIN(LAST_PRICE) as low,
                MAX(CASE WHEN rn_desc = 1 THEN LAST_PRICE END) as close,
                AVG(VOLUME_24H_QUOTE / 1440) as volume,
                AVG(VOLUME_24H_QUOTE / 1440) as volume_quote,
                COUNT(*) as trade_count
            FROM (
                SELECT 
                    DATE_TRUNC('minute', EXCHANGE_TS)::TIMESTAMP_NTZ as candle_time,
                    SYMBOL,
                    LAST_PRICE,
                    VOLUME_24H_QUOTE,
                    ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE_TRUNC('minute', EXCHANGE_TS) ORDER BY EXCHANGE_TS) as rn,
                    ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE_TRUNC('minute', EXCHANGE_TS) ORDER BY EXCHANGE_TS DESC) as rn_desc
                FROM MARKET_TICKS
                WHERE EXCHANGE_TS >= DATEADD('minute', -%s, CURRENT_TIMESTAMP())
            )
            GROUP BY SYMBOL, candle_time
        ) source
        ON target.symbol = source.symbol AND target.candle_time = source.candle_time
        WHEN MATCHED THEN UPDATE SET
            open = source.open,
            high = source.high,
            low = source.low,
            close = source.close,
            volume = source.volume,
            volume_quote = source.volume_quote,
            trade_count = source.trade_count
        WHEN NOT MATCHED THEN INSERT (
            candle_time, symbol, open, high, low, close, volume, volume_quote, trade_count
        ) VALUES (
            source.candle_time, source.symbol, source.open, source.high, source.low, 
            source.close, source.volume, source.volume_quote, source.trade_count
        )
        """
        
        try:
            cursor.execute(aggregate_sql, (lookback_minutes,))
            rows_affected = cursor.rowcount
            logger.info(f"Aggregated 1-min candles: {rows_affected} rows merged")
        except Exception as e:
            logger.error(f"Error aggregating 1-min candles: {e}")
        finally:
            cursor.close()
    
    async def aggregate_from_lower_timeframe(self, source_table: str, target_table: str, 
                                            interval: str, lookback_intervals: int = 10):
        """
        Aggregate candles from a lower timeframe to higher timeframe.
        
        Args:
            source_table: Source candle table (e.g., CANDLES_1MIN)
            target_table: Target candle table (e.g., CANDLES_5MIN)
            interval: Time interval for DATE_TRUNC (e.g., '5 minutes', 'hour', 'day')
            lookback_intervals: How many intervals to look back
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Map interval strings to proper Snowflake interval syntax
        # For multi-minute intervals, we need to use TIMESTAMPADD/FLOOR logic
        interval_map = {
            '5MIN': (5, 'minute'),
            '15MIN': (15, 'minute'),
            '1HOUR': (60, 'minute'),
            '4HOUR': (240, 'minute'),
            'DAILY': (1, 'day')
        }
        
        target_suffix = target_table.replace('CANDLES_', '')
        if target_suffix not in interval_map:
            logger.error(f"Unknown target table: {target_table}")
            return
        
        interval_minutes, unit = interval_map[target_suffix]
        
        # Build appropriate time bucket expression
        if unit == 'day':
            time_bucket = "DATE_TRUNC('day', candle_time)"
            lookback_expr = f"DATEADD('day', -{lookback_intervals}, CURRENT_TIMESTAMP())"
        elif interval_minutes == 60:
            time_bucket = "DATE_TRUNC('hour', candle_time)"
            lookback_expr = f"DATEADD('hour', -{lookback_intervals}, CURRENT_TIMESTAMP())"
        else:
            # For 5min, 15min, 4hour - use TIMEADD to round down
            time_bucket = f"TIMESTAMPADD('minute', FLOOR(DATEDIFF('minute', '2000-01-01', candle_time) / {interval_minutes}) * {interval_minutes}, '2000-01-01')"
            lookback_expr = f"DATEADD('minute', -{lookback_intervals * interval_minutes}, CURRENT_TIMESTAMP())"
        
        aggregate_sql = f"""
        MERGE INTO {target_table} target
        USING (
            SELECT 
                {time_bucket} as candle_time,
                symbol,
                MAX(CASE WHEN rn = 1 THEN open END) as open,
                MAX(high) as high,
                MIN(low) as low,
                MAX(CASE WHEN rn_desc = 1 THEN close END) as close,
                SUM(volume) as volume,
                SUM(volume_quote) as volume_quote,
                SUM(trade_count) as trade_count
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY symbol, {time_bucket} ORDER BY candle_time) as rn,
                    ROW_NUMBER() OVER (PARTITION BY symbol, {time_bucket} ORDER BY candle_time DESC) as rn_desc
                FROM {source_table}
                WHERE candle_time >= {lookback_expr}
            )
            GROUP BY symbol, {time_bucket}
        ) source
        ON target.symbol = source.symbol AND target.candle_time = source.candle_time
        WHEN MATCHED THEN UPDATE SET
            open = source.open,
            high = source.high,
            low = source.low,
            close = source.close,
            volume = source.volume,
            volume_quote = source.volume_quote,
            trade_count = source.trade_count
        WHEN NOT MATCHED THEN INSERT (
            candle_time, symbol, open, high, low, close, volume, volume_quote, trade_count
        ) VALUES (
            source.candle_time, source.symbol, source.open, source.high, source.low, 
            source.close, source.volume, source.volume_quote, source.trade_count
        )
        """
        
        try:
            cursor.execute(aggregate_sql)
            rows_affected = cursor.rowcount
            logger.info(f"Aggregated {source_table} -> {target_table}: {rows_affected} rows merged")
        except Exception as e:
            logger.error(f"Error aggregating {target_table}: {e}")
        finally:
            cursor.close()
    
    async def run_full_aggregation_pipeline(self):
        """Run complete aggregation pipeline from raw ticks to all timeframes."""
        logger.info("Starting full aggregation pipeline...")
        
        # Step 1: Raw ticks -> 1min candles
        await self.aggregate_1min_candles(lookback_minutes=5)
        
        # Step 2: 1min -> 5min candles
        await self.aggregate_from_lower_timeframe(
            'CANDLES_1MIN', 'CANDLES_5MIN', '5MIN', lookback_intervals=12
        )
        
        # Step 3: 5min -> 15min candles
        await self.aggregate_from_lower_timeframe(
            'CANDLES_5MIN', 'CANDLES_15MIN', '15MIN', lookback_intervals=8
        )
        
        # Step 4: 15min -> 1hour candles
        await self.aggregate_from_lower_timeframe(
            'CANDLES_15MIN', 'CANDLES_1HOUR', '1HOUR', lookback_intervals=8
        )
        
        # Step 5: 1hour -> 4hour candles
        await self.aggregate_from_lower_timeframe(
            'CANDLES_1HOUR', 'CANDLES_4HOUR', '4HOUR', lookback_intervals=6
        )
        
        # Step 6: 4hour -> daily candles
        await self.aggregate_from_lower_timeframe(
            'CANDLES_4HOUR', 'CANDLES_DAILY', 'DAILY', lookback_intervals=7
        )
        
        logger.info("Full aggregation pipeline completed")
    
    def close(self):
        """Close Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Snowflake aggregator connection closed")
