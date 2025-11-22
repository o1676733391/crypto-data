"""
Snowflake client for DeFi data storage
Handles protocol TVL, chain TVL, and DeFi-specific metrics
"""
from __future__ import annotations

import logging
import json
from typing import Dict, Any, List
import snowflake.connector

from .config import get_settings

logger = logging.getLogger(__name__)

# DeFi-specific table schemas
CREATE_PROTOCOL_TVL = """
CREATE TABLE IF NOT EXISTS PROTOCOL_TVL (
    ID STRING DEFAULT UUID_STRING(),
    PROTOCOL_NAME STRING NOT NULL,
    PROTOCOL_SLUG STRING NOT NULL,
    CHAIN STRING,
    CATEGORY STRING,
    TVL FLOAT NOT NULL,
    TVL_PREV_DAY FLOAT,
    TVL_PREV_WEEK FLOAT,
    TVL_PREV_MONTH FLOAT,
    CHANGE_1D FLOAT,
    CHANGE_7D FLOAT,
    CHANGE_1M FLOAT,
    MARKET_SHARE_PCT FLOAT,
    SYMBOL STRING,
    LOGO STRING,
    CHAINS STRING,
    TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PROTOCOL_SLUG, TIMESTAMP)
);
"""

CREATE_CHAIN_TVL = """
CREATE TABLE IF NOT EXISTS CHAIN_TVL (
    ID STRING DEFAULT UUID_STRING(),
    CHAIN_NAME STRING NOT NULL,
    TVL FLOAT NOT NULL,
    TVL_PREV_DAY FLOAT,
    TVL_PREV_WEEK FLOAT,
    CHANGE_1D FLOAT,
    CHANGE_7D FLOAT,
    TOKEN_SYMBOL STRING,
    CMCID STRING,
    GECKO_ID STRING,
    TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CHAIN_NAME, TIMESTAMP)
);
"""

CREATE_PROTOCOL_HISTORY = """
CREATE TABLE IF NOT EXISTS PROTOCOL_TVL_HISTORY (
    ID STRING DEFAULT UUID_STRING(),
    PROTOCOL_SLUG STRING NOT NULL,
    TVL FLOAT NOT NULL,
    DATE DATE NOT NULL,
    TIMESTAMP TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (PROTOCOL_SLUG, DATE)
);
"""

CREATE_DEFI_STABLECOINS = """
CREATE TABLE IF NOT EXISTS DEFI_STABLECOINS (
    ID STRING DEFAULT UUID_STRING(),
    STABLECOIN_NAME STRING NOT NULL,
    SYMBOL STRING,
    GECKO_ID STRING,
    CIRCULATING FLOAT NOT NULL,
    PRICE FLOAT,
    CHAINS VARIANT,
    PEG_MECHANISM STRING,
    TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SYMBOL, TIMESTAMP)
);
"""

# Analytical views for optimized queries
CREATE_VIEW_TOP_PROTOCOLS = """
CREATE OR REPLACE VIEW VW_TOP_PROTOCOLS_24H AS
SELECT 
    PROTOCOL_NAME,
    PROTOCOL_SLUG,
    CATEGORY,
    CHAIN,
    TVL,
    CHANGE_1D,
    CHANGE_7D,
    CHANGE_1M,
    MARKET_SHARE_PCT,
    SYMBOL,
    LOGO,
    RANK() OVER (ORDER BY TVL DESC) as TVL_RANK,
    TIMESTAMP
FROM PROTOCOL_TVL
WHERE TIMESTAMP >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND TVL IS NOT NULL
ORDER BY TVL DESC;
"""

CREATE_VIEW_CHAIN_DOMINANCE = """
CREATE OR REPLACE VIEW VW_CHAIN_DOMINANCE AS
WITH latest_snapshot AS (
    SELECT MAX(TIMESTAMP) as max_ts FROM CHAIN_TVL
),
chain_totals AS (
    SELECT 
        CHAIN_NAME,
        TVL,
        CHANGE_1D,
        CHANGE_7D,
        TOKEN_SYMBOL,
        TIMESTAMP
    FROM CHAIN_TVL
    WHERE TIMESTAMP = (SELECT max_ts FROM latest_snapshot)
      AND TVL IS NOT NULL
)
SELECT 
    CHAIN_NAME,
    TVL,
    CHANGE_1D,
    CHANGE_7D,
    TOKEN_SYMBOL,
    TVL / SUM(TVL) OVER () * 100 as DOMINANCE_PCT,
    RANK() OVER (ORDER BY TVL DESC) as TVL_RANK,
    CASE 
        WHEN CHAIN_NAME IN ('Arbitrum', 'Optimism', 'Base', 'Polygon', 'zkSync Era', 'Starknet') 
        THEN 'Layer 2'
        ELSE 'Layer 1'
    END as LAYER_TYPE,
    TIMESTAMP
FROM chain_totals
ORDER BY TVL DESC;
"""

CREATE_VIEW_CATEGORY_PERFORMANCE = """
CREATE OR REPLACE VIEW VW_CATEGORY_PERFORMANCE AS
WITH recent_data AS (
    SELECT *
    FROM PROTOCOL_TVL
    WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
      AND CATEGORY IS NOT NULL
      AND TVL IS NOT NULL
)
SELECT 
    CATEGORY,
    COUNT(DISTINCT PROTOCOL_SLUG) as PROTOCOL_COUNT,
    SUM(TVL) as TOTAL_TVL,
    AVG(TVL) as AVG_TVL,
    AVG(CHANGE_1D) as AVG_CHANGE_1D,
    AVG(CHANGE_7D) as AVG_CHANGE_7D,
    AVG(CHANGE_1M) as AVG_CHANGE_1M,
    SUM(TVL) / (SELECT SUM(TVL) FROM recent_data) * 100 as MARKET_SHARE_PCT,
    MAX(TVL) as MAX_PROTOCOL_TVL,
    MIN(TVL) as MIN_PROTOCOL_TVL
FROM recent_data
GROUP BY CATEGORY
ORDER BY TOTAL_TVL DESC;
"""

CREATE_VIEW_PROTOCOL_MOVERS = """
CREATE OR REPLACE VIEW VW_PROTOCOL_MOVERS AS
WITH latest_data AS (
    SELECT *
    FROM PROTOCOL_TVL
    WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
      AND TVL IS NOT NULL
      AND CHANGE_7D IS NOT NULL
),
ranked_changes AS (
    SELECT 
        PROTOCOL_NAME,
        PROTOCOL_SLUG,
        CATEGORY,
        CHAIN,
        TVL,
        CHANGE_1D,
        CHANGE_7D,
        CHANGE_1M,
        MARKET_SHARE_PCT,
        ABS(CHANGE_7D) as ABS_CHANGE_7D,
        CASE 
            WHEN CHANGE_7D > 20 THEN 'Hot'
            WHEN CHANGE_7D > 5 THEN 'Rising'
            WHEN CHANGE_7D > -5 THEN 'Stable'
            WHEN CHANGE_7D > -20 THEN 'Cooling'
            ELSE 'Declining'
        END as TREND,
        RANK() OVER (ORDER BY CHANGE_7D DESC) as GAINER_RANK,
        RANK() OVER (ORDER BY CHANGE_7D ASC) as LOSER_RANK,
        TIMESTAMP
    FROM latest_data
)
SELECT *
FROM ranked_changes
ORDER BY ABS_CHANGE_7D DESC;
"""

CREATE_VIEW_MARKET_SUMMARY = """
CREATE OR REPLACE VIEW VW_MARKET_SUMMARY AS
WITH recent_protocols AS (
    SELECT *
    FROM PROTOCOL_TVL
    WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
      AND TVL IS NOT NULL
),
recent_chains AS (
    SELECT *
    FROM CHAIN_TVL
    WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
      AND TVL IS NOT NULL
)
SELECT 
    -- Protocol metrics
    (SELECT SUM(TVL) FROM recent_protocols) as TOTAL_TVL,
    (SELECT COUNT(DISTINCT PROTOCOL_SLUG) FROM recent_protocols) as TOTAL_PROTOCOLS,
    (SELECT COUNT(DISTINCT CATEGORY) FROM recent_protocols WHERE CATEGORY IS NOT NULL) as TOTAL_CATEGORIES,
    (SELECT AVG(CHANGE_7D) FROM recent_protocols WHERE CHANGE_7D IS NOT NULL) as AVG_PROTOCOL_CHANGE_7D,
    
    -- Chain metrics
    (SELECT COUNT(DISTINCT CHAIN_NAME) FROM recent_chains) as TOTAL_CHAINS,
    (SELECT AVG(CHANGE_7D) FROM recent_chains WHERE CHANGE_7D IS NOT NULL) as AVG_CHAIN_CHANGE_7D,
    
    -- Market sentiment
    CASE 
        WHEN (SELECT AVG(CHANGE_7D) FROM recent_protocols WHERE CHANGE_7D IS NOT NULL) > 5 THEN 'Very Bullish'
        WHEN (SELECT AVG(CHANGE_7D) FROM recent_protocols WHERE CHANGE_7D IS NOT NULL) > 0 THEN 'Bullish'
        WHEN (SELECT AVG(CHANGE_7D) FROM recent_protocols WHERE CHANGE_7D IS NOT NULL) > -5 THEN 'Bearish'
        ELSE 'Very Bearish'
    END as MARKET_SENTIMENT,
    
    -- Diversity score (categories with > $1B TVL)
    (SELECT COUNT(DISTINCT CATEGORY) 
     FROM recent_protocols 
     WHERE CATEGORY IS NOT NULL 
     GROUP BY CATEGORY 
     HAVING SUM(TVL) > 1000000000) as LARGE_CATEGORIES,
    
    CURRENT_TIMESTAMP() as LAST_UPDATED;
"""


class DefiSnowflakeWriter:
    """Handles writing DeFi data to Snowflake"""
    
    def __init__(self):
        settings = get_settings()
        self._settings = settings
        self._ctx = None
    
    def _get_connection(self):
        """Get or create Snowflake connection"""
        if self._ctx is None or self._ctx.is_closed():
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
                logger.info("Connected to Snowflake for DeFi data")
            except Exception as e:
                logger.error(f"Snowflake connection error: {e}")
                raise
        return self._ctx
    
    def _ensure_tables(self):
        """Create DeFi tables if they don't exist"""
        cursor = self._ctx.cursor()
        try:
            cursor.execute(CREATE_PROTOCOL_TVL)
            cursor.execute(CREATE_CHAIN_TVL)
            cursor.execute(CREATE_PROTOCOL_HISTORY)
            cursor.execute(CREATE_DEFI_STABLECOINS)
            logger.info("DeFi tables ensured")
        except Exception as e:
            logger.error(f"Error creating DeFi tables: {e}")
        finally:
            cursor.close()
    
    def create_analytical_views(self):
        """Create analytical views for optimized queries"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            logger.info("Creating analytical views...")
            cursor.execute(CREATE_VIEW_TOP_PROTOCOLS)
            logger.info("✓ Created VW_TOP_PROTOCOLS_24H")
            
            cursor.execute(CREATE_VIEW_CHAIN_DOMINANCE)
            logger.info("✓ Created VW_CHAIN_DOMINANCE")
            
            cursor.execute(CREATE_VIEW_CATEGORY_PERFORMANCE)
            logger.info("✓ Created VW_CATEGORY_PERFORMANCE")
            
            cursor.execute(CREATE_VIEW_PROTOCOL_MOVERS)
            logger.info("✓ Created VW_PROTOCOL_MOVERS")
            
            cursor.execute(CREATE_VIEW_MARKET_SUMMARY)
            logger.info("✓ Created VW_MARKET_SUMMARY")
            
            logger.info("All analytical views created successfully")
        except Exception as e:
            logger.error(f"Error creating analytical views: {e}")
            raise
        finally:
            cursor.close()
    
    async def insert_protocol_tvl(self, protocol_data: Dict[str, Any]):
        """Insert protocol TVL snapshot"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # For STRING column storing JSON, just insert the JSON string directly
            insert_sql = """
            INSERT INTO PROTOCOL_TVL (
                PROTOCOL_NAME, PROTOCOL_SLUG, CHAIN, CATEGORY,
                TVL, TVL_PREV_DAY, TVL_PREV_WEEK, TVL_PREV_MONTH,
                CHANGE_1D, CHANGE_7D, CHANGE_1M,
                MARKET_SHARE_PCT, SYMBOL, LOGO, CHAINS
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Convert chains list to JSON string
            chains_json = json.dumps(protocol_data.get('chains', []))
            
            cursor.execute(insert_sql, (
                protocol_data.get('name'),
                protocol_data.get('slug'),
                protocol_data.get('chain'),
                protocol_data.get('category'),
                protocol_data.get('tvl', 0),
                protocol_data.get('tvlPrevDay'),
                protocol_data.get('tvlPrevWeek'),
                protocol_data.get('tvlPrevMonth'),
                protocol_data.get('change_1d'),
                protocol_data.get('change_7d'),
                protocol_data.get('change_1m'),
                protocol_data.get('marketShare'),
                protocol_data.get('symbol'),
                protocol_data.get('logo'),
                chains_json
            ))
            
        except Exception as e:
            logger.error(f"Error inserting protocol TVL: {e}")
        finally:
            cursor.close()
    
    async def insert_chain_tvl(self, chain_data: Dict[str, Any]):
        """Insert chain TVL snapshot"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            insert_sql = """
            INSERT INTO CHAIN_TVL (
                CHAIN_NAME, TVL, TVL_PREV_DAY, TVL_PREV_WEEK,
                CHANGE_1D, CHANGE_7D, TOKEN_SYMBOL, CMCID, GECKO_ID
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                chain_data.get('name'),
                chain_data.get('tvl', 0),
                chain_data.get('tvlPrevDay'),
                chain_data.get('tvlPrevWeek'),
                chain_data.get('change_1d'),
                chain_data.get('change_7d'),
                chain_data.get('tokenSymbol'),
                chain_data.get('cmcId'),
                chain_data.get('gecko_id')
            ))
            
        except Exception as e:
            logger.error(f"Error inserting chain TVL: {e}")
        finally:
            cursor.close()
    
    async def bulk_insert_protocols(self, protocols: List[Dict[str, Any]]):
        """Bulk insert multiple protocols"""
        for protocol in protocols:
            await self.insert_protocol_tvl(protocol)
        logger.info(f"Bulk inserted {len(protocols)} protocols")
    
    async def bulk_insert_chains(self, chains: List[Dict[str, Any]]):
        """Bulk insert multiple chains"""
        for chain in chains:
            await self.insert_chain_tvl(chain)
        logger.info(f"Bulk inserted {len(chains)} chains")
    
    def close(self):
        """Close Snowflake connection"""
        if self._ctx and not self._ctx.is_closed():
            self._ctx.close()
            logger.info("DeFi Snowflake connection closed")
