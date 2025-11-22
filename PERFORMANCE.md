# Crypto Data Lake - Low Latency Configuration

## Performance Optimizations Applied

### 1. **Fetch Interval**
- **Before**: 60 seconds
- **After**: 5 seconds
- **Impact**: 12x more frequent updates for near real-time data

### 2. **API Request Strategy**
- **Before**: Sequential requests with delays (200ms between symbols)
- **After**: Parallel requests with 100ms stagger
- **Impact**: ~70% faster data fetching

### 3. **Connection Pool**
- **Before**: 5 max connections, 2 keepalive
- **After**: 20 max connections, 10 keepalive
- **Impact**: Better concurrency for multiple symbols

### 4. **HTTP Protocol**
- **Enabled**: HTTP/2 with multiplexing
- **Impact**: Multiple requests over single connection

### 5. **Database Writes**
- **Removed**: Async locks on Snowflake writes
- **Impact**: Parallel writes to Supabase and Snowflake

### 6. **Latency Metrics**
- Added `/stats` endpoint to monitor performance
- Tracks last 100 fetch latencies
- Average latency: ~1.6 seconds per cycle

## Current Performance

```
Average fetch latency: 1.6 seconds
Data freshness: 5 seconds
Symbols tracked: 4 (BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT)
Success rate: 100% (all symbols fetched successfully)
```

## API Endpoints

### GET /health
Basic health check
```json
{
  "status": "ok",
  "tracked_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
}
```

### GET /stats
Performance statistics
```json
{
  "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
  "fetch_interval_seconds": 5,
  "last_fetch_time": 1732239594.166,
  "avg_fetch_latency_ms": 1607.82,
  "recent_latencies_ms": [3201.28, 1573.89, 1608.35],
  "tracked_symbols_count": 4
}
```

### GET /latest/{symbol}
Latest market data for a symbol
```bash
curl http://localhost:8000/latest/BTCUSDT
```

## Further Optimizations (if needed)

### For sub-second latency:
1. **WebSocket Streams**: Use Binance WebSocket API instead of REST
   - True real-time updates (10-100ms latency)
   - No rate limits
   - More efficient

2. **Redis Cache**: Add Redis layer between service and databases
   - In-memory data access
   - Publish/subscribe for real-time clients
   - Sub-millisecond reads

3. **Database Partitioning**: 
   - Partition Snowflake tables by date/symbol
   - Use Snowflake Tasks for automated processing
   - Stream data with Snowpipe

4. **Horizontal Scaling**:
   - Deploy multiple instances
   - Load balance across symbols
   - Each instance handles subset of symbols

## Monitoring

Monitor these metrics:
- Average fetch latency (should stay <2s)
- Error rate (should stay <1%)
- Database insert latency
- Memory usage (increase with more symbols)

## Configuration

Key settings in `.env`:
```
FETCH_INTERVAL_SECONDS=5        # How often to fetch
HTTP_TIMEOUT_SECONDS=15         # Request timeout
SYMBOLS=BTCUSDT,ETHUSDT,...     # Symbols to track
```

## Cost Considerations

**With current setup (5s interval):**
- API calls: 17,280 requests/day per symbol
- Database inserts: 17,280 rows/day per symbol
- For 4 symbols: ~69,000 rows/day total

**Binance Rate Limits:**
- Weight: 1200 requests/minute
- Current usage: ~48 requests/minute (well within limit)
- Room to add ~20 more symbols at 5s interval
