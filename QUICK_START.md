# Crypto Data Warehouse - Quick Start Guide

## 🚀 Launch Commands

### Option 1: Start Both Services (Recommended)

**Terminal 1 - Data Ingestion Service:**
```powershell
cd D:\postgresql\crypto-data
.\.venv\Scripts\Activate.ps1
python -m src.server
```
Wait for: `INFO: Fetched 6703 protocols from DefiLlama`

**Terminal 2 - Streamlit Dashboard:**
```powershell
cd D:\postgresql\crypto-data
.\.venv\Scripts\Activate.ps1
streamlit run streamlit_app.py
```

### Option 2: Dashboard Only (View Existing Data)
```powershell
cd D:\postgresql\crypto-data
.\.venv\Scripts\Activate.ps1
streamlit run streamlit_app.py
```

---

## 📊 Dashboard Access

Open browser: **http://localhost:8501**

### Navigation Guide:

#### 💰 **Crypto (CEX)** Section
Real-time crypto price data from Binance:
- 🔴 **Live Prices** - Current market prices
- ⚡ **Tick Stream** - Real-time price updates
- 📊 **Historical Charts** - Price history analysis
- 🔔 **Price Alerts** - Alert management
- 📈 **Market Overview** - CEX market summary

#### 🏦 **DeFi Analytics** Section ⭐ NEW
DeFi protocol & chain TVL tracking:
- 🏦 **Protocol Rankings** - Top 100 protocols by TVL
  - Filter by category (Lending, DEX, Liquid Staking, etc.)
  - Filter by chain (Ethereum, Solana, Multi-Chain, etc.)
  - Interactive TVL rankings chart
  - Market share analysis
  - Top gainers/losers

- ⛓️ **Chain Analysis** - 416 blockchain comparison
  - Chain TVL rankings
  - Chain dominance pie chart
  - L1 vs L2 distribution
  - Protocol activity per chain
  - Top gaining/declining chains

- 🌐 **Market Overview** - DeFi ecosystem health
  - Total TVL & protocol count
  - Category performance analysis
  - Biggest market movers
  - Market sentiment indicators
  - Ecosystem diversity score

---

## 🔍 What to Look For

### First Launch Checklist:

**DeFi Protocol Rankings:**
1. ✅ Total TVL should show ~$300B+
2. ✅ Top protocol: Binance CEX (~$162B TVL)
3. ✅ 6,703 protocols tracked
4. ✅ Charts load with color-coded changes
5. ✅ Filters work (category, chain, top N)

**Chain Analysis:**
1. ✅ 416 chains displayed
2. ✅ Top chain: Ethereum (~$65B TVL)
3. ✅ L1 vs L2 pie chart shows split
4. ✅ Protocol count per chain visible

**Market Overview:**
1. ✅ Category breakdown shows 20+ categories
2. ✅ Top gainers/losers lists populate
3. ✅ Ecosystem health indicators display
4. ✅ Market sentiment shows current state

---

## 🐛 Troubleshooting

### "No protocol data available"
**Solution:**
1. Start ingestion service: `python -m src.server`
2. Wait 10 seconds for first DeFi fetch
3. Check logs for: `INFO: Fetched 6703 protocols`
4. Refresh dashboard page

### "Error fetching protocol data"
**Solution:**
1. Verify `.env` file has Snowflake credentials
2. Check Snowflake connection: `python -c "from src.defi_snowflake_client import DefiSnowflakeWriter; w = DefiSnowflakeWriter(); print('Connected')"`
3. Restart ingestion service

### Charts not loading
**Solution:**
1. Check browser console (F12)
2. Verify plotly installed: `pip show plotly`
3. Clear browser cache (Ctrl+Shift+R)
4. Try different browser (Chrome, Firefox, Edge)

### Slow performance
**Solution:**
1. Reduce top N slider (show fewer protocols/chains)
2. Filter by specific category or chain
3. Close other browser tabs
4. Check CPU/memory usage

---

## 📈 Key Metrics to Monitor

### Protocol Rankings Page:
- **Total TVL** - Should be ~$300B+
- **Top 10 Protocols** - Market leaders
- **7D Change** - Growth/decline trends
- **Market Share %** - Protocol dominance

### Chain Analysis Page:
- **Ethereum TVL** - Largest ecosystem (~$65B)
- **L2 Growth** - Arbitrum, Base, Optimism
- **Chain Count** - 416 blockchains tracked
- **Protocol Activity** - Protocols per chain

### Market Overview:
- **Market Sentiment** - Bullish/Bearish indicator
- **Diversity Score** - Ecosystem health (0-100)
- **Growth Momentum** - % of growing categories
- **Category Leaders** - Top performing sectors

---

## 💡 Tips & Tricks

### Find High-Growth Protocols:
1. Go to **Protocol Rankings**
2. Sort by **7D Change** column
3. Look for protocols with >10% weekly growth
4. Check category and TVL size

### Compare Blockchains:
1. Go to **Chain Analysis**
2. Look at TVL rankings chart
3. Compare L1 vs L2 pie chart
4. Check protocol count per chain

### Identify Market Trends:
1. Go to **Market Overview**
2. Check **Category Analysis** charts
3. Review **Biggest Movers** lists
4. Monitor **Ecosystem Health** indicators

### Filter by Interest:
1. Use **Category Filter** for specific sectors (e.g., Lending)
2. Use **Chain Filter** for specific blockchain (e.g., Ethereum)
3. Adjust **Top N** slider to see more/less protocols

---

## 🎯 Use Cases

### Investment Research:
1. Identify top protocols by TVL
2. Track 7-day growth trends
3. Compare protocols within categories
4. Monitor market share changes

### Market Analysis:
1. Compare blockchain ecosystems
2. Track L2 vs L1 competition
3. Identify emerging chains
4. Analyze sector rotation

### Risk Management:
1. Monitor declining protocols
2. Check ecosystem diversity
3. Track market sentiment
4. Identify concentration risks

### Trend Spotting:
1. Find fastest-growing protocols
2. Identify hot categories
3. Track new chain launches
4. Spot migration patterns

---

## 📊 Data Freshness

**Update Intervals:**
- **CEX Data**: Every 5 seconds (Binance → Supabase)
- **DeFi Data**: Every 60 minutes (DefiLlama → Snowflake)

**Historical Data:**
- **CEX**: Real-time + historical ticks
- **DeFi**: Latest snapshots (last 2 hours shown)

**Data Source:**
- **CEX**: Binance WebSocket API
- **DeFi**: DefiLlama REST API (100% free)

---

## 🎨 Dashboard Features

### Interactive Elements:
- ✅ Dropdowns (category, chain selection)
- ✅ Sliders (top N adjustment)
- ✅ Sortable tables (click column headers)
- ✅ Hover tooltips (detailed metrics)
- ✅ Zoom/pan charts (plotly controls)

### Visualizations:
- ✅ Bar charts (horizontal TVL rankings)
- ✅ Pie charts (market share distribution)
- ✅ Scatter plots (TVL vs protocol count)
- ✅ Tables (detailed data grids)
- ✅ Metric cards (summary statistics)

### Color Coding:
- 🟢 **Green** - Positive changes (growth)
- 🔴 **Red** - Negative changes (decline)
- 🟡 **Yellow** - Neutral/mixed
- 📊 **Gradients** - Continuous scales (change %)

---

## 🚀 Next Steps

### Immediate Actions:
1. ✅ Launch dashboard: `streamlit run streamlit_app.py`
2. ✅ Explore all 3 DeFi pages
3. ✅ Test filters and interactivity
4. ✅ Verify data accuracy

### Optional Enhancements:
1. Add historical TVL trend charts (line graphs)
2. Implement protocol comparison tool
3. Create custom alerts for TVL changes
4. Add CSV export functionality
5. Build stablecoin tracking page

### Advanced Features:
1. Multi-timeframe analysis (1D, 7D, 30D views)
2. Correlation matrix (protocol relationships)
3. Risk metrics (volatility, drawdowns)
4. Portfolio tracker (custom watchlist)

---

## 📞 Support

### Check Status:
```powershell
# API health check
curl http://localhost:8000/health

# View ingestion logs
# (Terminal running src.server)

# Check Streamlit logs
# (Terminal running streamlit)
```

### Common Commands:
```powershell
# Re-run tests
python -m tests.test_defi_integration

# Manual DeFi fetch
curl -X POST http://localhost:8000/defi/fetch

# Check protocol data
curl http://localhost:8000/defi/protocols

# Check chain data
curl http://localhost:8000/defi/chains
```

---

## ✨ Success!

You now have a **complete DeFi analytics dashboard** with:
- 🏦 6,703 protocols tracked
- ⛓️ 416 blockchains monitored
- 📊 20+ interactive charts
- 💰 $300B+ TVL coverage
- 💵 $0.00/month cost

**Enjoy exploring the DeFi ecosystem! 🚀**
