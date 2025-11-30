import streamlit as st
import importlib
import os

# Page configuration
st.set_page_config(
    page_title="Crypto & DeFi Data Warehouse Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better aesthetics
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    h1 {
        color: #1f77b4;
    }
    </style>
    """, unsafe_allow_html=True)

# List available pages
PAGES = {
    "🏦 DeFi Protocol TVL Explorer": "protocol_tvl",
    "⛓️ Blockchain Chain Dominance": "chain_tvl",
    "💹 Crypto Market Overview": "market_overview",
    "💰 Stablecoin Analytics": "stablecoins",
    "📊 Technical Analysis Dashboard": "technical_indicators",
    "📈 OHLCV Candle Charts": "candles"
}

# Sidebar navigation
st.sidebar.title("🚀 Navigation")
st.sidebar.markdown("---")
st.sidebar.markdown("""
### Welcome to the Dashboard
Explore comprehensive crypto and DeFi analytics with interactive visualizations.

**Features:**
- Real-time market data
- DeFi protocol analytics
- Technical indicators
- Candlestick charts
""")

st.sidebar.markdown("---")
selection = st.sidebar.radio("Select Dashboard", list(PAGES.keys()))

# Footer in sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("**Crypto & DeFi Data Warehouse**")
st.sidebar.markdown("*Powered by Snowflake & Supabase*")

# Main content
try:
    # Dynamically import and run the selected page
    page_module = importlib.import_module(f"pages.{PAGES[selection]}")
    page_module.main()
except Exception as e:
    st.error(f"❌ Error loading page: {str(e)}")
    st.info("Please ensure all data files are present in the snowflake_export directory.")
