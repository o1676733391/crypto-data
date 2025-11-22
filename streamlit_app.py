"""
Crypto Data Warehouse Dashboard
Real-time market monitoring, historical analysis, and price alerts
"""
import streamlit as st

st.set_page_config(
    page_title="Crypto Data Warehouse",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar navigation
st.sidebar.title("📈 Crypto Dashboard")
st.sidebar.markdown("---")

# Main section selector
section = st.sidebar.radio(
    "Section",
    ["💰 Crypto (CEX)", "🏦 DeFi Analytics"]
)

st.sidebar.markdown("---")

# Page navigation based on section
if section == "💰 Crypto (CEX)":
    page = st.sidebar.radio(
        "Navigation",
        ["🔴 Live Prices", "⚡ Tick Stream", "📊 Historical Charts", "🔔 Price Alerts", "📈 Market Overview"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### Crypto Data
    Real-time CEX market data
    - **Source**: Binance API
    - **Real-time DB**: Supabase
    - **Analytics DB**: Snowflake
    - **Update**: 5 seconds
    """)
else:
    page = st.sidebar.radio(
        "Navigation",
        ["🏦 Protocol Rankings", "⛓️ Chain Analysis", "🌐 Market Overview"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### DeFi Data
    Protocol & chain TVL tracking
    - **Source**: DefiLlama API
    - **Protocols**: 6,703
    - **Chains**: 416
    - **Update**: 60 minutes
    """)

# Main content routing
if section == "💰 Crypto (CEX)":
    if page == "🔴 Live Prices":
        from pages import live_prices
        live_prices.render()

    elif page == "⚡ Tick Stream":
        from pages import tick_stream
        tick_stream.render()
        
    elif page == "📊 Historical Charts":
        from pages import historical_charts
        historical_charts.render()
        
    elif page == "🔔 Price Alerts":
        from pages import price_alerts
        price_alerts.render()
        
    elif page == "📈 Market Overview":
        from pages import market_overview
        market_overview.render()

else:  # DeFi Analytics section
    if page == "🏦 Protocol Rankings":
        from pages import defi_protocols
        defi_protocols.render()
    
    elif page == "⛓️ Chain Analysis":
        from pages import defi_chains
        defi_chains.render()
    
    elif page == "🌐 Market Overview":
        from pages import defi_overview
        defi_overview.render()
