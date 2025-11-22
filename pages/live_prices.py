"""Real-time price monitoring from Supabase"""
import streamlit as st
import time
from datetime import datetime
from src.config import get_settings
from supabase import create_client

settings = get_settings()


@st.cache_resource
def get_supabase_client():
    """Initialize Supabase client"""
    return create_client(
        str(settings.supabase_url),
        settings.supabase_service_role_key
    )


def fetch_latest_prices():
    """Fetch latest prices for all symbols"""
    client = get_supabase_client()
    
    # Get latest tick for each symbol
    response = client.table(settings.supabase_market_ticks_table)\
        .select("*")\
        .order("exchange_ts", desc=True)\
        .limit(100)\
        .execute()
    
    # Group by symbol and get latest
    latest_by_symbol = {}
    for tick in response.data:
        symbol = tick['symbol']
        if symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = tick
    
    return latest_by_symbol


def render():
    st.title("🔴 Live Market Prices")
    st.markdown("Real-time cryptocurrency prices updated every 5 seconds")
    
    # Auto-refresh toggle
    col1, col2 = st.columns([3, 1])
    with col2:
        auto_refresh = st.checkbox("Auto-refresh", value=True)
    
    # Placeholder for dynamic content
    price_container = st.container()
    
    # Metrics container
    with price_container:
        try:
            prices = fetch_latest_prices()
            
            if not prices:
                st.warning("No price data available yet. Make sure the ingestion service is running.")
                return
            
            # Display metrics in columns
            cols = st.columns(len(prices))
            
            for idx, (symbol, data) in enumerate(sorted(prices.items())):
                with cols[idx]:
                    # Calculate time since update
                    exchange_ts = datetime.fromisoformat(data['exchange_ts'].replace('Z', '+00:00'))
                    age_seconds = (datetime.now(exchange_ts.tzinfo) - exchange_ts).total_seconds()
                    
                    # Color coding based on 24h change
                    change_24h = data.get('price_change_pct_24h', 0)
                    delta_color = "normal" if change_24h >= 0 else "inverse"
                    
                    st.metric(
                        label=symbol,
                        value=f"${data['last_price']:,.2f}",
                        delta=f"{change_24h:.2f}%",
                        delta_color=delta_color
                    )
                    
                    # Additional info
                    st.caption(f"Vol 24h: ${data.get('volume_24h_quote', 0)/1e6:.1f}M")
                    st.caption(f"Updated: {int(age_seconds)}s ago")
            
            st.markdown("---")
            
            # Detailed table
            st.subheader("📋 Detailed Market Data")
            
            import pandas as pd
            
            # Prepare data for table
            table_data = []
            for symbol, data in sorted(prices.items()):
                table_data.append({
                    'Symbol': symbol,
                    'Price': f"${data['last_price']:,.2f}",
                    'Change 1h': f"{data.get('price_change_pct_1h', 0):.2f}%",
                    'Change 24h': f"{data.get('price_change_pct_24h', 0):.2f}%",
                    'Volume 24h': f"${data.get('volume_24h_quote', 0)/1e6:.1f}M",
                    'Bid/Ask Spread': f"{data.get('bid_ask_spread', 0):.4f}%",
                    'Exchange': data.get('exchange', 'N/A'),
                })
            
            df = pd.DataFrame(table_data)
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Market depth visualization
            st.subheader("📊 Order Book Depth")
            
            selected_symbol = st.selectbox("Select Symbol", sorted(prices.keys()))
            
            if selected_symbol in prices:
                data = prices[selected_symbol]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Bids (Buy Orders)**")
                    bid_depth = data.get('bid_depth', [])
                    if bid_depth:
                        bid_df = pd.DataFrame(bid_depth)
                        if not bid_df.empty and 'price' in bid_df.columns:
                            bid_df = bid_df.sort_values('price', ascending=False)
                            st.dataframe(bid_df, hide_index=True)
                    else:
                        st.info("No bid data available")
                
                with col2:
                    st.markdown("**Asks (Sell Orders)**")
                    ask_depth = data.get('ask_depth', [])
                    if ask_depth:
                        ask_df = pd.DataFrame(ask_depth)
                        if not ask_df.empty and 'price' in ask_df.columns:
                            ask_df = ask_df.sort_values('price', ascending=True)
                            st.dataframe(ask_df, hide_index=True)
                    else:
                        st.info("No ask data available")
            
            # Last update time
            st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            st.error(f"Error fetching prices: {str(e)}")
            st.exception(e)
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(5)
        st.rerun()
