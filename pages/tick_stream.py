"""Real-time tick-level streaming chart (lowest latency)"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
from datetime import datetime, timedelta
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


def fetch_recent_ticks(symbol: str, minutes: int = 5):
    """Fetch recent tick data from Supabase (real-time DB)"""
    client = get_supabase_client()
    
    # Calculate cutoff time
    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    cutoff_str = cutoff.isoformat()
    
    response = client.table(settings.supabase_market_ticks_table)\
        .select("*")\
        .eq("symbol", symbol)\
        .gte("exchange_ts", cutoff_str)\
        .order("exchange_ts", desc=False)\
        .execute()
    
    return pd.DataFrame(response.data) if response.data else pd.DataFrame()


def plot_tick_stream(df: pd.DataFrame, symbol: str):
    """Create streaming line chart with tick data"""
    if df.empty:
        return None
    
    # Convert timestamp
    df['exchange_ts'] = pd.to_datetime(df['exchange_ts'])
    
    fig = go.Figure()
    
    # Price line
    fig.add_trace(
        go.Scatter(
            x=df['exchange_ts'],
            y=df['last_price'],
            mode='lines+markers',
            name='Price',
            line=dict(color='#26a69a', width=2),
            marker=dict(size=4),
            hovertemplate='<b>%{x}</b><br>Price: $%{y:,.2f}<extra></extra>'
        )
    )
    
    # Update layout for streaming feel
    fig.update_layout(
        title=f"{symbol} - Real-Time Tick Stream",
        xaxis_title="Time (UTC)",
        yaxis_title="Price (USD)",
        hovermode='x unified',
        template='plotly_dark',
        height=500,
        margin=dict(l=50, r=50, t=50, b=50),
        xaxis=dict(
            rangeslider=dict(visible=False),
            type='date'
        )
    )
    
    return fig


def render():
    st.title("⚡ Real-Time Tick Stream")
    st.markdown("**Lowest latency** - Direct tick-level data from Supabase (5-second updates)")
    
    # Controls
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        symbol = st.selectbox(
            "Symbol",
            ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
            key="tick_symbol"
        )
    
    with col2:
        time_window = st.selectbox(
            "Time Window",
            [1, 2, 5, 10, 15, 30],
            index=2,
            format_func=lambda x: f"Last {x} minutes"
        )
    
    with col3:
        auto_refresh = st.checkbox("Auto-refresh", value=True)
    
    # Fetch and display
    with st.spinner("Loading tick data..."):
        df = fetch_recent_ticks(symbol, time_window)
    
    if df.empty:
        st.warning(f"""
        No tick data available for {symbol} in the last {time_window} minutes.
        
        Make sure the ingestion service is running and has collected some data.
        """)
        return
    
    # Create chart
    fig = plot_tick_stream(df, symbol)
    
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    
    # Live statistics
    st.subheader("📊 Live Statistics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    latest = df.iloc[-1]
    first = df.iloc[0]
    
    with col1:
        st.metric("Current Price", f"${latest['last_price']:,.2f}")
    
    with col2:
        price_change = latest['last_price'] - first['last_price']
        price_change_pct = (price_change / first['last_price']) * 100
        st.metric(
            f"Change ({time_window}m)", 
            f"${price_change:+,.2f}",
            f"{price_change_pct:+.2f}%"
        )
    
    with col3:
        st.metric("Highest", f"${df['last_price'].max():,.2f}")
    
    with col4:
        st.metric("Lowest", f"${df['last_price'].min():,.2f}")
    
    with col5:
        st.metric("Ticks", len(df))
    
    # Tick rate analysis
    if len(df) > 1:
        df['time_diff'] = df['exchange_ts'].diff().dt.total_seconds()
        avg_interval = df['time_diff'].mean()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Avg Tick Interval", f"{avg_interval:.1f}s")
        
        with col2:
            st.metric("Ticks/Minute", f"{60/avg_interval:.1f}")
        
        with col3:
            age_seconds = (datetime.utcnow() - df.iloc[-1]['exchange_ts'].to_pydatetime().replace(tzinfo=None)).total_seconds()
            st.metric("Last Tick Age", f"{int(age_seconds)}s ago")
    
    # Technical indicators (from real-time data)
    st.subheader("🔬 Technical Indicators (Live)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("24h Change", f"{latest.get('price_change_pct_24h', 0):.2f}%")
    
    with col2:
        st.metric("1h Change", f"{latest.get('price_change_pct_1h', 0):.2f}%")
    
    with col3:
        st.metric("Volume 24h", f"${latest.get('volume_24h_quote', 0)/1e6:.1f}M")
    
    with col4:
        st.metric("Bid/Ask Spread", f"{latest.get('bid_ask_spread', 0):.4f}%")
    
    # Price distribution
    st.subheader("📈 Price Distribution")
    
    import plotly.express as px
    
    fig_hist = px.histogram(
        df, 
        x='last_price',
        nbins=50,
        title=f"Price Distribution - Last {time_window} minutes",
        labels={'last_price': 'Price (USD)', 'count': 'Frequency'}
    )
    fig_hist.update_layout(
        template='plotly_dark',
        height=300,
        showlegend=False
    )
    st.plotly_chart(fig_hist, use_container_width=True)
    
    # Raw data table
    with st.expander("📋 Raw Tick Data"):
        display_df = df[['exchange_ts', 'last_price', 'volume_24h_quote', 'bid_ask_spread']].copy()
        display_df['exchange_ts'] = display_df['exchange_ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
        display_df.columns = ['Timestamp', 'Price', 'Volume 24h', 'Spread %']
        st.dataframe(display_df.sort_values('Timestamp', ascending=False), use_container_width=True)
    
    # Last update
    st.caption(f"Dashboard updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(5)
        st.rerun()
