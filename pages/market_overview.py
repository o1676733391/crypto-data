"""Market overview with aggregated statistics and insights"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import snowflake.connector
from src.config import get_settings

settings = get_settings()


@st.cache_resource
def get_snowflake_connection():
    """Initialize Snowflake connection"""
    return snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )


def fetch_market_summary():
    """Fetch market summary from VW_LATEST_PRICES view"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            SYMBOL,
            LAST_PRICE,
            VOLUME_24H,
            PRICE_CHANGE_PCT_24H,
            SECONDS_AGO
        FROM VW_LATEST_PRICES
        ORDER BY VOLUME_24H DESC
    """
    
    try:
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()


def fetch_volatility_data():
    """Fetch volatility metrics"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            SYMBOL,
            ROLLING_VOLATILITY_24H,
            HIGH_LOW_RANGE_24H,
            RSI_14
        FROM TECHNICAL_INDICATORS
        WHERE EXCHANGE_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        ORDER BY SYMBOL, EXCHANGE_TS DESC
    """
    
    try:
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Get latest for each symbol
        if not df.empty:
            df = df.groupby('symbol').first().reset_index()
        
        return df
    finally:
        cursor.close()


def fetch_hourly_volume():
    """Fetch hourly volume trends"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            symbol,
            candle_time,
            volume_quote
        FROM CANDLES_1HOUR
        WHERE candle_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        ORDER BY symbol, candle_time
    """
    
    try:
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()


def render():
    st.title("📈 Market Overview")
    st.markdown("Comprehensive market statistics and insights")
    
    # Fetch data
    with st.spinner("Loading market data..."):
        summary_df = fetch_market_summary()
        volatility_df = fetch_volatility_data()
        volume_df = fetch_hourly_volume()
    
    if summary_df.empty:
        st.warning("No market data available yet. Make sure the ingestion service is running.")
        return
    
    # Top metrics
    st.subheader("🎯 Market Snapshot")
    
    cols = st.columns(len(summary_df))
    for idx, row in summary_df.iterrows():
        with cols[idx]:
            delta_color = "normal" if row['price_change_pct_24h'] >= 0 else "inverse"
            st.metric(
                label=row['symbol'],
                value=f"${row['last_price']:,.2f}",
                delta=f"{row['price_change_pct_24h']:.2f}%",
                delta_color=delta_color
            )
    
    st.markdown("---")
    
    # Performance ranking
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Performers (24h)")
        top_performers = summary_df.nlargest(10, 'price_change_pct_24h')
        
        fig = px.bar(
            top_performers,
            x='symbol',
            y='price_change_pct_24h',
            color='price_change_pct_24h',
            color_continuous_scale=['red', 'yellow', 'green'],
            title="24h Price Change %"
        )
        fig.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Volume Leaders (24h)")
        top_volume = summary_df.nlargest(10, 'volume_24h')
        
        fig = px.bar(
            top_volume,
            x='symbol',
            y='volume_24h',
            color='volume_24h',
            color_continuous_scale='Blues',
            title="24h Trading Volume (USD)"
        )
        fig.update_layout(height=300, showlegend=False)
        fig.update_yaxes(title_text="Volume (USD)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Volatility analysis
    if not volatility_df.empty:
        st.markdown("---")
        st.subheader("📉 Volatility Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                volatility_df,
                x='symbol',
                y='rolling_volatility_24h',
                color='rolling_volatility_24h',
                color_continuous_scale='Reds',
                title="24h Volatility"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                volatility_df,
                x='symbol',
                y='rsi_14',
                color='rsi_14',
                color_continuous_scale='RdYlGn',
                title="RSI (14) - Overbought/Oversold"
            )
            fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought")
            fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold")
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    # Volume trends
    if not volume_df.empty:
        st.markdown("---")
        st.subheader("📈 Hourly Volume Trends (Last 24h)")
        
        fig = px.line(
            volume_df,
            x='candle_time',
            y='volume_quote',
            color='symbol',
            title="Trading Volume Over Time"
        )
        fig.update_layout(height=400)
        fig.update_yaxes(title_text="Volume (USD)")
        fig.update_xaxes(title_text="Time")
        st.plotly_chart(fig, use_container_width=True)
    
    # Market summary table
    st.markdown("---")
    st.subheader("📋 Complete Market Summary")
    
    # Merge dataframes
    if not volatility_df.empty:
        display_df = summary_df.merge(volatility_df, on='symbol', how='left')
    else:
        display_df = summary_df
    
    # Format for display
    display_df['last_price'] = display_df['last_price'].apply(lambda x: f"${x:,.2f}")
    display_df['volume_24h'] = display_df['volume_24h'].apply(lambda x: f"${x/1e6:.1f}M")
    display_df['price_change_pct_24h'] = display_df['price_change_pct_24h'].apply(lambda x: f"{x:.2f}%")
    
    if 'rolling_volatility_24h' in display_df.columns:
        display_df['rolling_volatility_24h'] = display_df['rolling_volatility_24h'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    
    if 'rsi_14' in display_df.columns:
        display_df['rsi_14'] = display_df['rsi_14'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
    
    # Rename columns for display
    display_df.rename(columns={
        'symbol': 'Symbol',
        'last_price': 'Price',
        'volume_24h': 'Volume 24h',
        'price_change_pct_24h': 'Change 24h',
        'rolling_volatility_24h': 'Volatility',
        'rsi_14': 'RSI',
        'seconds_ago': 'Data Age (s)'
    }, inplace=True)
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Data freshness indicator
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
