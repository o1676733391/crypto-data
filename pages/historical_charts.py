"""Historical OHLC charts from Snowflake data warehouse"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import time
from datetime import datetime
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


def fetch_candles(symbol: str, timeframe: str, limit: int = 500):
    """Fetch OHLC candles from Snowflake"""
    table_map = {
        "1 Minute": "CANDLES_1MIN",
        "5 Minutes": "CANDLES_5MIN",
        "15 Minutes": "CANDLES_15MIN",
        "1 Hour": "CANDLES_1HOUR",
        "4 Hours": "CANDLES_4HOUR",
        "Daily": "CANDLES_DAILY"
    }
    
    table = table_map.get(timeframe)
    if not table:
        return pd.DataFrame()
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = f"""
        SELECT 
            candle_time,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            volume_quote,
            trade_count
        FROM {table}
        WHERE symbol = %s
        ORDER BY candle_time DESC
        LIMIT %s
    """
    
    try:
        cursor.execute(query, (symbol, limit))
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        if not df.empty:
            df = df.sort_values('candle_time')
        
        return df
    finally:
        cursor.close()


def fetch_technical_indicators(symbol: str, limit: int = 500):
    """Fetch technical indicators from Snowflake"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            EXCHANGE_TS,
            RSI_14,
            MACD,
            MACD_SIGNAL,
            MOVING_AVERAGE_7,
            MOVING_AVERAGE_30
        FROM TECHNICAL_INDICATORS
        WHERE SYMBOL = %s
        ORDER BY EXCHANGE_TS DESC
        LIMIT %s
    """
    
    try:
        cursor.execute(query, (symbol, limit))
        columns = [desc[0].lower() for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        if not df.empty:
            df = df.sort_values('exchange_ts')
            df.rename(columns={'exchange_ts': 'timestamp'}, inplace=True)
        
        return df
    finally:
        cursor.close()


def plot_candlestick_chart(df: pd.DataFrame, symbol: str, timeframe: str, show_volume: bool = True, indicators_df: pd.DataFrame = None):
    """Create interactive candlestick chart with Plotly"""
    if df.empty:
        return None
    
    # Determine number of subplots
    rows = 2 if show_volume else 1
    row_heights = [0.7, 0.3] if show_volume else [1.0]
    
    fig = make_subplots(
        rows=rows, 
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=(f"{symbol} - {timeframe}", "Volume") if show_volume else (f"{symbol} - {timeframe}",)
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df['candle_time'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='OHLC',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ),
        row=1, col=1
    )
    
    # Add moving averages if technical indicators available
    if indicators_df is not None and not indicators_df.empty:
        # Try to align timestamps
        if 'moving_average_7' in indicators_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=indicators_df['timestamp'],
                    y=indicators_df['moving_average_7'],
                    mode='lines',
                    name='MA 7',
                    line=dict(color='orange', width=1)
                ),
                row=1, col=1
            )
        
        if 'moving_average_30' in indicators_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=indicators_df['timestamp'],
                    y=indicators_df['moving_average_30'],
                    mode='lines',
                    name='MA 30',
                    line=dict(color='blue', width=1)
                ),
                row=1, col=1
            )
    
    # Volume bar chart
    if show_volume:
        colors = ['#26a69a' if df.iloc[i]['close'] >= df.iloc[i]['open'] else '#ef5350' 
                  for i in range(len(df))]
        
        fig.add_trace(
            go.Bar(
                x=df['candle_time'],
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                showlegend=False
            ),
            row=2, col=1
        )
    
    # Update layout
    fig.update_layout(
        height=600,
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
        template='plotly_dark',
        margin=dict(l=50, r=50, t=50, b=50)
    )
    
    fig.update_xaxes(title_text="Time", row=rows, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    if show_volume:
        fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    return fig


def render():
    st.title("📊 Historical Charts")
    st.markdown("OHLC candlestick charts from Snowflake data warehouse")
    
    # Auto-refresh control
    col_refresh, col_spacer = st.columns([1, 3])
    with col_refresh:
        auto_refresh = st.checkbox("Auto-refresh (5s)", value=False)
    
    # Controls
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        symbol = st.selectbox(
            "Symbol",
            ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
        )
    
    with col2:
        timeframe = st.selectbox(
            "Timeframe",
            ["1 Minute", "5 Minutes", "15 Minutes", "1 Hour", "4 Hours", "Daily"]
        )
    
    with col3:
        limit = st.number_input(
            "Candles",
            min_value=50,
            max_value=1000,
            value=200,
            step=50
        )
    
    with col4:
        show_volume = st.checkbox("Show Volume", value=True)
        show_indicators = st.checkbox("Show Indicators", value=True)
    
    # Fetch and display data
    with st.spinner(f"Loading {timeframe} candles for {symbol}..."):
        df = fetch_candles(symbol, timeframe, limit)
        
        if df.empty:
            st.warning(f"""
            No candle data available for {symbol} at {timeframe} timeframe.
            
            **Possible reasons:**
            - Aggregation service hasn't run yet (waits 2 minutes after startup)
            - Not enough raw tick data collected yet
            - Service needs to run for a while to build historical data
            
            **Try:**
            - Wait a few minutes and refresh
            - Check if the ingestion service is running
            - Use a shorter timeframe (1 Minute has most data)
            """)
            return
        
        # Fetch indicators if requested
        indicators_df = None
        if show_indicators:
            indicators_df = fetch_technical_indicators(symbol, limit)
        
        # Create chart
        fig = plot_candlestick_chart(df, symbol, timeframe, show_volume, indicators_df)
        
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        
        # Statistics
        st.subheader("📈 Statistics")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Candles", len(df))
        
        with col2:
            st.metric("Highest", f"${df['high'].max():,.2f}")
        
        with col3:
            st.metric("Lowest", f"${df['low'].min():,.2f}")
        
        with col4:
            price_change = ((df.iloc[-1]['close'] - df.iloc[0]['open']) / df.iloc[0]['open'] * 100)
            st.metric("Period Change", f"{price_change:.2f}%")
        
        with col5:
            st.metric("Total Volume", f"${df['volume_quote'].sum()/1e6:.1f}M")
        
        # Show raw data
        with st.expander("📋 View Raw Data"):
            st.dataframe(df, use_container_width=True)
        
        # Last update time
        st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(5)
        st.rerun()
