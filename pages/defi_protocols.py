"""
DeFi Protocols Analytics
Protocol rankings, TVL tracking, and market share analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import asyncio
import sys
sys.path.append('.')

from src.defi_snowflake_client import DefiSnowflakeWriter


def get_protocol_data():
    """Fetch latest protocol data from Snowflake"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        # Get latest snapshot
        query = """
        SELECT 
            PROTOCOL_NAME,
            PROTOCOL_SLUG,
            CHAIN,
            CATEGORY,
            TVL,
            TVL_PREV_DAY,
            TVL_PREV_WEEK,
            CHANGE_1D,
            CHANGE_7D,
            CHANGE_1M,
            MARKET_SHARE_PCT,
            SYMBOL,
            TIMESTAMP
        FROM PROTOCOL_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        ORDER BY TVL DESC
        LIMIT 100
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        cursor.close()
        writer.close()
        
        if data:
            df = pd.DataFrame(data, columns=columns)
            return df
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Error fetching protocol data: {e}")
        return pd.DataFrame()


def render():
    """Render DeFi protocols page"""
    st.title("🏦 DeFi Protocols Analytics")
    st.markdown("Track the largest DeFi protocols by Total Value Locked (TVL)")
    
    # Fetch data
    with st.spinner("Loading protocol data..."):
        df = get_protocol_data()
    
    if df.empty:
        st.warning("No protocol data available. Run the DeFi ingestion service first.")
        st.code("python -m src.server", language="bash")
        return
    
    # Summary metrics
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_tvl = df['TVL'].sum() / 1e9
        st.metric("Total TVL", f"${total_tvl:.2f}B")
    
    with col2:
        protocol_count = len(df)
        st.metric("Protocols Tracked", f"{protocol_count}")
    
    with col3:
        avg_change_7d = df['CHANGE_7D'].mean()
        st.metric("Avg 7D Change", f"{avg_change_7d:.2f}%", delta=f"{avg_change_7d:.2f}%")
    
    with col4:
        categories = df['CATEGORY'].nunique()
        st.metric("Categories", f"{categories}")
    
    st.markdown("---")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Category filter
        categories = ['All'] + sorted(df['CATEGORY'].dropna().unique().tolist())
        selected_category = st.selectbox("Filter by Category", categories)
    
    with col2:
        # Chain filter
        chains = ['All'] + sorted(df['CHAIN'].dropna().unique().tolist())
        selected_chain = st.selectbox("Filter by Chain", chains)
    
    with col3:
        # Top N filter
        top_n = st.slider("Show Top N Protocols", min_value=10, max_value=100, value=20, step=10)
    
    # Apply filters
    filtered_df = df.copy()
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['CATEGORY'] == selected_category]
    if selected_chain != 'All':
        filtered_df = filtered_df[filtered_df['CHAIN'] == selected_chain]
    
    filtered_df = filtered_df.head(top_n)
    
    # Top Protocols Bar Chart
    st.subheader(f"📊 Top {len(filtered_df)} Protocols by TVL")
    
    if not filtered_df.empty:
        # Prepare data for chart
        chart_df = filtered_df.copy()
        chart_df['TVL_B'] = chart_df['TVL'] / 1e9
        chart_df = chart_df.sort_values('TVL', ascending=True)  # Ascending for horizontal bar
        
        # Create bar chart
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=chart_df['PROTOCOL_NAME'],
            x=chart_df['TVL_B'],
            orientation='h',
            text=chart_df['TVL_B'].apply(lambda x: f'${x:.2f}B'),
            textposition='outside',
            marker=dict(
                color=chart_df['CHANGE_7D'],
                colorscale='RdYlGn',
                showscale=True,
                colorbar=dict(title="7D Change %")
            ),
            hovertemplate='<b>%{y}</b><br>TVL: $%{x:.2f}B<br>7D Change: %{marker.color:.2f}%<extra></extra>'
        ))
        
        fig.update_layout(
            title="Protocol TVL Rankings (Color = 7-Day Change %)",
            xaxis_title="Total Value Locked (Billions USD)",
            yaxis_title="Protocol",
            height=max(400, len(chart_df) * 25),
            showlegend=False,
            hovermode='closest'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Market Share Pie Chart
    st.subheader("🥧 Market Share Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top protocols pie chart
        top_10 = filtered_df.head(10).copy()
        top_10['TVL_B'] = top_10['TVL'] / 1e9
        
        fig_pie = px.pie(
            top_10,
            values='TVL_B',
            names='PROTOCOL_NAME',
            title=f'Top 10 Protocols Market Share',
            hole=0.4
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Category distribution
        category_dist = filtered_df.groupby('CATEGORY')['TVL'].sum().reset_index()
        category_dist['TVL_B'] = category_dist['TVL'] / 1e9
        category_dist = category_dist.sort_values('TVL', ascending=False)
        
        fig_cat = px.pie(
            category_dist,
            values='TVL_B',
            names='CATEGORY',
            title='TVL Distribution by Category',
            hole=0.4
        )
        fig_cat.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_cat, use_container_width=True)
    
    # Detailed Table
    st.subheader("📋 Protocol Details")
    
    # Format dataframe for display
    display_df = filtered_df.copy()
    display_df['TVL'] = display_df['TVL'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notna(x) else "N/A")
    display_df['TVL_PREV_DAY'] = display_df['TVL_PREV_DAY'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notna(x) else "N/A")
    display_df['CHANGE_1D'] = display_df['CHANGE_1D'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    display_df['CHANGE_7D'] = display_df['CHANGE_7D'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    display_df['CHANGE_1M'] = display_df['CHANGE_1M'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    display_df['MARKET_SHARE_PCT'] = display_df['MARKET_SHARE_PCT'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    
    # Select columns to display
    cols_to_show = ['PROTOCOL_NAME', 'CATEGORY', 'CHAIN', 'TVL', 'CHANGE_1D', 'CHANGE_7D', 'CHANGE_1M', 'MARKET_SHARE_PCT']
    display_df = display_df[cols_to_show]
    display_df.columns = ['Protocol', 'Category', 'Chain', 'TVL', '1D Change', '7D Change', '30D Change', 'Market Share']
    
    st.dataframe(
        display_df,
        use_container_width=True,
        height=500
    )
    
    # Top Gainers/Losers
    st.markdown("---")
    st.subheader("📈 Top Gainers & Losers (7 Days)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚀 Top Gainers")
        gainers = df.nlargest(5, 'CHANGE_7D')[['PROTOCOL_NAME', 'CHANGE_7D', 'TVL']]
        gainers['TVL'] = gainers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
        gainers['CHANGE_7D'] = gainers['CHANGE_7D'].apply(lambda x: f"+{x:.2f}%")
        gainers.columns = ['Protocol', '7D Change', 'TVL']
        st.dataframe(gainers, hide_index=True, use_container_width=True)
    
    with col2:
        st.markdown("#### 📉 Top Losers")
        losers = df.nsmallest(5, 'CHANGE_7D')[['PROTOCOL_NAME', 'CHANGE_7D', 'TVL']]
        losers['TVL'] = losers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
        losers['CHANGE_7D'] = losers['CHANGE_7D'].apply(lambda x: f"{x:.2f}%")
        losers.columns = ['Protocol', '7D Change', 'TVL']
        st.dataframe(losers, hide_index=True, use_container_width=True)
    
    # Last updated timestamp
    if not df.empty and 'TIMESTAMP' in df.columns:
        last_update = df['TIMESTAMP'].max()
        st.caption(f"Last updated: {last_update}")
