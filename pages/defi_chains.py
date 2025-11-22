"""
DeFi Chains Analytics
Blockchain TVL comparison, chain dominance, and cross-chain analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
sys.path.append('.')

from src.defi_snowflake_client import DefiSnowflakeWriter


def get_chain_data():
    """Fetch latest chain TVL data from Snowflake"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            CHAIN_NAME,
            TVL,
            TVL_PREV_DAY,
            TVL_PREV_WEEK,
            CHANGE_1D,
            CHANGE_7D,
            TOKEN_SYMBOL,
            TIMESTAMP
        FROM CHAIN_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        ORDER BY TVL DESC
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
        st.error(f"Error fetching chain data: {e}")
        return pd.DataFrame()


def get_protocol_by_chain():
    """Get protocol count by chain"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            CHAIN,
            COUNT(DISTINCT PROTOCOL_SLUG) as PROTOCOL_COUNT,
            SUM(TVL) as TOTAL_TVL
        FROM PROTOCOL_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
            AND CHAIN IS NOT NULL
        GROUP BY CHAIN
        ORDER BY TOTAL_TVL DESC
        LIMIT 30
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
        st.error(f"Error fetching protocol by chain: {e}")
        return pd.DataFrame()


def render():
    """Render DeFi chains analytics page"""
    st.title("⛓️ DeFi Chains Analytics")
    st.markdown("Compare blockchain ecosystems by Total Value Locked and protocol activity")
    
    # Fetch data
    with st.spinner("Loading chain data..."):
        df = get_chain_data()
        protocol_df = get_protocol_by_chain()
    
    if df.empty:
        st.warning("No chain data available. Run the DeFi ingestion service first.")
        st.code("python -m src.server", language="bash")
        return
    
    # Summary metrics
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_tvl = df['TVL'].sum() / 1e9
        st.metric("Total Chains TVL", f"${total_tvl:.2f}B")
    
    with col2:
        chain_count = len(df)
        st.metric("Chains Tracked", f"{chain_count}")
    
    with col3:
        top_chain = df.iloc[0]['CHAIN_NAME'] if not df.empty else "N/A"
        top_tvl = df.iloc[0]['TVL'] / 1e9 if not df.empty else 0
        st.metric("Top Chain", f"{top_chain}", f"${top_tvl:.2f}B")
    
    with col4:
        avg_change = df['CHANGE_7D'].mean()
        st.metric("Avg 7D Change", f"{avg_change:.2f}%", delta=f"{avg_change:.2f}%")
    
    st.markdown("---")
    
    # Top Chains Bar Chart
    st.subheader("📊 Top Blockchains by TVL")
    
    top_n = st.slider("Show Top N Chains", min_value=10, max_value=50, value=20, step=5)
    chart_df = df.head(top_n).copy()
    chart_df['TVL_B'] = chart_df['TVL'] / 1e9
    chart_df = chart_df.sort_values('TVL', ascending=True)  # Ascending for horizontal bar
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=chart_df['CHAIN_NAME'],
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
        title="Chain TVL Rankings (Color = 7-Day Change %)",
        xaxis_title="Total Value Locked (Billions USD)",
        yaxis_title="Blockchain",
        height=max(400, len(chart_df) * 25),
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Chain Dominance
    st.subheader("🥧 Chain Dominance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 10 chains pie chart
        top_10 = df.head(10).copy()
        top_10['TVL_B'] = top_10['TVL'] / 1e9
        
        # Add "Others" category
        others_tvl = df[10:]['TVL'].sum() / 1e9 if len(df) > 10 else 0
        if others_tvl > 0:
            others_row = pd.DataFrame({
                'CHAIN_NAME': ['Others'],
                'TVL_B': [others_tvl]
            })
            pie_df = pd.concat([top_10[['CHAIN_NAME', 'TVL_B']], others_row], ignore_index=True)
        else:
            pie_df = top_10[['CHAIN_NAME', 'TVL_B']]
        
        fig_pie = px.pie(
            pie_df,
            values='TVL_B',
            names='CHAIN_NAME',
            title='Top 10 Chains Market Share',
            hole=0.4
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # L1 vs L2 analysis (simplified categorization)
        l2_chains = ['Arbitrum', 'Optimism', 'Base', 'Polygon', 'zkSync Era', 'Starknet', 
                     'Scroll', 'Linea', 'Metis', 'Blast', 'Mode', 'Mantle']
        
        df_copy = df.copy()
        df_copy['Type'] = df_copy['CHAIN_NAME'].apply(
            lambda x: 'Layer 2' if x in l2_chains else 'Layer 1 & Others'
        )
        
        type_dist = df_copy.groupby('Type')['TVL'].sum().reset_index()
        type_dist['TVL_B'] = type_dist['TVL'] / 1e9
        
        fig_type = px.pie(
            type_dist,
            values='TVL_B',
            names='Type',
            title='L1 vs L2 TVL Distribution',
            hole=0.4,
            color='Type',
            color_discrete_map={'Layer 1 & Others': '#1f77b4', 'Layer 2': '#ff7f0e'}
        )
        fig_type.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_type, use_container_width=True)
    
    # Protocol Activity by Chain
    if not protocol_df.empty:
        st.subheader("🔗 Protocol Activity by Chain")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Protocols per chain
            top_chains_protocols = protocol_df.head(15).copy()
            
            fig_protocols = go.Figure()
            fig_protocols.add_trace(go.Bar(
                x=top_chains_protocols['CHAIN'],
                y=top_chains_protocols['PROTOCOL_COUNT'],
                text=top_chains_protocols['PROTOCOL_COUNT'],
                textposition='outside',
                marker_color='lightblue'
            ))
            
            fig_protocols.update_layout(
                title="Number of Protocols per Chain",
                xaxis_title="Chain",
                yaxis_title="Protocol Count",
                height=400,
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig_protocols, use_container_width=True)
        
        with col2:
            # TVL vs Protocol count scatter
            scatter_df = protocol_df.copy()
            scatter_df['TVL_B'] = scatter_df['TOTAL_TVL'] / 1e9
            
            fig_scatter = px.scatter(
                scatter_df,
                x='PROTOCOL_COUNT',
                y='TVL_B',
                size='TVL_B',
                hover_name='CHAIN',
                title='TVL vs Protocol Count',
                labels={'PROTOCOL_COUNT': 'Number of Protocols', 'TVL_B': 'TVL (Billions USD)'},
                height=400
            )
            
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Detailed Chain Table
    st.subheader("📋 Chain Details")
    
    # Format dataframe for display
    display_df = df.copy()
    display_df['TVL'] = display_df['TVL'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notna(x) else "N/A")
    display_df['TVL_PREV_DAY'] = display_df['TVL_PREV_DAY'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notna(x) else "N/A")
    display_df['TVL_PREV_WEEK'] = display_df['TVL_PREV_WEEK'].apply(lambda x: f"${x/1e9:.2f}B" if pd.notna(x) else "N/A")
    display_df['CHANGE_1D'] = display_df['CHANGE_1D'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    display_df['CHANGE_7D'] = display_df['CHANGE_7D'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
    
    # Select columns
    cols_to_show = ['CHAIN_NAME', 'TVL', 'TVL_PREV_DAY', 'CHANGE_1D', 'CHANGE_7D', 'TOKEN_SYMBOL']
    display_df = display_df[cols_to_show]
    display_df.columns = ['Chain', 'TVL', 'TVL (24h ago)', '1D Change', '7D Change', 'Token']
    
    st.dataframe(
        display_df,
        use_container_width=True,
        height=500
    )
    
    # Top Gainers/Losers
    st.markdown("---")
    st.subheader("📈 Top Chain Performers (7 Days)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🚀 Top Gaining Chains")
        gainers = df.nlargest(5, 'CHANGE_7D')[['CHAIN_NAME', 'CHANGE_7D', 'TVL']]
        gainers['TVL'] = gainers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
        gainers['CHANGE_7D'] = gainers['CHANGE_7D'].apply(lambda x: f"+{x:.2f}%")
        gainers.columns = ['Chain', '7D Change', 'TVL']
        st.dataframe(gainers, hide_index=True, use_container_width=True)
    
    with col2:
        st.markdown("#### 📉 Declining Chains")
        losers = df.nsmallest(5, 'CHANGE_7D')[['CHAIN_NAME', 'CHANGE_7D', 'TVL']]
        losers['TVL'] = losers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
        losers['CHANGE_7D'] = losers['CHANGE_7D'].apply(lambda x: f"{x:.2f}%")
        losers.columns = ['Chain', '7D Change', 'TVL']
        st.dataframe(losers, hide_index=True, use_container_width=True)
    
    # Last updated
    if not df.empty and 'TIMESTAMP' in df.columns:
        last_update = df['TIMESTAMP'].max()
        st.caption(f"Last updated: {last_update}")
