"""
DeFi Market Overview
Combined crypto + DeFi market analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
sys.path.append('.')

from src.defi_snowflake_client import DefiSnowflakeWriter


def get_market_summary():
    """Get combined market summary"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        # DeFi Summary
        defi_query = """
        SELECT 
            COUNT(DISTINCT PROTOCOL_SLUG) as PROTOCOL_COUNT,
            SUM(TVL) as TOTAL_TVL,
            AVG(CHANGE_7D) as AVG_CHANGE_7D
        FROM PROTOCOL_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        """
        
        cursor.execute(defi_query)
        defi_data = cursor.fetchone()
        
        # Chain Summary
        chain_query = """
        SELECT 
            COUNT(DISTINCT CHAIN_NAME) as CHAIN_COUNT,
            SUM(TVL) as TOTAL_CHAIN_TVL
        FROM CHAIN_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        """
        
        cursor.execute(chain_query)
        chain_data = cursor.fetchone()
        
        cursor.close()
        writer.close()
        
        return {
            'protocols': defi_data[0] if defi_data else 0,
            'total_tvl': defi_data[1] if defi_data else 0,
            'avg_change': defi_data[2] if defi_data else 0,
            'chains': chain_data[0] if chain_data else 0,
            'chain_tvl': chain_data[1] if chain_data else 0
        }
        
    except Exception as e:
        st.error(f"Error fetching market summary: {e}")
        return None


def get_category_breakdown():
    """Get TVL breakdown by category"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            CATEGORY,
            COUNT(DISTINCT PROTOCOL_SLUG) as PROTOCOL_COUNT,
            SUM(TVL) as TOTAL_TVL,
            AVG(CHANGE_7D) as AVG_CHANGE_7D
        FROM PROTOCOL_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
            AND CATEGORY IS NOT NULL
        GROUP BY CATEGORY
        ORDER BY TOTAL_TVL DESC
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
        st.error(f"Error fetching category breakdown: {e}")
        return pd.DataFrame()


def get_top_movers():
    """Get biggest TVL movers"""
    try:
        writer = DefiSnowflakeWriter()
        conn = writer._get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            PROTOCOL_NAME,
            TVL,
            CHANGE_7D,
            CATEGORY,
            CHAIN
        FROM PROTOCOL_TVL
        WHERE TIMESTAMP >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
            AND CHANGE_7D IS NOT NULL
        ORDER BY ABS(CHANGE_7D) DESC
        LIMIT 20
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
        st.error(f"Error fetching movers: {e}")
        return pd.DataFrame()


def render():
    """Render DeFi market overview page"""
    st.title("🌐 DeFi Market Overview")
    st.markdown("Comprehensive view of the DeFi ecosystem")
    
    # Fetch data
    with st.spinner("Loading market data..."):
        summary = get_market_summary()
        category_df = get_category_breakdown()
        movers_df = get_top_movers()
    
    if not summary:
        st.warning("No market data available. Run the DeFi ingestion service first.")
        return
    
    # Market Summary Cards
    st.markdown("---")
    st.subheader("📊 Market Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total TVL", f"${summary['total_tvl']/1e9:.2f}B")
    
    with col2:
        st.metric("Protocols", f"{summary['protocols']:,}")
    
    with col3:
        st.metric("Blockchains", f"{summary['chains']}")
    
    with col4:
        avg_change = summary['avg_change']
        st.metric("Avg 7D Change", f"{avg_change:.2f}%", delta=f"{avg_change:.2f}%")
    
    with col5:
        market_cap_per_protocol = summary['total_tvl'] / summary['protocols'] if summary['protocols'] > 0 else 0
        st.metric("Avg Protocol TVL", f"${market_cap_per_protocol/1e6:.1f}M")
    
    st.markdown("---")
    
    # Category Analysis
    if not category_df.empty:
        st.subheader("📑 Category Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Category TVL Distribution
            cat_chart = category_df.copy()
            cat_chart['TVL_B'] = cat_chart['TOTAL_TVL'] / 1e9
            
            fig_cat = px.bar(
                cat_chart.head(15),
                x='CATEGORY',
                y='TVL_B',
                title='TVL by Category (Top 15)',
                labels={'TVL_B': 'TVL (Billions USD)', 'CATEGORY': 'Category'},
                color='AVG_CHANGE_7D',
                color_continuous_scale='RdYlGn',
                color_continuous_midpoint=0
            )
            fig_cat.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig_cat, use_container_width=True)
        
        with col2:
            # Category Protocol Count
            fig_count = px.bar(
                category_df.head(15),
                x='CATEGORY',
                y='PROTOCOL_COUNT',
                title='Number of Protocols by Category',
                labels={'PROTOCOL_COUNT': 'Protocol Count', 'CATEGORY': 'Category'},
                color='PROTOCOL_COUNT',
                color_continuous_scale='Blues'
            )
            fig_count.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig_count, use_container_width=True)
        
        # Category Performance Table
        st.markdown("#### Category Performance")
        
        display_cat = category_df.copy()
        display_cat['TOTAL_TVL'] = display_cat['TOTAL_TVL'].apply(lambda x: f"${x/1e9:.2f}B")
        display_cat['AVG_CHANGE_7D'] = display_cat['AVG_CHANGE_7D'].apply(lambda x: f"{x:.2f}%")
        display_cat.columns = ['Category', 'Protocols', 'Total TVL', 'Avg 7D Change']
        
        st.dataframe(display_cat.head(20), use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Market Movers
    if not movers_df.empty:
        st.subheader("🔥 Biggest Movers (7 Days)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top Gainers
            st.markdown("#### 🚀 Top Gainers")
            gainers = movers_df[movers_df['CHANGE_7D'] > 0].head(10)
            
            if not gainers.empty:
                display_gainers = gainers.copy()
                display_gainers['TVL'] = display_gainers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
                display_gainers['CHANGE_7D'] = display_gainers['CHANGE_7D'].apply(lambda x: f"+{x:.2f}%")
                display_gainers = display_gainers[['PROTOCOL_NAME', 'CHANGE_7D', 'TVL', 'CATEGORY']]
                display_gainers.columns = ['Protocol', 'Change', 'TVL', 'Category']
                
                st.dataframe(display_gainers, hide_index=True, use_container_width=True)
            else:
                st.info("No significant gainers in the past 7 days")
        
        with col2:
            # Top Losers
            st.markdown("#### 📉 Top Losers")
            losers = movers_df[movers_df['CHANGE_7D'] < 0].head(10)
            
            if not losers.empty:
                display_losers = losers.copy()
                display_losers['TVL'] = display_losers['TVL'].apply(lambda x: f"${x/1e9:.2f}B")
                display_losers['CHANGE_7D'] = display_losers['CHANGE_7D'].apply(lambda x: f"{x:.2f}%")
                display_losers = display_losers[['PROTOCOL_NAME', 'CHANGE_7D', 'TVL', 'CATEGORY']]
                display_losers.columns = ['Protocol', 'Change', 'TVL', 'Category']
                
                st.dataframe(display_losers, hide_index=True, use_container_width=True)
            else:
                st.info("No significant losers in the past 7 days")
    
    st.markdown("---")
    
    # Ecosystem Health Indicators
    st.subheader("💊 Ecosystem Health")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Market sentiment based on avg change
        avg_change = summary['avg_change']
        if avg_change > 5:
            sentiment = "🟢 Very Bullish"
            color = "green"
        elif avg_change > 0:
            sentiment = "🟡 Bullish"
            color = "orange"
        elif avg_change > -5:
            sentiment = "🟠 Bearish"
            color = "orange"
        else:
            sentiment = "🔴 Very Bearish"
            color = "red"
        
        st.markdown(f"**Market Sentiment**")
        st.markdown(f"### {sentiment}")
    
    with col2:
        # Diversity Score (number of categories with significant TVL)
        if not category_df.empty:
            significant_categories = len(category_df[category_df['TOTAL_TVL'] > 1e9])
            diversity_score = min(100, (significant_categories / 20) * 100)
            
            st.markdown(f"**Ecosystem Diversity**")
            st.markdown(f"### {diversity_score:.0f}/100")
            st.caption(f"{significant_categories} categories with >$1B TVL")
    
    with col3:
        # Growth Momentum
        if not category_df.empty:
            growing_categories = len(category_df[category_df['AVG_CHANGE_7D'] > 0])
            total_categories = len(category_df)
            growth_pct = (growing_categories / total_categories * 100) if total_categories > 0 else 0
            
            st.markdown(f"**Growth Momentum**")
            st.markdown(f"### {growth_pct:.0f}%")
            st.caption(f"{growing_categories}/{total_categories} categories growing")
    
    # Footer
    st.markdown("---")
    st.caption("Data source: DefiLlama API • Updates every 60 minutes")
