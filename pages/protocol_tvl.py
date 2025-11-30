import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

def main():
    st.title("🏦 DeFi Protocol TVL Explorer")
    st.markdown("Explore Total Value Locked (TVL) across DeFi protocols and chains")
    
    # Load data
    try:
        df = pd.read_csv("snowflake_export/table__PROTOCOL_TVL.csv")
        
        # Convert timestamp to datetime (handles timezone offsets manually)
        if 'TIMESTAMP' in df.columns:
            # Remove timezone offset for pandas compatibility
            df['TIMESTAMP'] = df['TIMESTAMP'].astype(str).str.replace(r'[-+]\d{2}:\d{2}$', '', regex=True)
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        
        # Calculate key metrics
        total_tvl = df['TVL'].sum()
        num_protocols = df['PROTOCOL_NAME'].nunique()
        num_chains = df['CHAIN'].apply(lambda x: len(eval(x)) if isinstance(x, str) and x.startswith('[') else 0).sum()
        avg_tvl = df['TVL'].mean()
        
        # Display overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total TVL", f"${total_tvl:,.0f}")
        with col2:
            st.metric("Protocols", f"{num_protocols}")
        with col3:
            st.metric("Avg TVL/Protocol", f"${avg_tvl:,.0f}")
        with col4:
            st.metric("Total Chains", f"{num_chains}")
        
        st.divider()
        
        # Sidebar filters
        st.sidebar.header("Filters")
        
        # Protocol filter
        all_protocols = sorted(df['PROTOCOL_NAME'].unique().tolist())
        selected_protocols = st.sidebar.multiselect(
            "Select Protocols",
            all_protocols,
            default=all_protocols[:5] if len(all_protocols) > 5 else all_protocols
        )
        
        # Category filter (if available)
        if 'CATEGORY' in df.columns:
            all_categories = sorted(df['CATEGORY'].dropna().unique().tolist())
            selected_categories = st.sidebar.multiselect(
                "Filter by Category",
                all_categories,
                default=all_categories
            )
            df = df[df['CATEGORY'].isin(selected_categories)]
        
        # Filter data
        if selected_protocols:
            df_filtered = df[df['PROTOCOL_NAME'].isin(selected_protocols)]
        else:
            df_filtered = df
        
        # Top Protocols Section
        st.subheader("📊 Top 10 Protocols by TVL")
        top_protocols = df.groupby('PROTOCOL_NAME')['TVL'].max().sort_values(ascending=False).head(10)
        
        fig_bar = go.Figure(data=[
            go.Bar(
                x=top_protocols.values,
                y=top_protocols.index,
                orientation='h',
                marker=dict(
                    color=top_protocols.values,
                    colorscale='Viridis',
                    showscale=True
                ),
                text=[f'${v:,.0f}' for v in top_protocols.values],
                textposition='auto',
            )
        ])
        fig_bar.update_layout(
            title="Top 10 Protocols by Total Value Locked",
            xaxis_title="TVL (USD)",
            yaxis_title="Protocol",
            height=500,
            showlegend=False
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        
        st.divider()
        
        # TVL Trends Section
        if 'TIMESTAMP' in df_filtered.columns and not df_filtered.empty:
            st.subheader("📈 TVL Trends Over Time")
            
            # Create line chart for selected protocols
            fig_trend = go.Figure()
            for protocol in selected_protocols:
                protocol_data = df_filtered[df_filtered['PROTOCOL_NAME'] == protocol].sort_values('TIMESTAMP')
                if not protocol_data.empty:
                    fig_trend.add_trace(go.Scatter(
                        x=protocol_data['TIMESTAMP'],
                        y=protocol_data['TVL'],
                        mode='lines+markers',
                        name=protocol,
                        line=dict(width=2),
                        marker=dict(size=6)
                    ))
            
            fig_trend.update_layout(
                title="TVL Trends for Selected Protocols",
                xaxis_title="Date",
                yaxis_title="TVL (USD)",
                height=500,
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
        st.divider()
        
        # Category Breakdown and Protocol Details
        col1, col2 = st.columns(2)
        
        with col1:
            if 'CATEGORY' in df.columns:
                st.subheader("🎯 TVL by Category")
                category_tvl = df.groupby('CATEGORY')['TVL'].sum().sort_values(ascending=False)
                
                fig_pie = go.Figure(data=[go.Pie(
                    labels=category_tvl.index,
                    values=category_tvl.values,
                    hole=0.3,
                    textinfo='label+percent',
                    textposition='auto'
                )])
                fig_pie.update_layout(
                    title="TVL Distribution by Category",
                    height=400
                )
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            st.subheader("📋 Protocol Details")
            # Show top 10 protocols table
            protocol_stats = df.groupby('PROTOCOL_NAME').agg({
                'TVL': 'max',
                'CATEGORY': 'first'
            }).sort_values('TVL', ascending=False).head(10)
            protocol_stats['TVL'] = protocol_stats['TVL'].apply(lambda x: f'${x:,.0f}')
            st.dataframe(protocol_stats, use_container_width=True)
        
        st.divider()
        
        # Detailed Data Table
        with st.expander("🔍 View Detailed Data"):
            st.dataframe(df_filtered, use_container_width=True)
        
    except FileNotFoundError:
        st.error("❌ Data file not found. Please ensure 'snowflake_export/table__PROTOCOL_TVL.csv' exists.")
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")

if __name__ == "__main__":
    main()
