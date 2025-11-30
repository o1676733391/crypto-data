import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def main():
    st.title("💰 Stablecoin Analytics")
    st.markdown("Monitor stablecoin market caps, supply, and peg stability")
    
    try:
        df = pd.read_csv("snowflake_export/table__DEFI_STABLECOINS.csv")
        
        # Convert timestamp (handles timezone offsets)
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        
        # Calculate metrics
        total_market_cap = df['MARKET_CAP'].sum() if 'MARKET_CAP' in df.columns else 0
        num_stablecoins = df['SYMBOL'].nunique() if 'SYMBOL' in df.columns else 0
        total_supply = df['CIRCULATING_SUPPLY'].sum() if 'CIRCULATING_SUPPLY' in df.columns else 0
        
        # Display overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Market Cap", f"${total_market_cap:,.0f}")
        with col2:
            st.metric("Stablecoins Tracked", f"{num_stablecoins}")
        with col3:
            st.metric("Total Supply", f"{total_supply:,.0f}")
        with col4:
            avg_price = df['PRICE'].mean() if 'PRICE' in df.columns else 1.0
            st.metric("Avg Price", f"${avg_price:.4f}")
        
        st.divider()
        
        # Sidebar filters
        st.sidebar.header("Filters")
        if 'SYMBOL' in df.columns:
            all_coins = sorted(df['SYMBOL'].unique().tolist())
            selected_coins = st.sidebar.multiselect(
                "Select Stablecoins",
                all_coins,
                default=all_coins[:5] if len(all_coins) > 5 else all_coins
            )
        else:
            selected_coins = []
        
        # Market Cap Distribution
        if 'MARKET_CAP' in df.columns and 'SYMBOL' in df.columns:
            st.subheader("📊 Market Cap Distribution")
            
            market_cap_by_coin = df.groupby('SYMBOL')['MARKET_CAP'].max().sort_values(ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Pie chart
                fig_pie = go.Figure(data=[go.Pie(
                    labels=market_cap_by_coin.index,
                    values=market_cap_by_coin.values,
                    hole=0.4,
                    textinfo='label+percent',
                    textposition='auto'
                )])
                fig_pie.update_layout(
                    title="Stablecoin Market Share",
                    height=400
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                # Bar chart
                fig_bar = go.Figure(data=[
                    go.Bar(
                        x=market_cap_by_coin.index,
                        y=market_cap_by_coin.values,
                        marker=dict(
                            color=market_cap_by_coin.values,
                            colorscale='Greens',
                            showscale=True
                        ),
                        text=[f'${v:,.0f}' for v in market_cap_by_coin.values],
                        textposition='auto'
                    )
                ])
                fig_bar.update_layout(
                    title="Market Cap by Stablecoin",
                    xaxis_title="Stablecoin",
                    yaxis_title="Market Cap (USD)",
                    height=400
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        
        st.divider()
        
        # Supply Trends
        if selected_coins and 'TIMESTAMP' in df.columns and 'CIRCULATING_SUPPLY' in df.columns:
            st.subheader("📈 Supply Trends Over Time")
            
            df_filtered = df[df['SYMBOL'].isin(selected_coins)].sort_values('TIMESTAMP')
            
            fig_supply = go.Figure()
            for coin in selected_coins:
                coin_data = df_filtered[df_filtered['SYMBOL'] == coin]
                if not coin_data.empty:
                    fig_supply.add_trace(go.Scatter(
                        x=coin_data['TIMESTAMP'],
                        y=coin_data['CIRCULATING_SUPPLY'],
                        mode='lines+markers',
                        name=coin,
                        line=dict(width=2),
                        marker=dict(size=6)
                    ))
            
            fig_supply.update_layout(
                title="Circulating Supply Trends",
                xaxis_title="Date",
                yaxis_title="Circulating Supply",
                height=500,
                hovermode='x unified'
            )
            st.plotly_chart(fig_supply, use_container_width=True)
        
        st.divider()
        
        # Peg Deviation Analysis
        if 'PRICE' in df.columns and 'SYMBOL' in df.columns:
            st.subheader("🎯 Peg Deviation Analysis")
            
            df_latest = df.sort_values('TIMESTAMP').groupby('SYMBOL').last().reset_index()
            df_latest['Deviation_from_1USD'] = ((df_latest['PRICE'] - 1.0) * 100).round(4)
            
            # Sort by absolute deviation
            df_latest['Abs_Deviation'] = df_latest['Deviation_from_1USD'].abs()
            df_latest = df_latest.sort_values('Abs_Deviation', ascending=False)
            
            fig_deviation = go.Figure(data=[
                go.Bar(
                    x=df_latest['SYMBOL'],
                    y=df_latest['Deviation_from_1USD'],
                    marker=dict(
                        color=df_latest['Deviation_from_1USD'],
                        colorscale='RdYlGn',
                        cmid=0,
                        showscale=True,
                        cmin=-1,
                        cmax=1
                    ),
                    text=[f'{v:+.4f}%' for v in df_latest['Deviation_from_1USD']],
                    textposition='auto'
                )
            ])
            fig_deviation.update_layout(
                title="Deviation from $1.00 Peg (%)",
                xaxis_title="Stablecoin",
                yaxis_title="Deviation (%)",
                height=400,
                shapes=[dict(
                    type='line',
                    x0=-0.5,
                    x1=len(df_latest)-0.5,
                    y0=0,
                    y1=0,
                    line=dict(color='black', width=2, dash='dash')
                )]
            )
            st.plotly_chart(fig_deviation, use_container_width=True)
            
            # Peg stability table
            st.subheader("📋 Current Peg Stability")
            peg_table = df_latest[['SYMBOL', 'PRICE', 'Deviation_from_1USD', 'MARKET_CAP']].copy()
            peg_table['PRICE'] = peg_table['PRICE'].apply(lambda x: f'${x:.6f}')
            peg_table['Deviation_from_1USD'] = peg_table['Deviation_from_1USD'].apply(lambda x: f'{x:+.4f}%')
            peg_table['MARKET_CAP'] = peg_table['MARKET_CAP'].apply(lambda x: f'${x:,.0f}')
            peg_table.columns = ['Symbol', 'Current Price', 'Deviation %', 'Market Cap']
            st.dataframe(peg_table, use_container_width=True, hide_index=True)
        
        st.divider()
        
        # Detailed Data
        with st.expander("🔍 View All Stablecoin Data"):
            st.dataframe(df, use_container_width=True)
    
    except FileNotFoundError:
        st.error("❌ Data file not found. Please ensure 'snowflake_export/table__DEFI_STABLECOINS.csv' exists.")
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")

if __name__ == "__main__":
    main()
