import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_charts():
    csv_path = r'd:\postgresql\crypto-data\snowflake_export\table__CHAIN_TVL.csv'
    output_dir = r'd:\postgresql\crypto-data\figure'
    
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    print("Loading data...")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    # Clean and parse data
    # Ensure column names are stripped of whitespace
    df.columns = df.columns.str.strip()
    
    if 'TIMESTAMP' not in df.columns or 'TVL' not in df.columns or 'CHAIN_NAME' not in df.columns:
        print(f"Error: Missing required columns. Found: {df.columns}")
        return

    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    df['TVL'] = pd.to_numeric(df['TVL'], errors='coerce')
    df = df.dropna(subset=['TVL', 'TIMESTAMP'])
    
    # Sort by date
    df = df.sort_values('TIMESTAMP')
    
    if df.empty:
        print("No valid data found after cleaning.")
        return

    # --- 1. Identify Top Chains (Current Snapshot) ---
    latest_date = df['TIMESTAMP'].max()
    print(f"Latest snapshot date: {latest_date}")
    
    # Get data for the latest timestamp (or close to it)
    # Since timestamps might be slightly different, let's take the last 24 hours or just the last entry per chain
    # Best approach: Group by CHAIN_NAME and take the last record
    latest_snapshot = df.sort_values('TIMESTAMP').groupby('CHAIN_NAME').tail(1)
    latest_snapshot = latest_snapshot.sort_values('TVL', ascending=False)
    
    # Define priority chains to highlight/track explicitly
    priority_chains = ['Ethereum', 'Arbitrum', 'Optimism', 'Solana', 'BSC', 'Base', 'Tron', 'Bitcoin']
    
    # Get top chains by TVL
    top_n = 10
    top_chains_list = latest_snapshot.head(top_n)['CHAIN_NAME'].tolist()
    
    # Union with priority chains (if they exist in the data)
    existing_chains = df['CHAIN_NAME'].unique()
    final_top_chains = list(set(top_chains_list) | (set(priority_chains) & set(existing_chains)))
    
    print(f"Tracking chains: {final_top_chains}")
    
    # --- 2. Data Processing for Time Series (Area Chart) ---
    print("Processing time series data...")
    # Create a copy for time series processing
    df_ts = df.copy()
    
    # Normalize timestamps to Day to aggregate correctly
    # Use 'Dt' to avoid overwriting original if needed, but here we can overwrite
    df_ts['Date'] = df_ts['TIMESTAMP'].dt.floor('D')
    
    # Group by Date and Chain, take the last TVL of the day
    df_daily = df_ts.groupby(['Date', 'CHAIN_NAME'])['TVL'].last().reset_index()
    
    # Pivot: Date x Chain -> TVL
    df_pivot = df_daily.pivot(index='Date', columns='CHAIN_NAME', values='TVL')
    
    # Fill missing values: forward fill first (carry over TVL), then fill remaining with 0
    df_pivot = df_pivot.ffill().fillna(0)
    
    # Filter for top chains + Others
    # Identify columns that are NOT in final_top_chains
    other_cols = [c for c in df_pivot.columns if c not in final_top_chains]
    
    # Create 'Others' column
    if other_cols:
        df_pivot['Others'] = df_pivot[other_cols].sum(axis=1)
    else:
        df_pivot['Others'] = 0
        
    # Keep only relevant columns
    cols_to_keep = [c for c in final_top_chains if c in df_pivot.columns] + ['Others']
    df_area_data = df_pivot[cols_to_keep].copy()
    
    # Sort columns by latest TVL for better visualization in Area chart
    latest_area_values = df_area_data.iloc[-1]
    sorted_cols = sorted(cols_to_keep, key=lambda x: latest_area_values.get(x, 0), reverse=True)
    # Move Others to end if present
    if 'Others' in sorted_cols:
        sorted_cols.remove('Others')
        sorted_cols.append('Others')
        
    df_area_data = df_area_data[sorted_cols]

    # --- 3. Generate Charts ---
    
    # Common Style
    plt.style.use('ggplot')
    
    # A. Pie Chart (Current Dominance)
    print("Generating Pie Chart...")
    
    # Prepare data from latest_snapshot
    # We need to group the non-top chains into Others for the Pie chart as well
    pie_data = latest_snapshot.copy()
    pie_data['Display_Chain'] = pie_data['CHAIN_NAME'].apply(lambda x: x if x in final_top_chains else 'Others')
    
    pie_grouped = pie_data.groupby('Display_Chain')['TVL'].sum().sort_values(ascending=False)
    
    # Ensure Others is at the bottom
    if 'Others' in pie_grouped.index:
        others_val = pie_grouped['Others']
        pie_grouped = pie_grouped.drop('Others')
        pie_grouped['Others'] = others_val
        
    labels = pie_grouped.index
    sizes = pie_grouped.values
    
    fig1, ax1 = plt.subplots(figsize=(12, 10))
    
    # Custom colors mapping
    chain_colors = {
        'Ethereum': '#627EEA', 'BSC': '#F3BA2F', 'Tron': '#FF0013', 'Solana': '#14F195',
        'Arbitrum': '#2D374B', 'Optimism': '#FF0420', 'Base': '#0052FF', 'Bitcoin': '#F7931A',
        'Polygon': '#8247E5', 'Avalanche': '#E84142', 'Others': '#999999'
    }
    colors = [chain_colors.get(l, None) for l in labels]
    
    explode = [0.05 if label == 'Ethereum' else 0 for label in labels]
    
    # Helper to hide small percentages and labels
    total_val = sum(sizes)
    def my_autopct(pct):
        return ('%1.1f%%' % pct) if pct > 3 else ''

    # Hide labels for small slices
    labels_display = [l if (s/total_val)*100 > 3 else '' for l, s in zip(labels, sizes)]
    
    wedges, texts, autotexts = ax1.pie(sizes, labels=labels_display, autopct=my_autopct, startangle=140, 
                                       pctdistance=0.85, explode=explode, shadow=False, colors=colors)
    
    # Add legend to the side
    ax1.legend(wedges, labels,
              title="Chains",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))
    
    # Donut style
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig1.gca().add_artist(centre_circle)
    
    plt.setp(texts, size=10, weight="bold")
    plt.setp(autotexts, size=9, weight="bold", color="black")
    
    total_tvl_b = sizes.sum() / 1e9
    ax1.set_title(f"Chain TVL Dominance (Total: ${total_tvl_b:.2f}B)", fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'chain_dominance_pie.png'), dpi=300)
    plt.close(fig1)
    
    # B. Bar Chart (Competitive Comparison)
    print("Generating Bar Chart...")
    fig2, ax2 = plt.subplots(figsize=(12, 8))
    
    # Use pie_grouped but exclude Others for the bar chart to focus on specific chains
    bar_data = pie_grouped.drop('Others', errors='ignore')
    bar_data = bar_data.sort_values(ascending=True) # Sort for horizontal bar (bottom to top)
    
    bar_labels = bar_data.index
    bar_values = bar_data.values / 1e9 # Billions
    
    bar_colors = [chain_colors.get(l, '#3498db') for l in bar_labels]
    
    bars = ax2.barh(bar_labels, bar_values, color=bar_colors)
    
    # Add value labels
    for bar in bars:
        width = bar.get_width()
        ax2.text(width, bar.get_y() + bar.get_height()/2, f' ${width:.2f}B', 
                 ha='left', va='center', fontweight='bold', fontsize=10)
        
    ax2.set_title("Top Chains by Total Value Locked (Billions USD)", fontsize=16, fontweight='bold')
    ax2.set_xlabel("TVL ($B)")
    ax2.grid(axis='x', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'chain_dominance_bar.png'), dpi=300)
    plt.close(fig2)
    
    # C. Stacked Area Chart (Capital Migration)
    print("Generating Stacked Area Chart...")
    fig3, ax3 = plt.subplots(figsize=(14, 8))
    
    # Convert to Billions
    df_area_plot = df_area_data / 1e9
    
    # Use consistent colors
    area_colors = [chain_colors.get(c, '#aaaaaa') for c in df_area_plot.columns]
    
    ax3.stackplot(df_area_plot.index, df_area_plot.T.values, labels=df_area_plot.columns, colors=area_colors, alpha=0.8)
    
    ax3.set_title("Evolution of Chain TVL (Capital Migration)", fontsize=16, fontweight='bold')
    ax3.set_ylabel("Total Value Locked (Billions USD)")
    ax3.set_xlabel("Date")
    ax3.legend(loc='upper left', fontsize=10, bbox_to_anchor=(1, 1))
    ax3.grid(True, linestyle='--', alpha=0.5)
    
    # Format x-axis
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig3.autofmt_xdate()
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'chain_tvl_area.png'), dpi=300)
    plt.close(fig3)
    
    print("All charts generated successfully.")

if __name__ == "__main__":
    generate_charts()
