import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import numpy as np

def generate_market_charts():
    csv_path = r'd:\postgresql\crypto-data\snowflake_export\table__CANDLES_DAILY.csv'
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
    df.columns = df.columns.str.strip()
    
    required_cols = ['CANDLE_TIME', 'SYMBOL', 'CLOSE', 'VOLUME_QUOTE']
    if not all(col in df.columns for col in required_cols):
        print(f"Error: Missing required columns. Found: {df.columns}")
        return

    df['CANDLE_TIME'] = pd.to_datetime(df['CANDLE_TIME'], errors='coerce')
    df['CLOSE'] = pd.to_numeric(df['CLOSE'], errors='coerce')
    df['VOLUME_QUOTE'] = pd.to_numeric(df['VOLUME_QUOTE'], errors='coerce')
    
    df = df.dropna(subset=['CANDLE_TIME', 'CLOSE', 'SYMBOL'])
    df = df.sort_values('CANDLE_TIME')
    
    if df.empty:
        print("No valid data found.")
        return

    # --- 1. Top Gainers & Losers (24h Change) ---
    print("Generating Gainers & Losers Chart...")
    
    latest_date = df['CANDLE_TIME'].max()
    target_date = latest_date - pd.Timedelta(days=1)
    
    print(f"Latest Date: {latest_date}")
    print(f"Target Date (24h ago): {target_date}")
    
    # Get latest prices
    df_latest = df[df['CANDLE_TIME'] == latest_date][['SYMBOL', 'CLOSE']].set_index('SYMBOL')
    df_latest.rename(columns={'CLOSE': 'Close_Latest'}, inplace=True)
    
    # Get prices 24h ago
    # We use a small window or exact match. Since it's daily data, exact match should work if data exists.
    df_prev = df[df['CANDLE_TIME'] == target_date][['SYMBOL', 'CLOSE']].set_index('SYMBOL')
    df_prev.rename(columns={'CLOSE': 'Close_Prev'}, inplace=True)
    
    # Merge
    df_change = df_latest.join(df_prev, how='inner')
    
    if df_change.empty:
        print("Warning: No matching data found for 24h change calculation (Latest vs Prev Day).")
        # Fallback: Try to find the 'previous available' record for each symbol if exact 24h match fails
        # This handles cases where data might have gaps
        print("Attempting fallback: Comparing Latest vs Previous Record per symbol...")
        df_sorted = df.sort_values(['SYMBOL', 'CANDLE_TIME'])
        latest_records = df_sorted.groupby('SYMBOL').tail(1)[['SYMBOL', 'CANDLE_TIME', 'CLOSE']]
        
        # Get the record before the last one
        prev_records = df_sorted.groupby('SYMBOL').apply(lambda x: x.iloc[-2] if len(x) > 1 else None).dropna()
        
        if not prev_records.empty:
             # Re-construct df_change
             latest_records = latest_records.set_index('SYMBOL')['CLOSE'].rename('Close_Latest')
             prev_records = prev_records.set_index('SYMBOL')['CLOSE'].rename('Close_Prev')
             df_change = pd.concat([latest_records, prev_records], axis=1, join='inner')
    
    if not df_change.empty:
        df_change['Change_Pct'] = ((df_change['Close_Latest'] - df_change['Close_Prev']) / df_change['Close_Prev']) * 100
        
        # Sort by change
        df_change = df_change.sort_values('Change_Pct', ascending=False)
        
        # Top 5 Gainers and Top 5 Losers
        top_gainers = df_change.head(5)
        top_losers = df_change.tail(5).sort_values('Change_Pct', ascending=True) # Sort losers to show largest drop first
        
        # Combine for plotting (Top Gainers first, then Losers reversed for visual balance if needed, or just separate)
        # Let's plot them together in one diverging bar chart
        
        # Prepare data for plotting
        plot_data = pd.concat([top_gainers, top_losers.sort_values('Change_Pct', ascending=False)])
        
        # Plot
        plt.style.use('ggplot')
        fig1, ax1 = plt.subplots(figsize=(12, 8))
        
        colors = ['g' if x >= 0 else 'r' for x in plot_data['Change_Pct']]
        bars = ax1.barh(plot_data.index, plot_data['Change_Pct'], color=colors)
        
        ax1.set_title(f"Top Gainers & Losers (24h Change) - {latest_date.date()}", fontsize=16, fontweight='bold')
        ax1.set_xlabel("Percentage Change (%)")
        ax1.axvline(0, color='black', linewidth=0.8)
        
        # Add value labels
        for bar in bars:
            width = bar.get_width()
            label_x_pos = width if width > 0 else width
            align = 'left' if width > 0 else 'right'
            ax1.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{width:.2f}%', 
                     ha=align, va='center', fontweight='bold', fontsize=10)
            
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'market_gainers_losers.png'), dpi=300)
        plt.close(fig1)
    else:
        print("Error: Could not calculate 24h change.")

    # --- 2. Price Comparison (Normalized) ---
    print("Generating Price Comparison Chart...")
    
    target_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
    df_price = df[df['SYMBOL'].isin(target_symbols)].copy()
    
    if not df_price.empty:
        # Pivot
        df_price_pivot = df_price.pivot(index='CANDLE_TIME', columns='SYMBOL', values='CLOSE')
        
        # Normalize to 100 at the start
        df_normalized = df_price_pivot / df_price_pivot.iloc[0] * 100
        
        fig2, ax2 = plt.subplots(figsize=(12, 8))
        
        for col in df_normalized.columns:
            ax2.plot(df_normalized.index, df_normalized[col], label=col, linewidth=2)
            
        ax2.set_title("Price Comparison (Normalized to 100)", fontsize=16, fontweight='bold')
        ax2.set_ylabel("Normalized Price")
        ax2.set_xlabel("Date")
        ax2.legend()
        ax2.grid(True, linestyle='--', alpha=0.5)
        
        # Format x-axis
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig2.autofmt_xdate()
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'market_price_comparison.png'), dpi=300)
        plt.close(fig2)
    else:
        print("No data found for price comparison symbols.")

    # --- 3. Volume Distribution ---
    print("Generating Volume Distribution Chart...")
    
    # Get volume for the latest date
    df_vol = df[df['CANDLE_TIME'] == latest_date][['SYMBOL', 'VOLUME_QUOTE']].copy()
    
    if not df_vol.empty:
        df_vol = df_vol.sort_values('VOLUME_QUOTE', ascending=False).head(10)
        
        fig3, ax3 = plt.subplots(figsize=(12, 8))
        
        # Convert to Billions/Millions
        # Check max volume to decide unit
        max_vol = df_vol['VOLUME_QUOTE'].max()
        if max_vol > 1e9:
            df_vol['Vol_Display'] = df_vol['VOLUME_QUOTE'] / 1e9
            unit = "Billions"
        else:
            df_vol['Vol_Display'] = df_vol['VOLUME_QUOTE'] / 1e6
            unit = "Millions"
            
        bars = ax3.bar(df_vol['SYMBOL'], df_vol['Vol_Display'], color='#3498db')
        
        ax3.set_title(f"Top 10 Assets by Trading Volume (24h) - {latest_date.date()}", fontsize=16, fontweight='bold')
        ax3.set_ylabel(f"Volume ({unit} USD)")
        ax3.set_xlabel("Asset")
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}', 
                     ha='center', va='bottom', fontsize=10)
            
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'market_volume_distribution.png'), dpi=300)
        plt.close(fig3)
    else:
        print("No volume data found for latest date.")

    print("All market charts generated.")

if __name__ == "__main__":
    generate_market_charts()
