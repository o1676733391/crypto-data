import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import os
import numpy as np

def generate_candlestick_chart():
    csv_path = r'd:\postgresql\crypto-data\snowflake_export\table__CANDLES_15MIN.csv'
    output_dir = r'd:\postgresql\crypto-data\figure'
    symbol = 'BTCUSDT'
    
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    print(f"Loading data for {symbol}...")
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        df['CANDLE_TIME'] = pd.to_datetime(df['CANDLE_TIME'], errors='coerce')
        
        # Filter for Symbol
        df = df[df['SYMBOL'] == symbol].copy()
        df = df.sort_values('CANDLE_TIME')
        
        # Convert numeric columns
        for col in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME_QUOTE']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print(f"Volume Quote range: {df['VOLUME_QUOTE'].min():.2f} to {df['VOLUME_QUOTE'].max():.2f}")
        print(f"Volume Quote mean: {df['VOLUME_QUOTE'].mean():.2f}")
        
        # Calculate Moving Averages (before slicing)
        df['MA7'] = df['CLOSE'].rolling(window=7).mean()
        df['MA25'] = df['CLOSE'].rolling(window=25).mean()
        df['MA99'] = df['CLOSE'].rolling(window=99).mean()
        
        # Filter for November 27, 2025 only, starting from 03:00 (skip first 3 hours)
        target_date = pd.Timestamp('2025-11-27').date()
        start_time = pd.Timestamp('2025-11-27 03:00:00')
        
        plot_df = df[(df['CANDLE_TIME'].dt.date == target_date) & (df['CANDLE_TIME'] >= start_time)].copy()
        
        if plot_df.empty:
            print(f"No data found for symbol on {target_date} after 03:00.")
            return
            
        print(f"Processing {len(plot_df)} records from {plot_df['CANDLE_TIME'].min()} to {plot_df['CANDLE_TIME'].max()}...")
        print(f"Plot Volume Quote range: {plot_df['VOLUME_QUOTE'].min():.2f} to {plot_df['VOLUME_QUOTE'].max():.2f}")
        
    except Exception as e:
        print(f"Error reading CSV: {e}")
        import traceback
        traceback.print_exc()
        return

    # Prepare Plot - Light Theme to match reference
    plt.style.use('default') 
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 8), sharex=True, gridspec_kw={'height_ratios': [3, 1]})
    fig.patch.set_facecolor('white')
    ax1.set_facecolor('white')
    ax2.set_facecolor('white')
    
    # Plot Candlesticks - Proper Japanese Candlestick with High/Low/Open/Close
    x_vals = range(len(plot_df))
    
    bullish_count = 0
    bearish_count = 0
    doji_count = 0
    
    for i, (index, row) in enumerate(plot_df.iterrows()):
        open_p = float(row['OPEN'])
        close_p = float(row['CLOSE'])
        high_p = float(row['HIGH'])
        low_p = float(row['LOW'])
        
        # Validate data
        if pd.isna(open_p) or pd.isna(close_p) or pd.isna(high_p) or pd.isna(low_p):
            continue
            
        # Binance Colors: Green (bullish) #0ECB81, Red (bearish) #F6465D
        # Bullish: CLOSE > OPEN (price went UP)
        # Bearish: CLOSE < OPEN (price went DOWN)
        # Doji: CLOSE == OPEN (no change, treat as bullish by convention)
        if close_p > open_p:
            is_bullish = True
            color = '#0ECB81'  # Green
            bullish_count += 1
        elif close_p < open_p:
            is_bullish = False
            color = '#F6465D'  # Red
            bearish_count += 1
        else:  # close_p == open_p (Doji)
            is_bullish = True  # Treat doji as bullish by convention
            color = '#0ECB81'
            doji_count += 1
        
        # Draw High-Low line (wick/shadow) - from LOW to HIGH
        ax1.plot([i, i], [low_p, high_p], color=color, linewidth=1.2, solid_capstyle='round', zorder=1)
        
        # Draw Open-Close body (candle body)
        body_height = abs(close_p - open_p)
        
        # For very small bodies (doji), make a minimum visible line
        if body_height < (high_p - low_p) * 0.01:
            body_height = (high_p - low_p) * 0.01
        
        # Bottom of the body is the minimum of open and close
        body_bottom = min(open_p, close_p)
        
        # Draw filled rectangle for the body
        rect = Rectangle((i - 0.35, body_bottom), 0.7, body_height, 
                         facecolor=color, edgecolor=color, linewidth=0.5, zorder=2)
        ax1.add_patch(rect)

    print(f"Candle stats: {bullish_count} bullish (green), {bearish_count} bearish (red), {doji_count} doji (neutral)")

    # Plot Moving Averages
    ax1.plot(x_vals, plot_df['MA7'], label='MA7', color='#F0B90B', linewidth=1.5, alpha=0.8, zorder=3)
    ax1.plot(x_vals, plot_df['MA25'], label='MA25', color='#C000C0', linewidth=1.5, alpha=0.8, zorder=3)
    ax1.plot(x_vals, plot_df['MA99'], label='MA99', color='#0000FF', linewidth=1.5, alpha=0.6, zorder=3)

    ax1.set_title(f"{symbol} 15-Minute Chart (Nov 27, 2025 from 03:00)", fontsize=14, fontweight='bold', color='black')
    ax1.set_ylabel("Price (USD)", color='black')
    ax1.legend(loc='upper left', frameon=False, fontsize=10)
    ax1.grid(True, linestyle='--', alpha=0.15, color='gray')
    ax1.tick_params(axis='x', colors='black')
    ax1.tick_params(axis='y', colors='black')
    
    # Plot Volume using VOLUME_QUOTE
    volume_data = plot_df['VOLUME_QUOTE'].values
    
    # Determine color for each bar based on CLOSE vs OPEN
    colors = []
    for _, row in plot_df.iterrows():
        if pd.notna(row['CLOSE']) and pd.notna(row['OPEN']):
            # Same logic as candles
            if row['CLOSE'] > row['OPEN']:
                colors.append('#0ECB81')  # Green for bullish
            elif row['CLOSE'] < row['OPEN']:
                colors.append('#F6465D')  # Red for bearish
            else:  # Doji
                colors.append('#0ECB81')  # Green for doji
        else:
            colors.append('#CCCCCC')  # Gray for missing data
    
    # Plot volume bars
    bars = ax2.bar(x_vals, volume_data, color=colors, alpha=0.7, width=0.8)
    
    ax2.set_ylabel("Volume (USD)", color='black', fontsize=10)
    ax2.grid(True, linestyle='--', alpha=0.15, color='gray')
    ax2.tick_params(axis='x', colors='black')
    ax2.tick_params(axis='y', colors='black')
    
    # Format volume axis
    ax2.ticklabel_format(style='plain', axis='y')
    
    # Format X-Axis
    tick_step = max(1, len(x_vals) // 15)  # Show ~15 labels
    tick_indices = list(x_vals)[::tick_step]
    tick_labels = [plot_df.iloc[i]['CANDLE_TIME'].strftime('%H:%M') for i in tick_indices]
    
    ax2.set_xticks(tick_indices)
    ax2.set_xticklabels(tick_labels, rotation=0)
    ax2.set_xlabel("Time (UTC)", color='black', fontsize=10)
    ax2.set_xlim(-0.5, len(x_vals) - 0.5)

    plt.tight_layout()
    output_path = os.path.join(output_dir, 'candlestick_volume_chart.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Chart saved to {output_path}")
    plt.close(fig)

if __name__ == "__main__":
    generate_candlestick_chart()
