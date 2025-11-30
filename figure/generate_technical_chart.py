import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_technical_chart():
    candles_path = r'd:\postgresql\crypto-data\snowflake_export\table__CANDLES_DAILY.csv'
    indicators_path = r'd:\postgresql\crypto-data\snowflake_export\table__TECHNICAL_INDICATORS.csv'
    output_dir = r'd:\postgresql\crypto-data\figure'
    symbol = 'BTCUSDT'
    
    if not os.path.exists(candles_path) or not os.path.exists(indicators_path):
        print("Error: Input CSV files not found.")
        return

    print(f"Loading data for {symbol}...")
    
    # Load Candles (Price)
    try:
        df_candles = pd.read_csv(candles_path)
        df_candles.columns = df_candles.columns.str.strip()
        df_candles['CANDLE_TIME'] = pd.to_datetime(df_candles['CANDLE_TIME'], errors='coerce')
        df_candles = df_candles[df_candles['SYMBOL'] == symbol].copy()
        df_candles = df_candles.sort_values('CANDLE_TIME')
        df_candles = df_candles.set_index('CANDLE_TIME')
    except Exception as e:
        print(f"Error reading candles CSV: {e}")
        return

    # Load Indicators (RSI, MACD)
    try:
        df_ind = pd.read_csv(indicators_path)
        df_ind.columns = df_ind.columns.str.strip()
        df_ind['EXCHANGE_TS'] = pd.to_datetime(df_ind['EXCHANGE_TS'], errors='coerce')
        df_ind = df_ind[df_ind['SYMBOL'] == symbol].copy()
        
        # Indicators might be more granular (1m) than daily candles. 
        # For this chart, we should try to align them. 
        # If indicators are high frequency, we might want to resample or just take the last value per day to match candles.
        # Let's inspect the frequency first.
        # Assuming daily candles, let's resample indicators to daily (last value) to match.
        df_ind = df_ind.sort_values('EXCHANGE_TS')
        df_ind = df_ind.set_index('EXCHANGE_TS')
        df_ind_daily = df_ind.resample('D').last()
        
    except Exception as e:
        print(f"Error reading indicators CSV: {e}")
        return

    # Merge Data
    # Align on index (Date)
    # Ensure both have same timezone info or remove it
    df_candles.index = df_candles.index.tz_localize(None)
    df_ind_daily.index = df_ind_daily.index.tz_localize(None)
    
    df_merged = df_candles.join(df_ind_daily, how='inner', lsuffix='_candle', rsuffix='_ind')
    
    if df_merged.empty:
        print("Error: No overlapping data found between Candles and Indicators.")
        return

    print(f"Data merged. Records: {len(df_merged)}")

    # Plotting
    plt.style.use('ggplot')
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), sharex=True, gridspec_kw={'height_ratios': [3, 1, 1]})
    
    # Panel 1: Price
    ax1.plot(df_merged.index, df_merged['CLOSE'], label='Price (Close)', color='black', linewidth=1.5)
    ax1.set_title(f"{symbol} Price, RSI, and MACD", fontsize=16, fontweight='bold')
    ax1.set_ylabel("Price (USD)")
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.5)
    
    # Panel 2: RSI
    ax2.plot(df_merged.index, df_merged['RSI_14'], label='RSI (14)', color='purple', linewidth=1.2)
    ax2.axhline(70, color='red', linestyle='--', linewidth=0.8, alpha=0.7)
    ax2.axhline(30, color='green', linestyle='--', linewidth=0.8, alpha=0.7)
    ax2.set_ylabel("RSI")
    ax2.set_ylim(0, 100)
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.5)
    
    # Panel 3: MACD
    ax3.plot(df_merged.index, df_merged['MACD'], label='MACD', color='blue', linewidth=1.2)
    ax3.plot(df_merged.index, df_merged['MACD_SIGNAL'], label='Signal', color='orange', linewidth=1.2)
    
    # Histogram
    hist_color = ['green' if x >= 0 else 'red' for x in (df_merged['MACD'] - df_merged['MACD_SIGNAL'])]
    ax3.bar(df_merged.index, (df_merged['MACD'] - df_merged['MACD_SIGNAL']), color=hist_color, alpha=0.3, width=1.0)
    
    ax3.set_ylabel("MACD")
    ax3.set_xlabel("Date")
    ax3.legend(loc='upper left')
    ax3.grid(True, linestyle='--', alpha=0.5)
    
    # Format x-axis
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    fig.autofmt_xdate()
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'technical_indicators_chart.png')
    plt.savefig(output_path, dpi=300)
    print(f"Chart saved to {output_path}")
    plt.close(fig)

if __name__ == "__main__":
    generate_technical_chart()
