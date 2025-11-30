import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import csv
from datetime import datetime
import os
from collections import defaultdict

def draw_tvl_trend_chart():
    csv_path = r'd:\postgresql\crypto-data\snowflake_export\table__PROTOCOL_TVL.csv'
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    # Data structure: protocol -> list of (timestamp, tvl)
    data = defaultdict(list)
    
    print("Reading CSV data...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                protocol = row['PROTOCOL_NAME']
                tvl_str = row['TVL']
                ts_str = row['TIMESTAMP']
                
                if not tvl_str or not ts_str:
                    continue
                    
                tvl = float(tvl_str)
                
                # Parse timestamp: 2025-11-21 13:12:29.723000-08:00
                # We'll handle the offset manually or ignore it for simplicity in plotting if it's consistent
                # Python 3.7+ fromisoformat handles some offsets, but let's be robust
                # Truncate offset for simple parsing if needed, or use a flexible approach
                try:
                    # Attempt standard ISO format
                    dt = datetime.fromisoformat(ts_str)
                except ValueError:
                    # Fallback for formats that might have issues (e.g. space instead of T)
                    # "2025-11-21 13:12:29.723000-08:00" -> replace space with T might help
                    # or just split
                    clean_ts = ts_str.replace(' ', 'T')
                    dt = datetime.fromisoformat(clean_ts)

                data[protocol].append((dt, tvl))
            except Exception as e:
                # print(f"Skipping row due to error: {e}")
                continue

    if not data:
        print("No valid data found.")
        return

    # Find Top 5 Protocols by Max TVL
    max_tvls = {}
    for proto, points in data.items():
        if points:
            max_tvls[proto] = max(p[1] for p in points)
    
    top_5_protocols = sorted(max_tvls.items(), key=lambda x: x[1], reverse=True)[:5]
    top_5_names = [p[0] for p in top_5_protocols]
    
    print(f"Top 5 Protocols: {top_5_names}")

    # Plotting
    fig, ax = plt.subplots(figsize=(14, 8))
    
    colors = ['#00aaff', '#9933ff', '#00cc66', '#ff3399', '#ff9900']
    
    for i, proto in enumerate(top_5_names):
        points = data[proto]
        # Sort by time
        points.sort(key=lambda x: x[0])
        
        dates = [p[0] for p in points]
        tvls = [p[1] / 1e9 for p in points] # Convert to Billions
        
        color = colors[i % len(colors)]
        ax.plot(dates, tvls, label=proto, color=color, linewidth=2, marker='.', markersize=4)
        
        # Annotate last point
        if dates:
            last_date = dates[-1]
            last_tvl = tvls[-1]
            ax.annotate(f'{last_tvl:.2f}B', xy=(last_date, last_tvl), xytext=(5, 0), 
                        textcoords='offset points', color=color, fontsize=9, fontweight='bold')

    # Formatting
    ax.set_title("Top 5 Protocols by TVL", fontsize=16, fontweight='bold', pad=20)
    ax.set_ylabel("Total Value Locked (Billions USD)", fontsize=12, labelpad=10)
    ax.set_xlabel("Timestamp (UTC)", fontsize=12, labelpad=10)
    
    # X-Axis Date Formatting
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M\n%d-%b'))
    plt.xticks(rotation=0)
    
    # Grid
    ax.grid(True, linestyle='--', alpha=0.6)
    
    # Legend
    ax.legend(loc='upper right', fontsize=10, frameon=True, shadow=True)
    
    # Background color
    ax.set_facecolor('#f8f9fa')
    
    # Save
    output_path = 'tvl_trend_chart.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_tvl_trend_chart()
