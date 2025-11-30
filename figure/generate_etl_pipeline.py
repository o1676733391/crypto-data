import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_etl_pipeline():
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Define box styles
    box_props = dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor='#333333', linewidth=2)
    process_props = dict(boxstyle='round,pad=0.5', facecolor='#e6f7ff', edgecolor='#0066cc', linewidth=2)
    db_props = dict(boxstyle='round,pad=0.5', facecolor='#fff2e6', edgecolor='#cc6600', linewidth=2)
    
    # Coordinates
    y_main = 0.5
    x_src = 0.1
    x_extract = 0.3
    x_transform = 0.5
    x_load = 0.7
    x_dest = 0.9
    
    # 1. Data Sources (Split)
    ax.text(x_src, y_main + 0.15, "Binance API\n(Crypto Prices)\n5-sec Polling", ha='center', va='center', bbox=box_props, fontsize=9)
    ax.text(x_src, y_main - 0.15, "DefiLlama API\n(DeFi TVL)\n60-min Polling", ha='center', va='center', bbox=box_props, fontsize=9)
    
    # 2. Extract Layer
    ax.text(x_extract, y_main, "EXTRACT\n\nAsync Fetcher\n(aiohttp / httpx)\nConnection Pooling", ha='center', va='center', bbox=process_props, fontsize=10)
    
    # Retry Logic Arrow (Loop below)
    retry_arrow = patches.FancyArrowPatch((x_extract + 0.02, y_main - 0.12), (x_extract - 0.02, y_main - 0.12),
                                          connectionstyle="arc3,rad=-2.0", 
                                          arrowstyle='->', color='red', linewidth=1.5, mutation_scale=15)
    ax.add_patch(retry_arrow)
    ax.text(x_extract, y_main - 0.25, "Retry Logic\n(Exp. Backoff)", ha='center', va='center', fontsize=9, color='red')

    # 3. Transform Layer
    ax.text(x_transform, y_main, "TRANSFORM\n\n• Cleansing\n• Normalization\n• Enrichment\n• Validations", ha='center', va='center', bbox=process_props, fontsize=10)
    
    # 4. Load Layer
    ax.text(x_load, y_main, "LOAD\n\nBulk Batching\n(executemany)\nTransaction Mgmt", ha='center', va='center', bbox=process_props, fontsize=10)
    
    # 5. Destinations (Split)
    ax.text(x_dest, y_main + 0.15, "Supabase\n(Real-time DB)\nPostgreSQL", ha='center', va='center', bbox=db_props, fontsize=9)
    ax.text(x_dest, y_main - 0.15, "Snowflake\n(Data Warehouse)\nOLAP Storage", ha='center', va='center', bbox=db_props, fontsize=9)
    
    # Draw Arrows
    arrow_props = dict(arrowstyle='->', linewidth=2, color='#333333', mutation_scale=20)
    
    # Sources -> Extract
    ax.annotate("", xy=(x_extract - 0.09, y_main + 0.02), xytext=(x_src + 0.09, y_main + 0.15), arrowprops=arrow_props)
    ax.annotate("", xy=(x_extract - 0.09, y_main - 0.02), xytext=(x_src + 0.09, y_main - 0.15), arrowprops=arrow_props)
    
    # Extract -> Transform
    ax.annotate("", xy=(x_transform - 0.09, y_main), xytext=(x_extract + 0.09, y_main), arrowprops=arrow_props)
    
    # Transform -> Load
    ax.annotate("", xy=(x_load - 0.09, y_main), xytext=(x_transform + 0.09, y_main), arrowprops=arrow_props)
    
    # Load -> Destinations
    ax.annotate("", xy=(x_dest - 0.09, y_main + 0.15), xytext=(x_load + 0.09, y_main + 0.02), arrowprops=arrow_props)
    ax.annotate("", xy=(x_dest - 0.09, y_main - 0.15), xytext=(x_load + 0.09, y_main - 0.02), arrowprops=arrow_props)
    
    # Set limits and remove axes
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.title("ETL Pipeline Flow (Based on Data Warehouse Report)", fontsize=14, pad=20)
    
    output_path = 'etl_pipeline_flow.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_etl_pipeline()
