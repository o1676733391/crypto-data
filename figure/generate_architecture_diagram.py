import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_architecture_diagram():
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Define box properties
    box_props = dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor='black', linewidth=1.5)
    layer_props = dict(boxstyle='round,pad=0.8', facecolor='#f0f0f0', edgecolor='gray', linestyle='--', linewidth=1)
    
    # Coordinates (0-1 scale)
    # Data Sources (Left)
    src_x = 0.15
    src_y_binance = 0.7
    src_y_defi = 0.5
    
    # Layers (Middle)
    layer_x = 0.5
    speed_y = 0.75
    batch_y = 0.45
    
    # Serving (Right/Bottom)
    serving_x = 0.85
    serving_y = 0.6
    
    # Draw "Data Sources" Group Label
    ax.text(src_x, 0.85, "DATA SOURCES", ha='center', va='center', fontsize=12, fontweight='bold')
    
    # Draw Data Source Boxes
    ax.text(src_x, src_y_binance, "Binance API\n(Crypto Prices)", ha='center', va='center', bbox=box_props, fontsize=10)
    ax.text(src_x, src_y_defi, "DefiLlama API\n(TVL Data)", ha='center', va='center', bbox=box_props, fontsize=10)
    
    # Draw Speed Layer
    ax.text(layer_x, speed_y, "SPEED LAYER\n\nSupabase\n(PostgreSQL)\n\nLatency: ~5s", ha='center', va='center', 
            bbox=dict(boxstyle='round,pad=1', facecolor='#e6f3ff', edgecolor='#0066cc', linewidth=2), fontsize=10)
    
    # Draw Batch Layer
    ax.text(layer_x, batch_y, "BATCH LAYER\n\nSnowflake\n(Data Warehouse)\n\nRefresh: 60min", ha='center', va='center', 
            bbox=dict(boxstyle='round,pad=1', facecolor='#fff2e6', edgecolor='#cc6600', linewidth=2), fontsize=10)
    
    # Draw Serving Layer
    # Placing it to the right or bottom. User sketch implies flow into Serving.
    # Let's put it on the right to show convergence.
    ax.text(serving_x, serving_y, "SERVING LAYER\n\nStreamlit\nDashboards", ha='center', va='center', 
            bbox=dict(boxstyle='round,pad=1', facecolor='#e6ffe6', edgecolor='#00cc00', linewidth=2), fontsize=11)
    
    # Draw Arrows
    arrow_props = dict(arrowstyle='->', linewidth=2, color='black', mutation_scale=20)
    
    # Source -> Speed
    ax.annotate("", xy=(layer_x - 0.12, speed_y), xytext=(src_x + 0.08, src_y_binance), arrowprops=arrow_props)
    
    # Source -> Batch
    ax.annotate("", xy=(layer_x - 0.12, batch_y), xytext=(src_x + 0.08, src_y_defi), arrowprops=arrow_props)
    # Also connect Binance to Batch (usually raw data goes to batch too)
    ax.annotate("", xy=(layer_x - 0.12, batch_y + 0.05), xytext=(src_x + 0.08, src_y_binance - 0.02), arrowprops=dict(arrowstyle='->', linewidth=1, color='gray', linestyle='--', mutation_scale=15))

    # Speed -> Serving
    ax.annotate("", xy=(serving_x - 0.08, serving_y + 0.02), xytext=(layer_x + 0.12, speed_y), arrowprops=arrow_props)
    
    # Batch -> Serving
    ax.annotate("", xy=(serving_x - 0.08, serving_y - 0.02), xytext=(layer_x + 0.12, batch_y), arrowprops=arrow_props)
    
    # Set limits and remove axes
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.title("Figure 3: System Architecture Diagram (Lambda Architecture)", fontsize=14, pad=20)
    
    output_path = 'system_architecture_diagram.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_architecture_diagram()
