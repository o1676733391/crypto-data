import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_star_schema():
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Define box properties
    fact_props = dict(boxstyle='round,pad=0.3', facecolor='#e6f2ff', edgecolor='#004080', linewidth=2)
    dim_highlight_props = dict(facecolor='#fff2cc', edgecolor='#d6b656', alpha=0.5)
    
    # Helper function to draw a table
    def draw_table(x, y, title, columns, width=0.35, height=0.35):
        # Main box (Background)
        rect = patches.Rectangle((x, y - height), width, height, 
                                 linewidth=2, edgecolor='#004080', facecolor='white')
        ax.add_patch(rect)
        
        # Title header (Top strip)
        header_height = 0.05
        header_rect = patches.Rectangle((x, y - header_height), width, header_height,
                                        linewidth=2, edgecolor='#004080', facecolor='#004080')
        ax.add_patch(header_rect)
        
        # Title Text
        ax.text(x + width/2, y - header_height/2, title, 
                ha='center', va='center', color='white', fontweight='bold', fontsize=10)
        
        # Columns
        start_text_y = y - header_height - 0.03
        for i, (col_name, col_type) in enumerate(columns):
            # Highlight embedded dimensions
            if "Dim" in col_type:
                text_color = '#b35900' # Brown/Orange for dims
                weight = 'bold'
            else:
                text_color = 'black'
                weight = 'normal'
                
            ax.text(x + 0.02, start_text_y - (i * 0.025), f"• {col_name}", 
                    ha='left', va='center', fontsize=9, color=text_color, fontweight=weight)
            
            ax.text(x + width - 0.02, start_text_y - (i * 0.025), col_type, 
                    ha='right', va='center', fontsize=8, color='gray', style='italic')

    # --- Define Tables ---
    
    # 1. PROTOCOL_TVL
    cols_protocol = [
        ("ID", "PK"),
        ("PROTOCOL_SLUG", "Dim (Degenerate)"),
        ("PROTOCOL_NAME", "Dim (Embedded)"),
        ("CATEGORY", "Dim (Embedded)"),
        ("CHAIN", "Dim (Embedded)"),
        ("TVL", "Measure"),
        ("CHANGE_1D", "Measure"),
        ("CHANGE_7D", "Measure"),
        ("TIMESTAMP", "Dim (Time)")
    ]
    draw_table(0.1, 0.85, "PROTOCOL_TVL (Fact)", cols_protocol)
    
    # 2. CHAIN_TVL
    cols_chain = [
        ("ID", "PK"),
        ("CHAIN_NAME", "Dim (Chain)"),
        ("TVL", "Measure"),
        ("CHANGE_1D", "Measure"),
        ("CHANGE_7D", "Measure"),
        ("DOMINANCE_PCT", "Measure"),
        ("TIMESTAMP", "Dim (Time)")
    ]
    draw_table(0.55, 0.85, "CHAIN_TVL (Fact)", cols_chain)
    
    # 3. MARKET_TICKS
    cols_ticks = [
        ("ID", "PK"),
        ("SYMBOL", "Dim (Embedded)"),
        ("EXCHANGE", "Dim (Source)"),
        ("LAST_PRICE", "Measure"),
        ("VOLUME_24H", "Measure"),
        ("MARKET_CAP", "Measure"),
        ("BID_ASK_SPREAD", "Measure"),
        ("INGESTED_AT", "Dim (Time)")
    ]
    draw_table(0.1, 0.4, "MARKET_TICKS (Fact)", cols_ticks)
    
    # 4. CANDLES_* (Aggregated)
    cols_candles = [
        ("ID", "PK"),
        ("SYMBOL", "Dim (Embedded)"),
        ("TIMEFRAME", "Dim (Metadata)"),
        ("OPEN", "Measure"),
        ("HIGH", "Measure"),
        ("LOW", "Measure"),
        ("CLOSE", "Measure"),
        ("VOLUME", "Measure"),
        ("TIMESTAMP", "Dim (Time)")
    ]
    draw_table(0.55, 0.4, "CANDLES_* (Agg Fact)", cols_candles)

    # --- Annotations / Legend ---
    
    # Legend
    # ax.text(0.5, 0.98, "Star Schema with Embedded Dimensions", ha='center', fontsize=16, fontweight='bold')
    # ax.text(0.5, 0.95, "(Denormalized for OLAP Performance)", ha='center', fontsize=12, color='gray')
    
    # Legend Box
    legend_x = 0.85
    legend_y = 0.95
    ax.text(legend_x, legend_y, "Legend", fontweight='bold')
    ax.text(legend_x, legend_y - 0.03, "• Dimension Column", color='#b35900', fontweight='bold')
    ax.text(legend_x, legend_y - 0.06, "• Measure Column", color='black')

    # Set limits and remove axes
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    output_path = 'star_schema_diagram.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_star_schema()
