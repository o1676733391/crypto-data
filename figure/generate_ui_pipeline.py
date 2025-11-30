import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_ui_pipeline():
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Define box styles
    data_props = dict(boxstyle='round,pad=0.5', facecolor='#fff2e6', edgecolor='#cc6600', linewidth=2)
    app_props = dict(boxstyle='round,pad=0.5', facecolor='#e6ffe6', edgecolor='#00cc00', linewidth=2)
    module_props = dict(boxstyle='round,pad=0.3', facecolor='#e6f7ff', edgecolor='#0066cc', linewidth=1.5)
    viz_props = dict(boxstyle='round,pad=0.5', facecolor='#f9f2ff', edgecolor='#6600cc', linewidth=2)
    
    # Coordinates
    x_data = 0.15
    x_app = 0.4
    x_modules = 0.65
    x_viz = 0.9
    
    y_center = 0.5
    
    # 1. Data Layer (Left)
    ax.text(x_data, y_center + 0.15, "CSV Exports\n(Historical Data)", ha='center', va='center', bbox=data_props, fontsize=10)
    ax.text(x_data, y_center - 0.15, "Live API Query\n(Real-time Data)", ha='center', va='center', bbox=data_props, fontsize=10)
    
    # 2. Streamlit App (Center-Left)
    ax.text(x_app, y_center, "Streamlit App\n(Main Controller)", ha='center', va='center', bbox=app_props, fontsize=11, fontweight='bold')
    
    # 3. Page Modules (Center-Right)
    modules = [
        "Market Overview",
        "Protocol TVL",
        "Chain Dominance",
        "Stablecoins",
        "Technical Analysis"
    ]
    
    y_start = y_center + 0.25
    for i, mod in enumerate(modules):
        y_pos = y_start - (i * 0.12)
        ax.text(x_modules, y_pos, mod, ha='center', va='center', bbox=module_props, fontsize=9)
        # Connect App to Module
        ax.annotate("", xy=(x_modules - 0.08, y_pos), xytext=(x_app + 0.08, y_center), 
                    arrowprops=dict(arrowstyle='->', color='#00cc00', linewidth=1))

    # Group Label for Modules
    ax.text(x_modules, y_start + 0.1, "BI Page Modules", ha='center', va='center', fontsize=10, fontweight='bold', color='#0066cc')

    # 4. Visualization Layer (Right)
    ax.text(x_viz, y_center, "Visualization Layer\n(Plotly / Altair)", ha='center', va='center', bbox=viz_props, fontsize=10)
    
    # Connect Modules to Viz Layer
    # Draw a bracket or multiple arrows
    for i in range(len(modules)):
        y_pos = y_start - (i * 0.12)
        ax.annotate("", xy=(x_viz - 0.08, y_center), xytext=(x_modules + 0.08, y_pos), 
                    arrowprops=dict(arrowstyle='->', color='#6600cc', linewidth=1, alpha=0.5))

    # Connect Data to App
    ax.annotate("", xy=(x_app - 0.08, y_center + 0.02), xytext=(x_data + 0.08, y_center + 0.15), 
                arrowprops=dict(arrowstyle='->', color='#cc6600', linewidth=2))
    ax.annotate("", xy=(x_app - 0.08, y_center - 0.02), xytext=(x_data + 0.08, y_center - 0.15), 
                arrowprops=dict(arrowstyle='->', color='#cc6600', linewidth=2))

    # Set limits and remove axes
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.title("UI / Data Pipeline Diagram", fontsize=14, pad=20)
    
    output_path = 'ui_pipeline_diagram.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_ui_pipeline()
