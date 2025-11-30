import matplotlib.pyplot as plt
import numpy as np

def draw_olap_cube():
    # Dimensions
    x_dim, y_dim, z_dim = 3, 3, 3
    
    # Create the grid
    x, y, z = np.indices((x_dim + 1, y_dim + 1, z_dim + 1))
    
    # Create a boolean array for voxels (all True initially)
    voxels = np.ones((x_dim, y_dim, z_dim), dtype=bool)
    
    # Define colors
    colors = np.empty(voxels.shape, dtype=object)
    colors[:] = '#A8DADC90'  # Light blue-green with transparency
    
    # Highlight a slice (e.g., "Dice" operation on Time)
    colors[:, :, 1] = '#457B9D' # Darker blue for the middle time slice
    
    # Edge colors
    edge_colors = 'black'
    
    # Create figure
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection='3d')
    
    # Plot voxels
    ax.voxels(voxels, facecolors=colors, edgecolors=edge_colors, linewidth=0.5)
    
    # Set labels matching the new requirements
    ax.set_xlabel('\nProtocol', fontsize=12, labelpad=10)
    ax.set_ylabel('\nChain', fontsize=12, labelpad=10)
    ax.set_zlabel('\nTime', fontsize=12, labelpad=10)
    
    # Custom ticks
    ax.set_xticks([0.5, 1.5, 2.5])
    ax.set_xticklabels(['Aave', 'Uniswap', 'Lido'])
    
    ax.set_yticks([0.5, 1.5, 2.5])
    ax.set_yticklabels(['Ethereum', 'Solana', 'BSC'])
    
    ax.set_zticks([0.5, 1.5, 2.5])
    ax.set_zticklabels(['Jan', 'Feb', 'Mar'])
    
    # Remove grid panes for cleaner look
    ax.grid(False)
    
    # Set view angle to see all 3 annotated sides
    ax.view_init(elev=30, azim=-60)
    
    # Title
    plt.title("OLAP Cube: Protocol x Chain x Time", fontsize=14, pad=20)
    
    # Save the chart
    output_path = 'olap_cube_diagram.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Chart saved to {output_path}")

if __name__ == "__main__":
    draw_olap_cube()
