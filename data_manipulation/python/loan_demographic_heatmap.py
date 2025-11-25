import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def create_heatmap(df, x_var, y_var, z_var, x_label=None, y_label=None, z_label=None, title=None,
                   n_bins=10, figsize=(14, 10), cmap='viridis', save_path=None):
    """
    Create a heatmap showing the relationship between three variables.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe
    x_var : str
        Column name for x-axis variable
    y_var : str  
        Column name for y-axis variable
    z_var : str
        Column name for z-axis variable (displayed as heatmap colors)
    x_label : str, optional
        Custom label for x-axis (defaults to x_var)
    y_label : str, optional
        Custom label for y-axis (defaults to y_var)
    z_label : str, optional
        Custom label for colorbar (defaults to z_var)
    n_bins : int
        Number of bins for x and y variables (default: 25)
    figsize : tuple
        Figure size (default: (14, 10))
    cmap : str
        Colormap for heatmap (default: 'viridis')
    save_path : str, optional
        Path to save the plot as PNG (e.g., 'my_heatmap.png'). If None, plot is not saved.
    
    Returns:
    --------
    fig : matplotlib.figure.Figure
        The created figure object
    """
    
    # Set default labels if not provided
    if x_label is None:
        x_label = x_var
    if y_label is None:
        y_label = y_var
    if z_label is None:
        z_label = z_var
    
    # Clean data
    df_clean = df[[x_var, y_var, z_var]].dropna()
    
    print(f"Data points: {len(df_clean)}")
    print(f"{z_var} range: {df_clean[z_var].min():.2f} to {df_clean[z_var].max():.2f}")
    
    # Create bins
    x_bins = pd.cut(df_clean[x_var], bins=n_bins)
    y_bins = pd.cut(df_clean[y_var], bins=n_bins)
    
    # Create pivot table
    heatmap_data = df_clean.groupby([y_bins, x_bins])[z_var].mean().unstack()
    
    # Calculate midpoints for labels
    x_midpoints = [(interval.left + interval.right) / 2 for interval in heatmap_data.columns]
    y_midpoints = [(interval.left + interval.right) / 2 for interval in heatmap_data.index]
    
    # Create single heatmap
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(heatmap_data, ax=ax, cmap=cmap, 
                cbar_kws={'label': z_label}, linewidths=0.1, linecolor='white')
    
    ax.set_title(title, fontsize=14, pad=20)
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    
    # Set midpoint labels
    n_x_ticks = len(x_midpoints)
    n_y_ticks = len(y_midpoints)
    ax.set_xticks(range(0, n_x_ticks, max(1, n_x_ticks//8)))
    ax.set_yticks(range(0, n_y_ticks, max(1, n_y_ticks//8)))
    ax.set_xticklabels([f'{x_midpoints[i]:.1f}' for i in range(0, n_x_ticks, max(1, n_x_ticks//8))], rotation=45)
    ax.set_yticklabels([f'{y_midpoints[i]:.1f}' for i in range(0, n_y_ticks, max(1, n_y_ticks//8))], rotation=0)
    
    plt.tight_layout()
    
    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"Plot saved as: {save_path}")
    
    plt.show()
    
    # Summary
    print(f"\nHeatmap Summary:")
    print(f"Non-empty cells: {heatmap_data.count().sum()}")
    print(f"Total possible cells: {n_bins * n_bins}")
    print(f"Data coverage: {heatmap_data.count().sum()/(n_bins*n_bins)*100:.1f}%")
    
    return fig

# Example usage with your data:
# Load your data first
df = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged_pca.dta')

# Create heatmap with custom labels and save as PNG
fig = create_heatmap(df, 
                    x_var='medinc', 
                    y_var='pct_mnty', 
                    z_var='AmtSBLr19',
                    x_label='Median Income ($)',
                    y_label='Percent Minority (%)', 
                    z_label='Small Business Lending Amount ($)',
                    title = 'Small Business Lending Amount by Median Income and Percent Minority',
                    n_bins=10,
                    cmap='turbo',
                    save_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/medinc_minority_amtsbl_3d_plot.png')

fig = create_heatmap(df, 
                    x_var='medinc', 
                    y_var='pct_black', 
                    z_var='AmtSBLr19',
                    x_label='Median Income ($)',
                    y_label='Percent Black (%)', 
                    z_label='Small Business Lending Amount ($)',
                    title = 'Small Business Lending Amount by Median Income and Percent Black',
                    n_bins=25,
                    cmap='turbo',
                    save_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/medinc_black_amtsbl_3d_plot.png')


fig = create_heatmap(df, 
                    x_var='pc1', 
                    y_var='pct_black', 
                    z_var='AmtSBLr19',
                    x_label='Component 1',
                    y_label='Percent Black (%)', 
                    z_label='Small Business Lending Amount ($)',
                    title = 'Small Business Lending Amount by Principle Component 1 and Percent Black',
                    n_bins=25,
                    cmap='turbo',
                    save_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/comp1_black_amtsbl_3d_plot.png')


fig = create_heatmap(df, 
                    x_var='pc1', 
                    y_var='pct_mnty', 
                    z_var='AmtSBLr19',
                    x_label='Component 1',
                    y_label='Percent Minority (%)', 
                    z_label='Small Business Lending Amount ($)',
                    title = 'Small Business Lending Amount by Principle Component 1 and Percent Minority',
                    n_bins=10,
                    cmap='turbo',
                    save_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/comp1_minority_amtsbl_3d_plot.png')

