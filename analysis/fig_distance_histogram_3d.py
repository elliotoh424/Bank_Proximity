"""
Bank Branch Distance Visualization for Pharmacy Analysis

This module generates 3D histograms showing the distribution of distances
from pharmacies to different types of bank branches (community banks vs 
non-community banks). The visualizations help analyze spatial relationships
between financial institutions and retail pharmacies.

Data Sources:
- adv_drug_stores_full.dta: All pharmacy location data from Advan
- adv_retailer_bankbranch_dist.parquet: Distance to nearest bank branch distancefor each pharmacy
- acs_drug_matched_month_radiusdef.dta: Regresison sample pharmacy location data from Advan

Output:
- 3D histogram histogram to nearest community bank and nearest non-community bank  
"""

import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
pd.options.display.float_format = '{:.6f}'.format
np.set_printoptions(suppress=True)


# data = drug
# nbins = 50
# title = 'test'

def gen_3d_hist(data, nbins, figure, title, ratio, z_manual, zmax, increment):
    """
    Generate a 3D histogram visualization of bank branch distances.
    
    Parameters:
    -----------
    data : Pharmacy data
        DataFrame containing columns 'mindist_cb' and 'mindist_noncb'
    nbins : int
        Number of bins for the histogram in both x and y dimensions
    figure : str
        Filename for saving the figure (without extension)
    title : str
        Plot title to display above the histogram
    ratio : bool
        If True, normalize histogram counts to proportions (0-1)
        If False, use raw frequency counts
    z_manual : bool
        If True, manually set z-axis limits using zmax and increment
        If False, use automatic z-axis scaling
    zmax : float
        Maximum value for z-axis when z_manual=True
    increment : float
        Tick interval for z-axis when z_manual=True
    
    Returns:
    --------
    None
        Saves the figure to the results/figures directory
    
    Example:
    --------
    >>> gen_3d_hist(drug_data, 50, 'my_plot', 'Bank Distances', 
    ...             ratio=True, z_manual=True, zmax=0.04, increment=0.01)
    """
    # Create histogram
    x = data.mindist_cb.to_numpy()
    y = data.mindist_noncb.to_numpy()
    #
    # Create a 3D plot
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    #
    # Create a 2D histogram (with bins=30 for example)
    hist, xedges, yedges = np.histogram2d(x, y, bins=nbins)
    if ratio:
        hist = hist / hist.sum()
    #
    # Construct arrays for the anchor positions of the bars
    xpos, ypos = np.meshgrid(xedges[:-1] + 0.25, yedges[:-1] + 0.25, indexing="ij")
    xpos = xpos.ravel()
    ypos = ypos.ravel()
    zpos = 0
    #
    # Construct arrays with the dimensions for the bars
    dx = dy = np.ones_like(zpos)
    dz = hist.ravel()
    #
    # Plot the bars
    ax.bar3d(xpos, ypos, zpos, dx, dy, dz, zsort='average')
    #
    # Invert x axis so that y and x axis meet where they are both zero
    ax.set_ylim(nbins,0)
    ax.set_xlim(0,nbins)
    
    # Set labels
    ax.set_xlabel('100s Yard to Nearest CB')
    ax.set_ylabel('100s Yard to Nearest Non-CB')
    ax.set_zlabel('Frequency')
    ax.set_title(title)
    #
    if z_manual:
        ax.set_zlim(0, zmax)  # Set the range of the z axis from 0 to 5000
        ax.set_zticks(np.arange(0, zmax+0.01, increment))  # Set z axis ticks from 0 to 5000 with 1000 increments
    #
    plt.savefig(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/{figure}.png')
    plt.close()

########################################################################################################################################
# Full sample of drug stores
########################################################################################################################################
full_cols = ['placekey','d_brand','date_range_start','latitude','longitude','tract']
drug_full = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_full.dta", columns = full_cols)
drug_full = drug_full[drug_full.date_range_start=='2020-01-01'].drop('date_range_start',axis=1).drop_duplicates() # Filter to baseline period (January 2020) for cross-sectional analysis

# Distance to nearest community bank branch, nearest non-community bank branch
retail_cols = ['placekey','mindist_from_branch_yd','mindist_from_cb_branch_yd','mindist_from_noncb_branch_yd']
retailer = pd.read_parquet("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_retailer_bankbranch_dist.parquet", columns = retail_cols).drop_duplicates()
drug_full = drug_full.merge(retailer , on=['placekey'], how='left', indicator=True)
drug_full._merge.value_counts() # left onlys are drug stores that are at least 5000 yards away from the closest bank or has no bank in the county.

drug_full.columns = drug_full.columns.str.replace('_yd','')
drug_full.columns = drug_full.columns.str.replace('mindist_from_branch','mindist_from_bank_branch')
drug_full.columns = drug_full.columns.str.replace('mindist_from','mindist')
drug_full.columns = drug_full.columns.str.replace('_branch','')

drug_full['mindist_bank'] = drug_full['mindist_bank']/100
drug_full['mindist_cb'] = drug_full['mindist_cb']/100
drug_full['mindist_noncb'] = drug_full['mindist_noncb']/100

# Truncate Dist to nearest bank branch to 5000 yards.
for c in ['mindist_bank','mindist_cb','mindist_noncb']:
    drug_full.loc[drug_full[c]>=50,c] = 50
    drug_full.loc[drug_full[c].isnull(),c] = 50

drug_full_nonbrand = drug_full[drug_full.d_brand==0]
drug_full_brand = drug_full[drug_full.d_brand==1]

# Full sample figures for all independent pharmacies and all brand pharmacies
gen_3d_hist(drug_full_nonbrand, 50, 'hist_cb_noncb_nonbrand_full_drug_all','All Independent Drug Stores (Full)', ratio=0, z_manual = 0, zmax=np.nan, increment=np.nan)
gen_3d_hist(drug_full_brand, 50, 'hist_cb_noncb_brand_full_drug_all','All Brand Drug Stores (Full)', ratio=0, z_manual = 0, zmax=np.nan, increment=np.nan)

########################################################################################################################################
# Regression sample of drug stores
########################################################################################################################################
cols = ['placekey','start','mindist_from_bank_branch_yd', 'mindist_from_cb_branch_yd', 'mindist_from_noncb_branch_yd', 'pct_brandtr_jan2020', 'd_brand']
drug = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", columns = cols)

drug.columns = drug.columns.str.replace('pct_','pct')
drug.columns = drug.columns.str.replace('mindist_from_','mindist_')
drug.columns = drug.columns.str.replace('_yd','')
drug.columns = drug.columns.str.replace('_branch','')

drug['mindist_bank'] = drug['mindist_bank']/100
drug['mindist_cb'] = drug['mindist_cb']/100
drug['mindist_noncb'] = drug['mindist_noncb']/100

# Truncate Dist.s at 5000 yards.
for c in ['mindist_bank','mindist_cb','mindist_noncb']:
    drug.loc[drug[c]>=50,c] = 50

# Regression sample diagram
gen_3d_hist(drug, 50, 'hist_cb_noncb_full_drug','All Drug Stores', ratio=0, z_manual = 0, zmax=np.nan, increment=np.nan)
gen_3d_hist(drug, 50, 'hist_cb_noncb_full_ratio_drug','All Drug Stores', ratio=1, z_manual=1, zmax=0.04, increment=0.01)

drug_nonbrand = drug[drug.d_brand==0]
drug_brand = drug[drug.d_brand==1]

gen_3d_hist(drug_nonbrand, 50, 'hist_cb_noncb_nonbrand_full_drug','All Independent Drug Stores', ratio=0, z_manual = 0, zmax=np.nan, increment=np.nan)
gen_3d_hist(drug_brand, 50, 'hist_cb_noncb_brand_full_drug','All Brand Drug Stores', ratio=0, z_manual = 0, zmax=np.nan, increment=np.nan)