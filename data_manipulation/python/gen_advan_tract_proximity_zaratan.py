import pandas as pd
import numpy as np
import duckdb
import time
import gc
import re
pd.options.display.float_format = '{:.6f}'.format
import warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

datadir = '/home/elliotoh/scratch/data/'
# Load bank branch data (from first code's SOD processing)
print("Loading bank branch data...")
relevant_cols = ['rssdid', 'brnum', 'namefull', 'cb', 'latitude', 'longitude','zipbr','GEOID10']
sod = pd.read_excel(f'{datadir}sod2019_tract10_new.xlsx', 
                    usecols=relevant_cols, dtype=str)
sod.rename(columns={'GEOID10':'tract', 'zipbr':'zip'}, inplace=True)

# Load additional SOD data
sod_vars = ['RSSDID','BRNUM','ASSET','SIMS_ESTABLISHED_DATE','ADDRESBR','CITYBR','STALPBR','ZIPBR']
sod1 = pd.read_csv(f'{datadir}sod_2019.csv', 
                    usecols=sod_vars, dtype=str)
sod1.columns = sod1.columns.str.lower()
sod1 = sod1[~sod1.stalpbr.str.contains('^PR$|^VI$|^AS$|^FM$|^GU$|^MH$|^MP$|^PW$', na=False)]
sod1.rename(columns={'sims_established_date':'date_established'}, inplace=True)

# Merge SOD datasets
sod = sod.merge(sod1, on=['rssdid','brnum'], how='outer', indicator=True)
assert sod[sod._merge!='both'].shape[0] == 0
sod = sod.drop('_merge', axis=1)

# Convert data types
for c in ['latitude', 'longitude', 'asset', 'rssdid', 'cb']:
    sod[c] = pd.to_numeric(sod[c])

sod['stcntybr'] = sod.tract.str[:5]
sod['zip'] = sod.zip.str.zfill(5)

# Create bank size categories (matching first code)
sod['asset'] = sod.asset/1000  # assets in millions
sod['big4_asset'] = ((sod.rssdid == 852218) | (sod.rssdid == 480228) | 
                    (sod.rssdid == 476810) | (sod.rssdid == 451965)).astype(float)
sod['large4_asset'] = ((sod.asset > 100*10**3) & (sod.big4_asset == 0) & 
                        (~sod.asset.isnull())).astype(float)
sod['mega_asset'] = ((sod.asset > 100*10**3) & (~sod.asset.isnull())).astype(float)
sod['cb_asset'] = ((sod['cb'] == 1) & (~sod.asset.isnull())).astype(float)
sod['nonmega_asset'] = ((sod.asset <= 100*10**3) & (sod.cb_asset == 0) & 
                        (~sod.asset.isnull())).astype(float)

# Create unique bank branch identifier
sod['bank_branch_id'] = sod.rssdid.astype(str) + '_' + sod.brnum.astype(str)

# Calculate bank competition within same tract and CBG
print("Calculating bank competition within geographic areas...")

# Same tract competition
sod_tract = sod[['bank_branch_id', 'tract', 'mega_asset', 'nonmega_asset', 'cb_asset']].copy()
sod_tract_merge = sod_tract.merge(sod_tract, on='tract', how='left', suffixes=('_bank', '_other'))
sod_tract_merge = sod_tract_merge[sod_tract_merge.bank_branch_id_bank != sod_tract_merge.bank_branch_id_other]

# Aggregate competition by tract
tract_competition = sod_tract_merge.groupby('bank_branch_id_bank').agg({
    'mega_asset_other': ['max', 'sum'],
    'nonmega_asset_other': ['max', 'sum'], 
    'cb_asset_other': ['max', 'sum']
}).reset_index()

tract_competition.columns = ['bank_branch_id'] + [f"{col[0]}_{col[1]}_tract" for col in tract_competition.columns[1:]]
tract_competition.rename(columns={
    'mega_asset_other_max_tract': 'mega_tract',
    'mega_asset_other_sum_tract': 'nmega_tract',
    'nonmega_asset_other_max_tract': 'nonmega_tract', 
    'nonmega_asset_other_sum_tract': 'nnonmega_tract',
    'cb_asset_other_max_tract': 'cb_tract',
    'cb_asset_other_sum_tract': 'ncb_tract'
}, inplace=True)

# # Add CBG to SOD data (derived from tract)
# sod['cbg'] = sod['tract'] + sod.groupby('tract').cumcount().astype(str).str.zfill(1)  # This is a simplification
# # Note: You may need to load actual CBG data if available

# # Same CBG competition (similar process)
# sod_cbg = sod[['bank_branch_id', 'cbg', 'mega_asset', 'nonmega_asset', 'cb_asset']].copy()
# sod_cbg_merge = sod_cbg.merge(sod_cbg, on='cbg', how='left', suffixes=('_bank', '_other'))
# sod_cbg_merge = sod_cbg_merge[sod_cbg_merge.bank_branch_id_bank != sod_cbg_merge.bank_branch_id_other]

# cbg_competition = sod_cbg_merge.groupby('bank_branch_id_bank').agg({
#     'mega_asset_other': ['max', 'sum'],
#     'nonmega_asset_other': ['max', 'sum'],
#     'cb_asset_other': ['max', 'sum']
# }).reset_index()

# cbg_competition.columns = ['bank_branch_id'] + [f"{col[0]}_{col[1]}_cbg" for col in cbg_competition.columns[1:]]
# cbg_competition.rename(columns={
#     'mega_asset_other_max_cbg': 'mega_cbg',
#     'mega_asset_other_sum_cbg': 'nmega_cbg',
#     'nonmega_asset_other_max_cbg': 'nonmega_cbg',
#     'nonmega_asset_other_sum_cbg': 'nnonmega_cbg', 
#     'cb_asset_other_max_cbg': 'cb_cbg',
#     'cb_asset_other_sum_cbg': 'ncb_cbg'
# }, inplace=True)

# Merge competition data back to SOD
sod = sod.merge(tract_competition, on='bank_branch_id', how='left')
# sod = sod.merge(cbg_competition, on='bank_branch_id', how='left')

# Fill NaN values with 0 for competition variables
competition_cols = [col for col in sod.columns if col.endswith('_tract') or col.endswith('_cbg')]
sod[competition_cols] = sod[competition_cols].fillna(0)

print(f"Processed bank competition for {len(sod)} bank branches")

# Time periods to analyze
dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01',
            '2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01',
            '2020-01-01','2020-02-01','2020-03-01','2020-04-01','2020-05-01','2020-06-01',
            '2020-07-01','2020-08-01','2020-09-01','2020-10-01','2020-11-01','2020-12-01',
            '2021-01-01','2021-02-01','2021-03-01','2021-04-01','2021-05-01','2021-06-01',
            '2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01',
            '2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01',
            '2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01']
# dates = ['2019-01-01']

geographic_df = []
t1 = time.time()

for d in dates:
    print(f"Processing date: {d}")
    # Load all POI data for this date
    allpoi_cols = ['placekey', 'region','zip', 'latitude', 'longitude', 'location_name',
                    'street_address', 'city','naics_code', 'poi_cbg', 'nvisits', 'brands']
    advan = pd.read_parquet(f'{datadir}all_foottraffic_monthly_201901_202211_full.parquet', 
                            columns=allpoi_cols, 
                            filters=[('date_range_start', '==', d), ('geometry_type','!=','POINT')])
    assert advan.shape[0] > 0
    # Clean data
    advan = advan[~advan.region.str.contains('^AS$|^GU$|^MP$|^PR$|^VI$', na=False)]
    advan = advan[(~advan.poi_cbg.isnull())]
    advan['latitude'] = pd.to_numeric(advan['latitude'])
    advan['longitude'] = pd.to_numeric(advan['longitude'])
    # Create tract from CBG
    advan['poi_tract'] = advan['poi_cbg'].str[:11]
    # Create business type indicators
    advan['d_retail'] = (advan.naics_code.str.contains('^4[45]|^7[12]|^81[12]')).astype(int)
    advan['d_brand'] = ((advan.d_retail==1) & (~advan.brands.isnull())).astype(int)
    advan['d_nonbrand'] = ((advan.d_retail==1) & (advan.brands.isnull())).astype(int)
    advan['d_remain'] = (advan.d_retail == 0).astype(int)
    #        
    naics = pd.read_excel(f'{datadir}naics_code2017.xlsx')
    naics = naics['2017- All NAICS Codes']
    naics = naics.astype(str)
    naics = naics.tolist()
    # Mark specific industries
    for nc in naics:
    # for nc in naics_cols[:10]:
        advan[f'naics_{nc}'] = advan.naics_code.str.contains(f'^{nc}', na=False).astype(int)

    # Create brand-specific variables for aggregation
    for brandtype in ['brand','nonbrand','retail','remain']:
        advan[f'placekey_{brandtype}'] = advan.placekey
        advan.loc[advan[f'd_{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
        advan[f'nvisits_{brandtype}'] = advan.nvisits
        advan.loc[advan[f'd_{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
        if brandtype != 'remain':
        #     advan[f'colocated_pharmacy_{brandtype}'] = advan.colocated_pharmacy
            advan.loc[advan[f'd_{brandtype}'] == 0, f'colocated_pharmacy_{brandtype}'] = np.nan

    advan.loc[advan.nvisits == 0, 'nvisits'] = np.nan  # Set zero visits to NaN
    advan['date_range_start'] = d
    # Initialize results for this date
    bank_geo_combined = sod[['bank_branch_id', 'rssdid', 'brnum', 'asset', 'big4_asset', 'large4_asset', 
                            'mega_asset', 'cb_asset', 'nonmega_asset', 'stcntybr', 'namefull', 'tract'] + 
                            competition_cols].copy()
    bank_geo_combined['date_range_start'] = d
    print(f"Processing tract level analysis for {len(bank_geo_combined)} bank branches")
    # Process TRACT level aggregation
    print("  Processing tract level...")
    tract_agg = advan.groupby(['poi_tract','date_range_start']).agg({
        'placekey': 'count', 
        'nvisits': ['sum','mean','count'], 
        'placekey_brand': 'count', 
        'placekey_nonbrand': 'count', 
        'placekey_retail': 'count', 
        'placekey_remain': 'count',  
        'nvisits_brand': ['sum','mean','count'], 
        'nvisits_nonbrand': ['sum','mean','count'], 
        'nvisits_retail': ['sum','mean','count'], 
        'nvisits_remain': ['sum','mean','count']
    }).reset_index()
    # Flatten column names for tract
    tract_agg.columns = tract_agg.columns.get_level_values(0) + ' ' + tract_agg.columns.get_level_values(1)
    tract_agg.columns = tract_agg.columns.str.strip()
    # Rename tract columns
    tract_agg.rename(columns={
        'placekey count': 'npoi_tract', 
        'placekey_brand count': 'brandnpoi_tract', 
        'placekey_nonbrand count': 'nonbrandnpoi_tract', 
        'placekey_retail count': 'retailnpoi_tract', 
        'placekey_remain count': 'remainnpoi_tract',
        'nvisits sum': 'allpoi_totnvisits_tract', 
        'nvisits_brand sum': 'brandpoi_totnvisits_tract', 
        'nvisits_nonbrand sum': 'nonbrandpoi_totnvisits_tract', 
        'nvisits_retail sum': 'retailpoi_totnvisits_tract', 
        'nvisits_remain sum': 'remainpoi_totnvisits_tract',
        'nvisits mean': 'allpoi_avgnvisits_tract', 
        'nvisits_brand mean': 'brandpoi_avgnvisits_tract', 
        'nvisits_nonbrand mean': 'nonbrandpoi_avgnvisits_tract', 
        'nvisits_retail mean': 'retailpoi_avgnvisits_tract', 
        'nvisits_remain mean': 'remainpoi_avgnvisits_tract',
        'nvisits count': 'allpoi_npoinm_tract', 
        'nvisits_brand count': 'brandpoi_npoinm_tract', 
        'nvisits_nonbrand count': 'nonbrandpoi_npoinm_tract', 
        'nvisits_retail count': 'retailpoi_npoinm_tract', 
        'nvisits_remain count': 'remainpoi_npoinm_tract'
    }, inplace=True)
    # Merge tract data
    bank_geo_combined = bank_geo_combined.merge(tract_agg, left_on=['tract','date_range_start'], 
                                            right_on=['poi_tract','date_range_start'], how='outer', indicator=True)
    print('SOD only tracts:',bank_geo_combined[bank_geo_combined._merge=='left_only'].tract.nunique())
    print('Advan only tracts:',bank_geo_combined[bank_geo_combined._merge=='right_only'].poi_tract.nunique())
    bank_geo_combined = bank_geo_combined.drop(['poi_tract','_merge'], axis=1)    
    # Create tract-level aggregations for each NAICS code
    print("  Processing tract-level NAICS indicators...")
    # Get all NAICS columns that were created
    naics_cols = [col for col in advan.columns if col.startswith('naics_')]
    # Create tract-level aggregations for NAICS codes
    naics_tract_agg = advan.groupby(['poi_tract', 'date_range_start']).agg({
        **{naics_col: ['sum', 'max'] for naics_col in naics_cols}
    }).reset_index()
    #
    # Flatten column names
    naics_tract_agg.columns = naics_tract_agg.columns.get_level_values(0) + '_' + naics_tract_agg.columns.get_level_values(1)
    naics_tract_agg.columns = naics_tract_agg.columns.str.strip('_')
    #
    # Rename columns for clarity
    naics_rename_dict = {}
    # for naics_col in naics_cols[:10]:
    for naics_col in naics_cols:
        naics_code = naics_col.replace('naics_', '')
        naics_rename_dict[f'{naics_col}_sum'] = f'n_{naics_code}_tract'  # Count of POIs in this NAICS code
        naics_rename_dict[f'{naics_col}_max'] = f'd_{naics_code}_tract'  # Percentage of POIs in this NAICS code
    
    naics_tract_agg.rename(columns=naics_rename_dict, inplace=True)
    # Merge NAICS tract data back to main dataset
    bank_geo_combined = bank_geo_combined.merge(
        naics_tract_agg, 
        left_on=['tract', 'date_range_start'], 
        right_on=['poi_tract', 'date_range_start'], 
        how='outer', indicator=True
    )
    print('SOD only tracts:',bank_geo_combined[bank_geo_combined._merge=='left_only'].tract.nunique())
    print('Advan only tracts:',bank_geo_combined[bank_geo_combined._merge=='right_only'].poi_tract.nunique())
    bank_geo_combined = bank_geo_combined.drop(['poi_tract','_merge'], axis=1)
    print(f"    Added {len(naics_cols)} NAICS indicators at tract level")
    #    
    # Fill NaN values with 0 for count variables
    count_cols = [col for col in bank_geo_combined.columns if 'npoi_' in col or '_npoinm_' in col or 'n_' in col]
    bank_geo_combined[count_cols] = bank_geo_combined[count_cols].fillna(0)
    #
    # Fill NaN values for bank competition variables
    fillna_cols = [col for col in bank_geo_combined.columns if col.endswith('_tract') and not re.search('nvisits',col)]
    bank_geo_combined[fillna_cols] = bank_geo_combined[fillna_cols].fillna(0)    
    geographic_df.append(bank_geo_combined)
    
    # Clean up memory
    del advan, tract_agg, naics_tract_agg
    gc.collect()
    
    t2 = time.time()
    print(f"Completed {d} in {(t2-t1)/60:.2f} minutes")

# Combine all dates
print("Combining results across all dates...")
geographic_df = pd.concat(geographic_df, axis=0, ignore_index=True)

# Add time variables
geographic_df['month'] = geographic_df.date_range_start.str.extract('^\d{4}-(\d{2})-\d{2}').astype(int)
geographic_df['year'] = geographic_df.date_range_start.str.extract('^(\d{4})-\d{2}-\d{2}').astype(int)

# Shorten column names to fit Stata limits
geographic_df.columns = geographic_df.columns.str.replace('nonbrandpoi_','ind_')
geographic_df.columns = geographic_df.columns.str.replace('brandpoi_','brn_')
geographic_df.columns = geographic_df.columns.str.replace('retailpoi_','ret_')
geographic_df.columns = geographic_df.columns.str.replace('remainpoi_','rem_')
geographic_df.to_parquet(
        f'{datadir}advan_tract_nearby_businesses.parquet',
        index=False, compression='zstd'
    )
print(f"Final dataset shape: {geographic_df.shape}")
print("Analysis complete!")