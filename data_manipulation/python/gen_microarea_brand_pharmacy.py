#########################################################################################################################################
# Calculate average/total allpoi nvisits within 200 or 500 yards. Also calculate tract average/total traffic, average/total traffic for business types.  
#########################################################################################################################################
import pathlib as path, pandas as pd, numpy as np, pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import pyarrow.dataset as ds
from datetime import timedelta
from pyarrow import csv
import os, re, gc, time
pd.options.mode.chained_assignment = None 

esttype = 'drug'
dist_list = [200, 500, 1000]


def gen_microarea_brand_pharmacy(esttype):
    # Sample drug stores.
    # drug_cols = ['placekey', 'location_name', 'street_address', 'city', 'region', 'zip', 'latitude', 'longitude', 'brands', 'tract']
    drug_cols = ['placekey', 'region','zip', 'latitude', 'longitude', 'brands', 'tract', 'cty', 'location_name','street_address', 'city','naics_code']
    drug_jan2020 = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_stores_final.dta')
    assert drug_jan2020[(drug_jan2020.tract.isnull()) | (drug_jan2020.tract.str.len()==0)].shape[0] == 0
    drug_jan2020 = drug_jan2020[drug_jan2020.date_range_start == '2020-01-01']
    drug_jan2020['cty'] = drug_jan2020.tract.str[:5]
    assert drug_jan2020[(drug_jan2020.zip.isnull()) ].shape[0] == 0
    drug_jan2020['latitude'] = pd.to_numeric(drug_jan2020['latitude'])
    drug_jan2020['longitude'] = pd.to_numeric(drug_jan2020['longitude'])
    drug_jan2020 = drug_jan2020[drug_cols]
    drug_jan2020['zip'] = drug_jan2020.zip.str.zfill(5)
    # Tract-level retail traffic and nearby store traffic calculation
    dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01','2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01',
             '2020-01-01','2020-02-01','2020-03-01','2020-04-01','2020-05-01','2020-06-01','2020-07-01','2020-08-01','2020-09-01','2020-10-01','2020-11-01','2020-12-01',
             '2021-01-01','2021-02-01','2021-03-01','2021-04-01','2021-05-01','2021-06-01','2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01',
             '2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01']
    # dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01','2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01']
    # dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01']
    # dates = ['2019-01-01','2019-02-01']
    # dates = ['2019-01-01']
    dist_df = []
    t1 = time.time()
    for d in dates:
        # d = '2019-01-01'
        allpoi_cols = ['placekey', 'region','zip', 'latitude', 'longitude', 'location_name','street_address', 'city','naics_code', 'tract', 'nvisits', 'brands']
        advan = pd.read_parquet('D:/pharmacy/data/advan/all_foottraffic_monthly_201901_202211_full.parquet', columns = allpoi_cols , filters = [('date_range_start', '==', d), ('geometry_type','!=','POINT'), ('naics_code','==','446110')])
        assert advan.shape[0] > 0 
        advan = advan[~advan.region.str.contains('^AS$|^GU$|^MP$|^PR$|^VI$',na=False)]
        advan['cty'] = advan.tract.str[:5]
        advan = advan[(~advan.zip.isnull()) ]
        advan['latitude'] = pd.to_numeric(advan['latitude'])
        advan['longitude'] = pd.to_numeric(advan['longitude'])
        advan['zip'] = advan.zip.str.zfill(5)
        advan['brand_drug'] = ((~advan.brands.isnull())).astype(int)
        advan['nonbrand_drug'] = ((advan.brands.isnull())).astype(int)
        #
        assert advan[(advan.brand_drug==0) & (advan.nonbrand_drug==0)].shape[0] == 0 
        # Mark grocery chain pharmacies and co-located pharmacies
        grocery_drug = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/grocery_drug_placekeys.dta')
        colocated_drug1 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_exact_drug_placekeys.dta')
        colocated_drug2 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_place9_drug_placekeys.dta')
        colocated_drug3 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_mod_drug_placekeys.dta')
        colocated_drug4 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/shared_polygon_drug_placekeys.dta')
        colocated_drug = pd.concat([colocated_drug1, colocated_drug2, colocated_drug3, colocated_drug4], axis=0).drop_duplicates()
        colocated_drug = colocated_drug[~colocated_drug.placekey.isin(grocery_drug.placekey)]
        #
        advan['grocery_drug'] = (advan.placekey.isin(grocery_drug.placekey)).astype(int)
        advan['colocated_drug'] = (advan.placekey.isin(colocated_drug.placekey)).astype(int)
        # Nearby traffic
        final_cols = ['placekey', 'location_name', 'street_address', 'city', 'region', 'cty', 'zip', 'tract', 'latitude', 'longitude', 'naics_code', 'nvisits', 'brand_drug', 'nonbrand_drug', 'grocery_drug', 'colocated_drug']
        advan = advan[final_cols]
        # Create nvisits and placekey variables separately for brands and nonbrands. Used for aggregation
        for brandtype in ['brand_drug', 'nonbrand_drug', 'grocery_drug', 'colocated_drug']:
            advan[f'placekey_{brandtype}'] = advan.placekey
            advan.loc[advan[f'{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
            advan[f'nvisits_{brandtype}'] = advan.nvisits
            advan.loc[advan[f'{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
        #
        drug_merged = drug_jan2020.merge(advan, on=['tract'], how='left')
        # drug_merged = drug_merged.drop('_merge',axis=1)
        #
        pd.options.display.float_format = '{:.6f}'.format
        drug_merged.rename(columns = {'placekey_y':'placekey','placekey_x':'placekey_drug'}, inplace=True)
        drug_merged['date_range_start'] = d
        drug_merged.loc[drug_merged.nvisits == 0 ,'nvisits'] = np.nan # No zero visits. Still want to include them in number of nearby stores.
        # Tract-level foot traffic and # poi
        assert drug_merged[(drug_merged.tract.str.len()!=11)].shape[0] == 0
        drug_merged['npoi_drug_tract'] = drug_merged.groupby('placekey_drug')['placekey'].transform('count')
        drug_merged['npoi_drug_brand_tract'] = drug_merged.groupby('placekey_drug')['placekey_brand_drug'].transform('count')
        drug_merged['npoi_drug_nonbrand_tract'] = drug_merged.groupby('placekey_drug')['placekey_nonbrand_drug'].transform('count')
        #
        drug_merged['drug_totnvisits_tract'] = drug_merged.groupby('placekey_drug')['nvisits'].transform('sum')
        drug_merged['drug_brand_totnvisits_tract'] = drug_merged.groupby('placekey_drug')['nvisits_brand_drug'].transform('sum')
        drug_merged['drug_nonbrand_totnvisits_tract'] = drug_merged.groupby('placekey_drug')['nvisits_nonbrand_drug'].transform('sum')
        #
        drug_merged['npoi_drug_poinm_tract'] = drug_merged.groupby('placekey_drug')['nvisits'].transform('count')
        drug_merged['npoi_drug_brand_poinm_tract'] = drug_merged.groupby('placekey_drug')['nvisits_brand_drug'].transform('count')
        drug_merged['npoi_drug_nonbrand_poinm_tract'] = drug_merged.groupby('placekey_drug')['nvisits_nonbrand_drug'].transform('count')
        #
        # Calculate nearby businesses
        drug_dist_combined = drug_merged[drug_merged.placekey_drug==drug_merged.placekey][['placekey_drug','date_range_start','tract','npoi_drug_tract','npoi_drug_brand_tract','npoi_drug_nonbrand_tract','drug_totnvisits_tract','drug_brand_totnvisits_tract','drug_nonbrand_totnvisits_tract','npoi_drug_poinm_tract','npoi_drug_brand_poinm_tract','npoi_drug_nonbrand_poinm_tract']].drop_duplicates()
        assert drug_dist_combined[drug_dist_combined.placekey_drug.duplicated()].shape[0] == 0
        dist_df.append(drug_dist_combined)
        del drug_dist_combined, advan
        gc.collect()
        t2 = time.time()
        print(d, (t2-t1)/60)
    # Nearby stores and average nvisits (2019)
    dist_df = pd.concat(dist_df, axis=0)
    return( [dist_df] )

drug2 = gen_microarea_brand_pharmacy('drug', 'adv_drug_stores_final')
# Nearby Traffic
drug_dist = drug2[0]
drug_dist['month'] = drug_dist.date_range_start.str.extract('^\d{4}-(\d{2})-\d{2}').astype(int)
drug_dist['year'] = drug_dist.date_range_start.str.extract('^(\d{4})-\d{2}-\d{2}').astype(int)
drug_dist.rename(columns = {'placekey_drug':'placekey'}, inplace=True)
drug_dist.to_parquet(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_pharmacies_tract.parquet',index=False, compression='zstd')
# drug_dist = pd.read_parquet(f'C:/Users/elliotoh/Box/analysis/adv_drug_pharmacies_tract.parquet')
drug_dist.columns = drug_dist.columns.str.replace('nonbrand','ind')
drug_dist.columns = drug_dist.columns.str.replace('brand','brn')
drug_dist.columns[drug_dist.columns.str.len()>32]
drug_dist.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_pharmacies_tract.dta',write_index=False, version=118)
drug_dist19 = drug_dist[drug_dist.year==2019]
drug_dist19.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_pharmacies_tract19.dta',write_index=False, version=118)

