#########################################################################################################################################
# Calculate average/total allpoi nvisits within 200 or 500 yards. Also calculate tract average/total traffic, average/total traffic for business types.  
#########################################################################################################################################
import pathlib as path, pandas as pd, numpy as np, pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import pyarrow.dataset as ds
from datetime import timedelta
from pyarrow import csv
import os, re, gc, time
pd.options.mode.chained_assignment = None 

def gen_surrounding_business_drug():
    # Sample drug stores.
    # drug_cols = ['placekey', 'location_name', 'street_address', 'city', 'region', 'zip', 'latitude', 'longitude', 'brands', 'tract']
    dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01','2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01',
             '2020-01-01','2020-02-01','2020-03-01','2020-04-01','2020-05-01','2020-06-01','2020-07-01','2020-08-01','2020-09-01','2020-10-01','2020-11-01','2020-12-01',
             '2021-01-01','2021-02-01','2021-03-01','2021-04-01','2021-05-01','2021-06-01','2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01',
             '2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01']
    dates = ['2019-04-01','2020-01-01','2020-04-01']
    t1 = time.time()
    dist_df = []
    for d in dates:
        # d = dates[0]
        allpoi_cols = ['placekey', 'date_range_start','region','zip', 'nvisits', 'latitude','longitude', 'location_name','street_address', 'city','naics_code', 'tract', 'brands']
        advan = pd.read_parquet('E:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet', columns = allpoi_cols , filters = [('date_range_start', '==', d), ('geometry_type','!=','POINT')])
        assert advan.shape[0] > 0 
        advan = advan[~advan.region.str.contains('^AS$|^GU$|^MP$|^PR$|^VI$',na=False)]
        advan['cty'] = advan.tract.str[:5]
        advan['zip'] = advan.zip.str.zfill(5)
        advan = advan[(~advan.zip.isnull()) ]
        advan.loc[advan.nvisits == 0 ,'nvisits'] = np.nan # No zero visits. Still want to include them in number of nearby stores.
        advan['latitude'] = pd.to_numeric(advan['latitude'])
        advan['longitude'] = pd.to_numeric(advan['longitude'])
        advan['zip'] = advan.zip.str.zfill(5)
        advan['d_retail'] = (advan.naics_code.str.contains('^4[45]|^7[12]|^81[12]')).astype(int) # Retail, arts/entertainment/recreation, accomodation and food services, other services (repair and maintenace/personal and laundry services). Excludes religious+NGOs (813) + private households (814).
        advan['d_brand'] = ((advan.d_retail==1) & (~advan.brands.isnull())).astype(int)
        advan['d_nonbrand'] = ((advan.d_retail==1) & (advan.brands.isnull())).astype(int)
        advan['d_remain'] = (advan.d_retail == 0).astype(int)
        advan['pharmacy'] = advan.naics_code.str.contains('446110',na=False).astype(int)
        advan['finance'] = advan.naics_code.str.contains('^52',na=False).astype(int)
        advan['finance_excbank'] = (advan.naics_code.str.contains('^52',na=False).astype(int) & ~advan.naics_code.str.contains('^52211',na=False).astype(int)) # Exclude banks
        advan['bank'] = (advan.naics_code.str.contains('^52211',na=False).astype(int)) 
        #
        # Variables needed: ndrug_tract, npoi_tract, all_tract_chg_exc4 (log difference for change in tract total foot traffic excluding store and bank), dln_brn_tract (log difference for average foot traffic for brand in tracts)
        # Nearby traffic
        final_cols = ['placekey', 'date_range_start','location_name', 'street_address', 'city', 'region', 'cty', 'zip', 'tract', 'latitude', 'longitude', 'naics_code', 'nvisits', 'd_retail', 'd_brand', 'd_nonbrand', 'd_remain',
                      'pharmacy','finance', 'finance_excbank', 'bank']
        advan = advan[final_cols]
        # Create nvisits and placekey variables separately for brands and nonbrands. Used for aggregation
        for brandtype in ['brand','nonbrand','retail','remain']:
            advan[f'placekey_{brandtype}'] = advan.placekey
            advan.loc[advan[f'd_{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
            advan[f'nvisits_{brandtype}'] = advan.nvisits
            advan.loc[advan[f'd_{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
            #
        for brandtype in ['pharmacy','finance','finance_excbank','bank']:
            advan[f'placekey_{brandtype}'] = advan.placekey
            advan.loc[advan[f'{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
            advan[f'nvisits_{brandtype}'] = advan.nvisits
            advan.loc[advan[f'{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
            #
        advan_tract = advan.groupby(['tract','date_range_start']).agg({
            'placekey':'count', 'nvisits':{'sum','mean','count'}, 
            'placekey_brand':'count', 'placekey_nonbrand':'count', 'placekey_retail':'count', 'placekey_remain':'count', 
            'pharmacy':'sum', 'nvisits_pharmacy':{'sum','mean','count'},
            'nvisits_finance':{'sum','mean','count'},
            'nvisits_finance_excbank':{'sum','mean','count'},
            'nvisits_bank':{'sum','mean','count'},
            'nvisits_brand':{'sum','mean','count'}, 'nvisits_nonbrand':{'sum','mean','count'}, 'nvisits_retail':{'sum','mean','count'}, 'nvisits_remain':{'sum','mean','count'}  
            }).reset_index()
        advan_tract.columns = advan_tract.columns.get_level_values(0) + ' ' + advan_tract.columns.get_level_values(1)
        advan_tract.columns = advan_tract.columns.str.strip()
        advan_tract.rename(columns = {
            'placekey count': f'npoi_tract',
            'placekey_brand count': f'brandnpoi_tract',
            'placekey_nonbrand count': f'nonbrandnpoi_tract',
            'placekey_retail count': f'retailnpoi_tract',
            'placekey_remain count': f'remainnpoi_tract',
            #
            'nvisits sum': f'allpoi_totnvisits_tract',
            'nvisits_brand sum': f'brandnpoi_totnvisits_tract',
            'nvisits_nonbrand sum': f'nonbrandnpoi_totnvisits_tract',
            'nvisits_retail sum': f'retailpoi_totnvisits_tract',
            'nvisits_remain sum': f'remainpoi_totnvisits_tract',
            #
            'nvisits mean': f'allpoi_avgnvisits_tract',
            'nvisits_brand mean': f'brandnpoi_avgnvisits_tract',
            'nvisits_nonbrand mean': f'nonbrandnpoi_avgnvisits_tract',
            'nvisits_retail mean': f'retailpoi_avgnvisits_tract',
            'nvisits_remain mean': f'remainpoi_avgnvisits_tract',
            #
            'nvisits count': f'allpoi_npoinm_tract',
            'nvisits_brand count': f'brandnpoi_npoinm_tract',
            'nvisits_nonbrand count': f'nonbrandnpoi_npoinm_tract',
            'nvisits_retail count': f'retailpoi_npoinm_tract',
            'nvisits_remain count': f'remainpoi_npoinm_tract',
            #
            'pharmacy sum': f'npoi_pharmacy_tract',
            'nvisits_pharmacy count': f'npoi_pharmacy_tract',
            'nvisits_pharmacy sum': f'pharmacy_totnvisits_tract',
            'nvisits_pharmacy mean': f'pharmacy_avgnvisits_tract',
            'nvisits_pharmacy count': f'pharmacy_npoinm_tract',
            #
            'nvisits_finance count': f'npoi_finance_tract',
            'nvisits_finance sum': f'finance_totnvisits_tract',
            'nvisits_finance mean': f'finance_avgnvisits_tract',
            'nvisits_finance count': f'finance_npoinm_tract',
            #
            'nvisits_finance_excbank count': f'npoi_finance_excbank_tract',
            'nvisits_finance_excbank sum': f'finance_excbank_totnvisits_tract',
            'nvisits_finance_excbank mean': f'finance_excbank_avgnvisits_tract',
            'nvisits_finance_excbank count': f'finance_excbank_npoinm_tract',
            #
            'nvisits_bank count': f'npoi_bank_tract',
            'nvisits_bank sum': f'bank_totnvisits_tract',
            'nvisits_bank mean': f'bank_avgnvisits_tract',
            'nvisits_bank count': f'bank_npoinm_tract'
        }, inplace=True)
        advan_tract.columns = advan_tract.columns.str.strip()
        assert (advan_tract[f'brandnpoi_tract'] + advan_tract[f'nonbrandnpoi_tract'] != advan_tract[f'retailnpoi_tract']).sum() == 0
        assert (advan_tract[f'brandnpoi_tract'] + advan_tract[f'nonbrandnpoi_tract'] + advan_tract[f'remainnpoi_tract'] != advan_tract[f'npoi_tract']).sum() == 0
        assert (advan_tract[f'brandnpoi_totnvisits_tract'] + advan_tract[f'nonbrandnpoi_totnvisits_tract'] != advan_tract[f'retailpoi_totnvisits_tract']).sum() == 0
        assert (advan_tract[f'brandnpoi_totnvisits_tract'] + advan_tract[f'nonbrandnpoi_totnvisits_tract'] + advan_tract[f'remainpoi_totnvisits_tract'] != advan_tract[f'allpoi_totnvisits_tract']).sum() == 0
        dist_df.append(advan_tract)
        del advan
        gc.collect()
        t2 = time.time()
        print(d, (t2-t1)/60)
    # Nearby stores and average nvisits (2019)
    dist_df = pd.concat(dist_df, axis=0)
    return( [dist_df] )

# drug_dist[['placekey_drug','nvisits','date_range_start','tract','npoi_200yd','allpoi_avgnvisits_200yd','allpoi_totnvisits_200yd','allpoi_npoinm_200yd']]
# drug_dist[['placekey_drug','nvisits','date_range_start','tract','npoi_tract','avgnvisits_tract','totnvisits_tract','npoinm_tract']]
# dist_list = [200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]
drug2 = gen_surrounding_business_drug()
# Nearby Traffic
drug_dist = drug2[0]
drug_dist['month'] = drug_dist.date_range_start.str.extract('^\d{4}-(\d{2})-\d{2}').astype(int)
drug_dist['year'] = drug_dist.date_range_start.str.extract('^(\d{4})-\d{2}-\d{2}').astype(int)
drug_dist.to_parquet(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_full_tract.parquet',index=False, compression='zstd')

a = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_full_tract.parquet')

drug_dist.columns = drug_dist.columns.str.replace('finance_excbank','finance_xbk')
drug_dist.columns = drug_dist.columns.str.replace('_xbk_','_xb_')
drug_dist.columns = drug_dist.columns.str.replace('bank','bank_adv')
drug_dist.columns = drug_dist.columns.str.replace('nonbrandnpoi_','ind_')
drug_dist.columns = drug_dist.columns.str.replace('brandnpoi_','brn_')
drug_dist.columns = drug_dist.columns.str.replace('retailpoi_','ret_')
drug_dist.columns = drug_dist.columns.str.replace('remainpoi_','rem_')
drug_dist.columns[drug_dist.columns.str.len()>32]
drug_dist.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_full_tract.dta',write_index=False, version=118)
# drug_dist19 = drug_dist[drug_dist.year==2019]
# # drug_dist19.to_stata(f'C:/Users/elliotoh/Box/analysis/adv_drug_nearby_businesses19_old_brandtype_earlyversion.dta',write_index=False, version=118)
# drug_dist19.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_businesses19_old_brandtype.dta',write_index=False, version=118)

