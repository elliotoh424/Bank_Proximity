#########################################################################################################################################
# Calculate average/total allpoi nvisits within 200 or 500 yards. Also calculate tract average/total traffic, average/total traffic for business types.  
#########################################################################################################################################
import pathlib as path, pandas as pd, numpy as np, pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import pyarrow.dataset as ds
from datetime import timedelta
from pyarrow import csv
import os, re, gc, time
pd.options.mode.chained_assignment = None 

def distance(s_lat, s_lng, e_lat, e_lng):
    # approximate radius of earth in km
    R = 6373 
    s_lat = s_lat*np.pi/180.0                      
    s_lng = np.deg2rad(s_lng)     
    e_lat = np.deg2rad(e_lat)                       
    e_lng = np.deg2rad(e_lng)  
    # Calculate distance    
    d = np.sin((e_lat - s_lat)/2)**2 + np.cos(s_lat)*np.cos(e_lat) * np.sin((e_lng - s_lng)/2)**2
    return (2 * R * np.arcsin(np.sqrt(d)))/1.60934 # Convert km to miles

esttype = 'drug'
dist_list = [200, 500, 1000]


def gen_surrounding_business_drug(esttype, dist_list):
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
    micro_df = [] 
    t1 = time.time()
    for d in dates:
        # d = '2019-01-01'
        allpoi_cols = ['placekey', 'region','zip', 'latitude', 'longitude', 'location_name','street_address', 'city','naics_code', 'tract', 'nvisits', 'brands']
        advan = pd.read_parquet('D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet', columns = allpoi_cols , filters = [('date_range_start', '==', d), ('geometry_type','!=','POINT')])
        assert advan.shape[0] > 0 
        advan = advan[~advan.region.str.contains('^AS$|^GU$|^MP$|^PR$|^VI$',na=False)]
        advan['cty'] = advan.tract.str[:5]
        advan = advan[(~advan.zip.isnull()) ]
        advan['latitude'] = pd.to_numeric(advan['latitude'])
        advan['longitude'] = pd.to_numeric(advan['longitude'])
        advan['zip'] = advan.zip.str.zfill(5)
        advan['d_retail'] = (advan.naics_code.str.contains('^4[45]|^7[12]|^81[12]')).astype(int) # Retail, arts/entertainment/recreation, accomodation and food services, other services (repair and maintenace/personal and laundry services). Excludes religious+NGOs (813) + private households (814).
        advan['d_brand'] = ((advan.d_retail==1) & (~advan.brands.isnull())).astype(int)
        advan['d_nonbrand'] = ((advan.d_retail==1) & (advan.brands.isnull())).astype(int)
        advan['d_remain'] = (advan.d_retail == 0).astype(int)
        #
        # Mark industries
        advan['grocery'] = advan.naics_code.str.contains('^4451|^4523',na=False).astype(int) # grocery stores, convenience stores, warehouse, dollar stores 
        advan['pharmacy'] = advan.naics_code.str.contains('446110',na=False).astype(int)
        advan['wholesale_retail'] = (advan.naics_code.str.contains('^4[245]',na=False) & (advan.grocery==0) & (advan.pharmacy == 0)).astype(int)
        advan['postoffice'] = (advan.naics_code.str.contains('^491',na=False)).astype(int)
        advan['finance'] = advan.naics_code.str.contains('^52',na=False).astype(int)
        advan['realestate_profservice'] = advan.naics_code.str.contains('^5[3456]',na=False).astype(int) # In order of most common establishments: real estate brokers, residential property managers
        advan['finance_excbank'] = (advan.naics_code.str.contains('^52',na=False).astype(int) & ~advan.naics_code.str.contains('^52211',na=False).astype(int)) # Exclude banks
        advan['bank'] = (advan.naics_code.str.contains('^52211',na=False).astype(int)) 
        advan['medical_healthcare'] = advan.naics_code.str.contains('^62',na=False).astype(int)
        advan['medical'] = advan.naics_code.str.contains('^62[123]',na=False).astype(int)
        advan['hotel_restaurant'] = advan.naics_code.str.contains('^72',na=False).astype(int)
        advan['hotel'] = advan.naics_code.str.contains('^721',na=False).astype(int)
        advan['restaurant'] = advan.naics_code.str.contains('^722',na=False).astype(int)
        advan['other_service'] = advan.naics_code.str.contains('^[567]1|^81[124]',na=False).astype(int) # information (51), education (61), arts/recreation (71), repair/maintenace (811), personal/laundry services (812), private household (814, nonexistent in advan)
        advan['religious_ngo'] = advan.naics_code.str.contains('^813',na=False).astype(int) # information (51), education (61), arts/recreation (71), repair/maintenace (811), personal/laundry services (812)
        advan['government'] = advan.naics_code.str.contains('^92',na=False).astype(int) 
        advan['other'] = ((advan.naics_code.str.contains('^11|^2[123]|^3[123]|^4[89]',na=False)) & (~advan.naics_code.str.contains('^491'))).astype(int) # agriculture (11), oil/gas/mining (21), utilities (22), construction (23), mfg (31-33), transport + warehouse (48-49). Excluding post office (491)
        assert advan[(advan.grocery==0) & (advan.pharmacy==0) & (advan.wholesale_retail==0) & (advan.postoffice==0) & (advan.finance==0) & (advan.realestate_profservice==0) & (advan.medical_healthcare==0) & (advan.hotel_restaurant==0) & (advan.other_service==0) & (advan.religious_ngo==0) & (advan.government==0) & (advan.other==0)].naics_code.drop_duplicates().shape[0] == 0 # Covers all NAICS
        # Mark grocery chain pharmacies and co-located pharmacies
        grocery_pharmacy = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/grocery_drug_placekeys.dta')
        colocated_pharmacy1 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_exact_drug_placekeys.dta')
        colocated_pharmacy2 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_place9_drug_placekeys.dta')
        colocated_pharmacy3 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/colocate_mod_drug_placekeys.dta')
        colocated_pharmacy4 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/shared_polygon_drug_placekeys.dta')
        colocated_pharmacy = pd.concat([colocated_pharmacy1, colocated_pharmacy2, colocated_pharmacy3, colocated_pharmacy4], axis=0).drop_duplicates()
        colocated_pharmacy = colocated_pharmacy[~colocated_pharmacy.placekey.isin(grocery_pharmacy.placekey)]
        #
        advan['grocery_pharmacy'] = (advan.placekey.isin(grocery_pharmacy.placekey)).astype(int)
        advan['colocated_pharmacy'] = (advan.placekey.isin(colocated_pharmacy.placekey)).astype(int)
        # Nearby traffic
        final_cols = ['placekey', 'location_name', 'street_address', 'city', 'region', 'cty', 'zip', 'tract', 'latitude', 'longitude', 'naics_code', 'nvisits', 'd_retail', 'd_brand', 'd_nonbrand', 'd_remain', 
        'grocery', 'pharmacy', 'wholesale_retail', 'postoffice', 'finance', 'realestate_profservice', 'finance_excbank', 'bank', 'medical_healthcare', 'medical', 'hotel_restaurant', 'hotel', 'restaurant', 'other_service', 'religious_ngo', 'government', 'other',         
        'grocery_pharmacy', 'colocated_pharmacy']
        advan = advan[final_cols]
        # Create nvisits and placekey variables separately for brands and nonbrands. Used for aggregation
        for brandtype in ['brand','nonbrand','retail','remain']:
            advan[f'placekey_{brandtype}'] = advan.placekey
            advan.loc[advan[f'd_{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
            advan[f'nvisits_{brandtype}'] = advan.nvisits
            advan.loc[advan[f'd_{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
            if brandtype != 'remain':
                advan[f'colocated_pharmacy_{brandtype}'] = advan.colocated_pharmacy
                advan.loc[advan[f'd_{brandtype}'] == 0, f'colocated_pharmacy_{brandtype}'] = np.nan
        #
        drug_merged = drug_jan2020.merge(advan, on=['zip'])
        # drug_merged = drug_merged.drop('_merge',axis=1)
        #
        pd.options.display.float_format = '{:.6f}'.format
        # calculate distance
        # X is for drug store, Y is for other stores
        drug_merged['dist_from_drug'] = distance(drug_merged['latitude_x'], drug_merged['longitude_x'], drug_merged['latitude_y'], drug_merged['longitude_y'])
        drug_merged['dist_from_drug'] = drug_merged['dist_from_drug']*1760 # Convert from miles to yard
        drug_merged.rename(columns = {'placekey_y':'placekey','placekey_x':'placekey_drug'}, inplace=True)
        drug_merged['date_range_start'] = d
        drug_merged.loc[drug_merged.nvisits == 0 ,'nvisits'] = np.nan # No zero visits. Still want to include them in number of nearby stores.
        # # Tract-level foot traffic and # poi
        # assert drug_merged[(drug_merged.tract_x.str.len()!=11) | (drug_merged.tract_y.str.len()!=11)].shape[0] == 0
        # drug_merged['npoi_tract'] = drug_merged.placekey
        # drug_merged.loc[drug_merged.tract_x != drug_merged.tract_y, 'npoi_tract'] = np.nan
        # drug_merged['npoi_tract'] = drug_merged.groupby('placekey_drug')['npoi_tract'].transform('count')
        # #
        # drug_merged['brandnpoi_tract'] = drug_merged.placekey
        # drug_merged.loc[drug_merged.d_brand == 0,'brandnpoi_tract'] = np.nan
        # drug_merged.loc[drug_merged.tract_x != drug_merged.tract_y, 'brandnpoi_tract'] = np.nan
        # drug_merged['brandnpoi_tract'] = drug_merged.groupby('placekey_drug')['brandnpoi_tract'].transform('count')
        # #
        # drug_merged['nonbrandnpoi_tract'] = drug_merged.placekey
        # drug_merged.loc[drug_merged.d_brand == 1,'nonbrandnpoi_tract'] = np.nan
        # drug_merged.loc[drug_merged.tract_x != drug_merged.tract_y, 'nonbrandnpoi_tract'] = np.nan
        # drug_merged['nonbrandnpoi_tract'] = drug_merged.groupby('placekey_drug')['nonbrandnpoi_tract'].transform('count')
        # assert drug_merged[drug_merged.brandnpoi_tract + drug_merged.nonbrandnpoi_tract != drug_merged.npoi_tract].shape[0] == 0
        # #
        # drug_merged['nvisits_tract'] = drug_merged.nvisits
        # drug_merged.loc[drug_merged.tract_x != drug_merged.tract_y, 'nvisits_tract'] = np.nan
        # drug_merged['avgnvisits_tract'] = drug_merged.groupby('placekey_drug')['nvisits_tract'].transform('mean')
        # drug_merged['totnvisits_tract'] = drug_merged.groupby('placekey_drug')['nvisits_tract'].transform('sum')
        # drug_merged['npoinm_tract'] = drug_merged.groupby('placekey_drug')['nvisits_tract'].transform('count')
        #
        # Calculate nearby businesses
        drug_dist_combined = drug_merged[drug_merged.placekey_drug==drug_merged.placekey][['placekey_drug','nvisits','date_range_start','tract_x']]
        drug_dist_combined.rename(columns = {'tract_x':'tract'}, inplace=True)
        assert drug_dist_combined[drug_dist_combined.placekey_drug.duplicated()].shape[0] == 0
        print('start', drug_dist_combined.shape[0])
        # drug_dist_combined.rename(columns={'nvisits':'nvisits_drugstore'}, inplace=True)
        for i in range(len(dist_list)):
            dist = dist_list[i]
            # Calculate nearby poi and traffic per drug store
            drug_merged[f'within_{dist}yd'] = (drug_merged.dist_from_drug<=dist).astype(int)
            drug_merged.loc[drug_merged.nvisits == 0 ,'nvisits'] = np.nan # No zero visits. Still want to include them in number of nearby stores.
            drug_within_dist = drug_merged[(drug_merged[f'within_{dist}yd']==1)]
            drug_allpoi = drug_within_dist.groupby(['placekey_drug','date_range_start']).agg({
                'placekey':'count', 'nvisits':{'sum','mean','count'}, 'grocery_pharmacy':'sum', 
                'colocated_pharmacy':'sum', 'colocated_pharmacy_brand':'sum', 'colocated_pharmacy_nonbrand':'sum', 'colocated_pharmacy_retail':'sum',
                'placekey_brand':'count', 'placekey_nonbrand':'count', 'placekey_retail':'count', 'placekey_remain':'count',  
                'nvisits_brand':{'sum','mean','count'}, 'nvisits_nonbrand':{'sum','mean','count'}, 'nvisits_retail':{'sum','mean','count'}, 'nvisits_remain':{'sum','mean','count'} 
                }).reset_index()
            drug_allpoi.columns = drug_allpoi.columns.get_level_values(0) + ' ' + drug_allpoi.columns.get_level_values(1)
            drug_allpoi.columns = drug_allpoi.columns.str.strip()
            drug_allpoi.rename(columns = {
                'placekey count':f'npoi_{dist}yd', 'placekey_brand count':f'brandnpoi_{dist}yd', 'placekey_nonbrand count':f'nonbrandnpoi_{dist}yd', 'placekey_retail count':f'retailnpoi_{dist}yd', 'placekey_remain count':f'remainnpoi_{dist}yd',
                'nvisits sum':f'allpoi_totnvisits_{dist}yd', 'nvisits_brand sum':f'brandpoi_totnvisits_{dist}yd', 'nvisits_nonbrand sum':f'nonbrandpoi_totnvisits_{dist}yd', 'nvisits_retail sum':f'retailpoi_totnvisits_{dist}yd', 'nvisits_remain sum':f'remainpoi_totnvisits_{dist}yd', 
                'nvisits mean':f'allpoi_avgnvisits_{dist}yd', 'nvisits_brand mean':f'brandpoi_avgnvisits_{dist}yd', 'nvisits_nonbrand mean':f'nonbrandpoi_avgnvisits_{dist}yd', 'nvisits_retail mean':f'retailpoi_avgnvisits_{dist}yd', 'nvisits_remain mean':f'remainpoi_avgnvisits_{dist}yd', 
                'nvisits count':f'allpoi_npoinm_{dist}yd', 'nvisits_brand count':f'brandpoi_npoinm_{dist}yd', 'nvisits_nonbrand count':f'nonbrandpoi_npoinm_{dist}yd', 'nvisits_retail count':f'retailpoi_npoinm_{dist}yd' , 'nvisits_remain count':f'remainpoi_npoinm_{dist}yd' ,
                'grocery_pharmacy sum':f'ngrocery_pharmacy_{dist}yd', 
                'colocated_pharmacy sum':f'ncolocated_pharmacy_{dist}yd', 'colocated_pharmacy_brand sum':f'brandpoi_ncolocated_pharmacy_{dist}yd', 'colocated_pharmacy_nonbrand sum':f'nonbrandpoi_ncolocated_pharmacy_{dist}yd', 'colocated_pharmacy_retail sum':f'retailpoi_ncolocated_pharmacy_{dist}yd'
                }, inplace=True)
            drug_allpoi.columns = drug_allpoi.columns.str.strip()
            assert (drug_allpoi[f'brandnpoi_{dist}yd'] + drug_allpoi[f'nonbrandnpoi_{dist}yd'] != drug_allpoi[f'retailnpoi_{dist}yd']).sum() == 0
            assert (drug_allpoi[f'brandnpoi_{dist}yd'] + drug_allpoi[f'nonbrandnpoi_{dist}yd'] + drug_allpoi[f'remainnpoi_{dist}yd'] != drug_allpoi[f'npoi_{dist}yd']).sum() == 0
            assert (drug_allpoi[f'brandpoi_totnvisits_{dist}yd'] + drug_allpoi[f'nonbrandpoi_totnvisits_{dist}yd'] != drug_allpoi[f'retailpoi_totnvisits_{dist}yd']).sum() == 0
            assert (drug_allpoi[f'brandpoi_totnvisits_{dist}yd'] + drug_allpoi[f'nonbrandpoi_totnvisits_{dist}yd'] + drug_allpoi[f'remainpoi_totnvisits_{dist}yd'] != drug_allpoi[f'allpoi_totnvisits_{dist}yd']).sum() == 0
            if dist == 200:
                print('drug_dist', drug_allpoi.columns.tolist())
            #
            drug_dist_combined = drug_dist_combined.merge(drug_allpoi, on=['placekey_drug','date_range_start'], how='outer', indicator=True)
            assert drug_dist_combined[drug_dist_combined._merge=='right_only'].shape[0] == 0 # All matches because own store is included
            drug_dist_combined = drug_dist_combined.drop('_merge',axis=1)
            # Check this line
            for c in ['grocery', 'pharmacy', 'wholesale_retail', 'postoffice', 'finance', 'realestate_profservice', 'finance_excbank', 'bank', 'medical_healthcare', 'medical', 'hotel_restaurant', 'hotel', 'restaurant', 'other_service', 'religious_ngo', 'government', 'other']:
                drug_dist = drug_within_dist[drug_within_dist[c]==1]
                # drug_dist = drug_dist[drug_dist.placekey_drug!=drug_dist.placekey]
                # keep_cols = ['placekey_drug', 'placekey']
                # drug_dist.sort_values(['placekey_drug','placekey'], inplace=True)
                drug_dist = drug_dist.groupby(['placekey_drug','date_range_start']).agg({'placekey':'count', 'nvisits':{'mean','sum','count'} }).reset_index()
                drug_dist.columns = drug_dist.columns.get_level_values(0) + ' ' + drug_dist.columns.get_level_values(1)
                drug_dist.rename(columns = {'placekey count':f'npoi_{c}_{dist}yd','nvisits sum':f'{c}_totnvisits_{dist}yd', 'nvisits mean':f'{c}_avgnvisits_{dist}yd', 'nvisits count':f'{c}_npoinm_{dist}yd'}, inplace=True)
                drug_dist.columns = drug_dist.columns.str.strip()
                drug_dist_combined = drug_dist_combined.merge(drug_dist, on=['placekey_drug','date_range_start'], how='outer', indicator=True)
                # print(dist, c, drug_dist_combined.shape[0])
                #
                assert drug_dist_combined[drug_dist_combined._merge=='right_only'].shape[0] == 0
                assert drug_dist_combined[drug_dist_combined.placekey_drug.duplicated()].shape[0] == 0
                # drug_dist_combined._merge.value_counts()
                # [drug_dist_combined._merge=='left_only']
                # print(drug_dist_combined._merge.value_counts())
                drug_dist_combined = drug_dist_combined.drop('_merge',axis=1)
                # t2 = time.time()
                # print(dist, drug_dist_combined.columns.tolist())
                # drug_dist_combined[['placekey_drug','nvisits','date_range_start','tract','npoi_200yd','allpoi_avgnvisits_200yd','allpoi_totnvisits_200yd','allpoi_npoinm_200yd']]
        dist_df.append(drug_dist_combined)
        del drug_dist, drug_dist_combined, advan
        gc.collect()
        t2 = time.time()
        print(d, (t2-t1)/60)
    # Nearby stores and average nvisits (2019)
    dist_df = pd.concat(dist_df, axis=0)
    return( [dist_df] )

# drug_dist[['placekey_drug','nvisits','date_range_start','tract','npoi_200yd','allpoi_avgnvisits_200yd','allpoi_totnvisits_200yd','allpoi_npoinm_200yd']]
# drug_dist[['placekey_drug','nvisits','date_range_start','tract','npoi_tract','avgnvisits_tract','totnvisits_tract','npoinm_tract']]
# dist_list = [200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]
dist_list = [100, 200, 500, 1000]
drug2 = gen_surrounding_business_drug('drug', 'adv_drug_stores_final', dist_list)
# Nearby Traffic
drug_dist = drug2[0]
drug_dist['month'] = drug_dist.date_range_start.str.extract('^\d{4}-(\d{2})-\d{2}').astype(int)
drug_dist['year'] = drug_dist.date_range_start.str.extract('^(\d{4})-\d{2}-\d{2}').astype(int)
drug_dist.rename(columns = {'placekey_drug':'placekey'}, inplace=True)
drug_dist.to_parquet(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_businesses_old_brandtype.parquet',index=False, compression='zstd')
drug_dist.columns = drug_dist.columns.str.replace('wholesale_retail','whsale_ret')
drug_dist.columns = drug_dist.columns.str.replace('realestate_profservice','re_prosvc')
drug_dist.columns = drug_dist.columns.str.replace('finance_excbank','finance_xbk')
drug_dist.columns = drug_dist.columns.str.replace('_xbk_','_xb_')
drug_dist.columns = drug_dist.columns.str.replace('medical_healthcare','medhealth')
drug_dist.columns = drug_dist.columns.str.replace('hotel_restaurant','hotel_rest')
drug_dist.columns = drug_dist.columns.str.replace('other_service','othersvc')
drug_dist.columns = drug_dist.columns.str.replace('bank','bank_adv')
drug_dist.columns = drug_dist.columns.str.replace('nonbrandpoi_','ind_')
drug_dist.columns = drug_dist.columns.str.replace('brandpoi_','brn_')
drug_dist.columns = drug_dist.columns.str.replace('retailpoi_','ret_')
drug_dist.columns = drug_dist.columns.str.replace('remainpoi_','rem_')
drug_dist.columns[drug_dist.columns.str.len()>32]
drug_dist.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_businesses_old_brandtype.dta',write_index=False, version=118)
drug_dist19 = drug_dist[drug_dist.year==2019]
# drug_dist19.to_stata(f'C:/Users/elliotoh/Box/analysis/adv_drug_nearby_businesses19_old_brandtype_earlyversion.dta',write_index=False, version=118)
drug_dist19.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_businesses19_old_brandtype.dta',write_index=False, version=118)

