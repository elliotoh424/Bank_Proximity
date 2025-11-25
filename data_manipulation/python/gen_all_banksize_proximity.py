import pandas as pd
import numpy as np
import duckdb
import time
import gc
import re
pd.options.display.float_format = '{:.6f}'.format

def distance(s_lat, s_lng, e_lat, e_lng):
    """Calculate distance between two points in miles, then convert to yards"""
    # approximate radius of earth in km
    R = 6373
    s_lat = s_lat*np.pi/180.0
    s_lng = np.deg2rad(s_lng)
    e_lat = np.deg2rad(e_lat)
    e_lng = np.deg2rad(e_lng)
    # Calculate distance
    d = np.sin((e_lat - s_lat)/2)**2 + np.cos(s_lat)*np.cos(e_lat) * np.sin((e_lng - s_lng)/2)**2
    return (2 * R * np.arcsin(np.sqrt(d)))/1.60934 # Convert km to miles

def gen_surrounding_business_banks(dist_list):
    """
    Generate microarea features around bank branches similar to how it was done for drug stores
    """
    
	# Load bank branch data (from first code's SOD processing)
	print("Loading bank branch data...")
	relevant_cols = ['rssdid', 'brnum', 'namefull', 'cb', 'latitude', 'longitude','zipbr','GEOID10']
	sod = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10_new.xlsx', 
						usecols=relevant_cols, dtype=str)
	sod.rename(columns={'GEOID10':'tract', 'zipbr':'zip'}, inplace=True)

	# Load additional SOD data
	sod_vars = ['RSSDID','BRNUM','ASSET','SIMS_ESTABLISHED_DATE','ADDRESBR','CITYBR','STALPBR','ZIPBR']
	sod1 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2019.csv', 
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
	#
	sod_coordinates = sod[['bank_branch_id','rssdid','brnum','zip','latitude','longitude','mega_asset','nonmega_asset','cb_asset']]	
	sod_merged = sod.merge(sod_coordinates, on='zip', how='left', indicator=True, suffixes=('_bank','_other'))
	assert sod_merged[sod_merged._merge!='both'].shape[0] == 0
	sod_merged = sod_merged.drop('_merge',axis=1)
	sod_merged['dist_btw_banks'] = distance(sod_merged.latitude_bank, sod_merged.longitude_bank, sod_merged.latitude_other, sod_merged.longitude_other)
	sod_merged['dist_btw_banks'] = sod_merged['dist_btw_banks'] * 1760  # Convert to yards
	sod_merged = sod_merged[sod_merged.bank_branch_id_bank != sod_merged.bank_branch_id_other]
	assert sod_merged.dist_btw_banks.isnull().sum() == 0
	for d in dist_list:
		sod_merged[f'mega_{d}yd'] = ((sod_merged.dist_btw_banks <= d) & (sod_merged.mega_asset_other == 1)).astype(float)
		sod_merged[f'nonmega_{d}yd'] = ((sod_merged.dist_btw_banks <= d)  & (sod_merged.nonmega_asset_other == 1)).astype(float)
		sod_merged[f'cb_{d}yd'] = ((sod_merged.dist_btw_banks <= d)  & (sod_merged.cb_asset_other == 1)).astype(float)
		#		
		sod_merged[f'mega_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'mega_{d}yd'].transform('max')
		sod_merged[f'nmega_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'mega_{d}yd'].transform('sum')
		sod_merged[f'nonmega_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'nonmega_{d}yd'].transform('max')
		sod_merged[f'nnonmega_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'nonmega_{d}yd'].transform('sum')
		sod_merged[f'cb_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'cb_{d}yd'].transform('max')
		sod_merged[f'ncb_{d}yd'] = sod_merged.groupby('bank_branch_id_bank')[f'cb_{d}yd'].transform('sum')

	#
	keep_cols = ['bank_branch_id_bank', 
	'mega_100yd', 'nonmega_100yd', 'cb_100yd', 'nmega_100yd', 'nnonmega_100yd', 'ncb_100yd',
	'mega_200yd', 'nonmega_200yd', 'cb_200yd', 'nmega_200yd', 'nnonmega_200yd', 'ncb_200yd', 
	'mega_500yd', 'nonmega_500yd', 'cb_500yd', 'nmega_500yd', 'nnonmega_500yd', 'ncb_500yd',
	'mega_1000yd', 'nonmega_1000yd', 'cb_1000yd', 'nmega_1000yd', 'nnonmega_1000yd', 'ncb_1000yd'	
	]
	sod_merged = sod_merged[keep_cols].drop_duplicates()
	assert sod_merged[sod_merged.bank_branch_id_bank.duplicated()].shape[0] == 0
	sod_merged.rename(columns = {'bank_branch_id_bank':'bank_branch_id'}, inplace=True)
	#
	sod = sod.merge(sod_merged,on ='bank_branch_id',how='left', indicator=True)
	print('sod-sod_merged merge:',sod._merge.value_counts())
	sod = sod.drop('_merge',axis=1)
	for c in ['mega_100yd', 'nonmega_100yd', 'cb_100yd', 'nmega_100yd', 'nnonmega_100yd', 'ncb_100yd',
			'mega_200yd', 'nonmega_200yd', 'cb_200yd', 'nmega_200yd', 'nnonmega_200yd', 'ncb_200yd', 
			'mega_500yd', 'nonmega_500yd', 'cb_500yd', 'nmega_500yd', 'nnonmega_500yd', 'ncb_500yd',
			'mega_1000yd', 'nonmega_1000yd', 'cb_1000yd', 'nmega_1000yd', 'nnonmega_1000yd', 'ncb_1000yd']:
		sod[c] = sod[c].fillna(0)
	
	sod.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/sod_processed_.parquet', index=False, compression='zstd')
	
    # Time periods to analyze
    dates = ['2019-01-01','2019-02-01','2019-03-01','2019-04-01','2019-05-01','2019-06-01',
             '2019-07-01','2019-08-01','2019-09-01','2019-10-01','2019-11-01','2019-12-01',
             '2020-01-01','2020-02-01','2020-03-01','2020-04-01','2020-05-01','2020-06-01',
             '2020-07-01','2020-08-01','2020-09-01','2020-10-01','2020-11-01','2020-12-01',
             '2021-01-01','2021-02-01','2021-03-01','2021-04-01','2021-05-01','2021-06-01',
             '2021-07-01','2021-08-01','2021-09-01','2021-10-01','2021-11-01','2021-12-01',
             '2022-01-01','2022-02-01','2022-03-01','2022-04-01','2022-05-01','2022-06-01',
             '2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01']
    # dates = ['2019-01-01','2019-02-01']
    # dates = ['2019-01-01']

    dist_df = []
    t1 = time.time()
    
    for d in dates:
      # d = '2019-01-01'
      print(f"Processing date: {d}")
      
      # Load all POI data for this date
      allpoi_cols = ['placekey', 'region','zip', 'latitude', 'longitude', 'location_name',
                    'street_address', 'city','naics_code', 'tract', 'nvisits', 'brands']
      
      advan = pd.read_parquet('E:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet', 
                              columns=allpoi_cols, 
                              filters=[('date_range_start', '==', d), ('geometry_type','!=','POINT')])
      assert advan.shape[0] > 0
      
      # Clean data
      advan = advan[~advan.region.str.contains('^AS$|^GU$|^MP$|^PR$|^VI$', na=False)]
      advan['cty'] = advan.tract.str[:5]
      advan = advan[(~advan.zip.isnull())]
      advan['latitude'] = pd.to_numeric(advan['latitude'])
      advan['longitude'] = pd.to_numeric(advan['longitude'])
      advan['zip'] = advan.zip.str.zfill(5)
      
      # Create business type indicators
      advan['d_retail'] = (advan.naics_code.str.contains('^4[45]|^7[12]|^81[12]')).astype(int)
      advan['d_brand'] = ((advan.d_retail==1) & (~advan.brands.isnull())).astype(int)
      advan['d_nonbrand'] = ((advan.d_retail==1) & (advan.brands.isnull())).astype(int)
      advan['d_remain'] = (advan.d_retail == 0).astype(int)
      
      # Mark specific industries
      advan['grocery'] = advan.naics_code.str.contains('^4451|^4523', na=False).astype(int)
      advan['pharmacy'] = advan.naics_code.str.contains('446110', na=False).astype(int)
      advan['wholesale_retail'] = (advan.naics_code.str.contains('^4[245]', na=False) & 
                                  (advan.grocery==0) & (advan.pharmacy == 0)).astype(int)
      advan['postoffice'] = (advan.naics_code.str.contains('^491', na=False)).astype(int)
      advan['finance'] = advan.naics_code.str.contains('^52', na=False).astype(int)
      advan['realestate_profservice'] = advan.naics_code.str.contains('^5[3456]', na=False).astype(int)
      advan['finance_excbank'] = (advan.naics_code.str.contains('^52', na=False).astype(int) & 
                                  ~advan.naics_code.str.contains('^52211', na=False).astype(int))
      advan['bank'] = (advan.naics_code.str.contains('^52211', na=False).astype(int))
      advan['medical_healthcare'] = advan.naics_code.str.contains('^62', na=False).astype(int)
      advan['medical'] = advan.naics_code.str.contains('^62[123]', na=False).astype(int)
      advan['hotel_restaurant'] = advan.naics_code.str.contains('^72', na=False).astype(int)
      advan['hotel'] = advan.naics_code.str.contains('^721', na=False).astype(int)
      advan['restaurant'] = advan.naics_code.str.contains('^722', na=False).astype(int)
      advan['other_service'] = advan.naics_code.str.contains('^[567]1|^81[124]', na=False).astype(int)
      advan['religious_ngo'] = advan.naics_code.str.contains('^813', na=False).astype(int)
      advan['government'] = advan.naics_code.str.contains('^92', na=False).astype(int)
      advan['other'] = ((advan.naics_code.str.contains('^11|^2[123]|^3[123]|^4[89]', na=False)) & 
                        (~advan.naics_code.str.contains('^491'))).astype(int)
      
      # Load co-located pharmacy data (similar to drug store analysis)
      try:
          grocery_pharmacy = pd.read_stata('E:/pharmacy/advan/grocery_drug_placekeys.dta')
          colocated_pharmacy1 = pd.read_stata('E:/pharmacy/advan/colocate_exact_drug_placekeys.dta')
          colocated_pharmacy2 = pd.read_stata('E:/pharmacy/advan/colocate_place9_drug_placekeys.dta')
          colocated_pharmacy3 = pd.read_stata('E:/pharmacy/advan/colocate_mod_drug_placekeys.dta')
          colocated_pharmacy4 = pd.read_stata('E:/pharmacy/advan/shared_polygon_drug_placekeys.dta')
          colocated_pharmacy = pd.concat([colocated_pharmacy1, colocated_pharmacy2, colocated_pharmacy3, colocated_pharmacy4], axis=0).drop_duplicates()
          colocated_pharmacy = colocated_pharmacy[~colocated_pharmacy.placekey.isin(grocery_pharmacy.placekey)]
          
          advan['grocery_pharmacy'] = (advan.placekey.isin(grocery_pharmacy.placekey)).astype(int)
          advan['colocated_pharmacy'] = (advan.placekey.isin(colocated_pharmacy.placekey)).astype(int)
      except:
          print("Warning: Could not load pharmacy co-location data, setting to 0")
          advan['grocery_pharmacy'] = 0
          advan['colocated_pharmacy'] = 0
      
      # Create brand-specific variables for aggregation
      for brandtype in ['brand','nonbrand','retail','remain']:
          advan[f'placekey_{brandtype}'] = advan.placekey
          advan.loc[advan[f'd_{brandtype}'] == 0, f'placekey_{brandtype}'] = np.nan
          advan[f'nvisits_{brandtype}'] = advan.nvisits
          advan.loc[advan[f'd_{brandtype}'] == 0, f'nvisits_{brandtype}'] = np.nan
          if brandtype != 'remain':
              advan[f'colocated_pharmacy_{brandtype}'] = advan.colocated_pharmacy
              advan.loc[advan[f'd_{brandtype}'] == 0, f'colocated_pharmacy_{brandtype}'] = np.nan
      
        # Merge bank branches with all POI data by zip code
        bank_merged = sod.merge(advan, on=['zip'], suffixes=('_bank', '_poi'))
        
        # Calculate distance from bank branch to each POI
        bank_merged['dist_from_bank'] = distance(bank_merged['latitude_bank'], bank_merged['longitude_bank'], 
                                               bank_merged['latitude_poi'], bank_merged['longitude_poi'])
        bank_merged['dist_from_bank'] = bank_merged['dist_from_bank'] * 1760  # Convert to yards
        
        bank_merged['date_range_start'] = d
        bank_merged.loc[bank_merged.nvisits == 0, 'nvisits'] = np.nan  # Set zero visits to NaN
        
        # Initialize results for this date
        bank_dist_combined = sod[['bank_branch_id', 'rssdid', 'brnum', 'asset', 'big4_asset', 'large4_asset', 
                                 'mega_asset', 'cb_asset', 'nonmega_asset', 'stcntybr', 'namefull']].copy()
        bank_dist_combined['date_range_start'] = d
        
        print(f"Processing {len(dist_list)} distance thresholds for {len(bank_dist_combined)} bank branches")
        
        # Calculate features for each distance threshold
        for dist in dist_list:
            print(f"  Processing {dist} yard radius...")
            
            # Filter to POIs within distance
            bank_within_dist = bank_merged[bank_merged.dist_from_bank <= dist].copy()
            
            # Aggregate all POI features by bank branch
            bank_allpoi = bank_within_dist.groupby(['bank_branch_id','date_range_start']).agg({
                'placekey': 'count', 
                'nvisits': ['sum','mean','count'], 
                'grocery_pharmacy': 'sum',
                'colocated_pharmacy': 'sum', 
                'colocated_pharmacy_brand': 'sum', 
                'colocated_pharmacy_nonbrand': 'sum', 
                'colocated_pharmacy_retail': 'sum',
                'placekey_brand': 'count', 
                'placekey_nonbrand': 'count', 
                'placekey_retail': 'count', 
                'placekey_remain': 'count',  
                'nvisits_brand': ['sum','mean','count'], 
                'nvisits_nonbrand': ['sum','mean','count'], 
                'nvisits_retail': ['sum','mean','count'], 
                'nvisits_remain': ['sum','mean','count']
            }).reset_index()
            
            # Flatten column names
            bank_allpoi.columns = bank_allpoi.columns.get_level_values(0) + ' ' + bank_allpoi.columns.get_level_values(1)
            bank_allpoi.columns = bank_allpoi.columns.str.strip()
            
            # Rename columns with distance suffix
            bank_allpoi.rename(columns={
                'placekey count': f'npoi_{dist}yd', 
                'placekey_brand count': f'brandnpoi_{dist}yd', 
                'placekey_nonbrand count': f'nonbrandnpoi_{dist}yd', 
                'placekey_retail count': f'retailnpoi_{dist}yd', 
                'placekey_remain count': f'remainnpoi_{dist}yd',
                'nvisits sum': f'allpoi_totnvisits_{dist}yd', 
                'nvisits_brand sum': f'brandpoi_totnvisits_{dist}yd', 
                'nvisits_nonbrand sum': f'nonbrandpoi_totnvisits_{dist}yd', 
                'nvisits_retail sum': f'retailpoi_totnvisits_{dist}yd', 
                'nvisits_remain sum': f'remainpoi_totnvisits_{dist}yd',
                'nvisits mean': f'allpoi_avgnvisits_{dist}yd', 
                'nvisits_brand mean': f'brandpoi_avgnvisits_{dist}yd', 
                'nvisits_nonbrand mean': f'nonbrandpoi_avgnvisits_{dist}yd', 
                'nvisits_retail mean': f'retailpoi_avgnvisits_{dist}yd', 
                'nvisits_remain mean': f'remainpoi_avgnvisits_{dist}yd',
                'nvisits count': f'allpoi_npoinm_{dist}yd', 
                'nvisits_brand count': f'brandpoi_npoinm_{dist}yd', 
                'nvisits_nonbrand count': f'nonbrandpoi_npoinm_{dist}yd', 
                'nvisits_retail count': f'retailpoi_npoinm_{dist}yd', 
                'nvisits_remain count': f'remainpoi_npoinm_{dist}yd',
                'grocery_pharmacy sum': f'ngrocery_pharmacy_{dist}yd',
                'colocated_pharmacy sum': f'ncolocated_pharmacy_{dist}yd', 
                'colocated_pharmacy_brand sum': f'brandpoi_ncolocated_pharmacy_{dist}yd', 
                'colocated_pharmacy_nonbrand sum': f'nonbrandpoi_ncolocated_pharmacy_{dist}yd', 
                'colocated_pharmacy_retail sum': f'retailpoi_ncolocated_pharmacy_{dist}yd'
            }, inplace=True)
            
            # Merge with main results
            bank_dist_combined = bank_dist_combined.merge(bank_allpoi, on=['bank_branch_id','date_range_start'], 
                                                        how='left')
            
            # Calculate industry-specific features
            industry_list = ['grocery', 'pharmacy', 'wholesale_retail', 'postoffice', 'finance', 
                           'realestate_profservice', 'finance_excbank', 'bank', 'medical_healthcare', 
                           'medical', 'hotel_restaurant', 'hotel', 'restaurant', 'other_service', 
                           'religious_ngo', 'government', 'other']
            
            for industry in industry_list:
                industry_data = bank_within_dist[bank_within_dist[industry]==1]
                if len(industry_data) > 0:
                    industry_agg = industry_data.groupby(['bank_branch_id','date_range_start']).agg({
                        'placekey': 'count', 
                        'nvisits': ['mean','sum','count']
                    }).reset_index()
                    
                    industry_agg.columns = industry_agg.columns.get_level_values(0) + ' ' + industry_agg.columns.get_level_values(1)
                    industry_agg.rename(columns={
                        'placekey count': f'npoi_{industry}_{dist}yd',
                        'nvisits sum': f'{industry}_totnvisits_{dist}yd', 
                        'nvisits mean': f'{industry}_avgnvisits_{dist}yd', 
                        'nvisits count': f'{industry}_npoinm_{dist}yd'
                    }, inplace=True)
                    industry_agg.columns = industry_agg.columns.str.strip()
                    
                    bank_dist_combined = bank_dist_combined.merge(industry_agg, 
                                                                on=['bank_branch_id','date_range_start'], 
                                                                how='left')
        
        # Fill NaN values with 0 for count variables
        count_cols = [col for col in bank_dist_combined.columns if 'npoi_' in col or '_npoinm_' in col]
        bank_dist_combined[count_cols] = bank_dist_combined[count_cols].fillna(0)
        
        dist_df.append(bank_dist_combined)
        
        # Clean up memory
        del bank_merged, bank_within_dist, advan
        gc.collect()
        
        t2 = time.time()
        print(f"Completed {d} in {(t2-t1)/60:.2f} minutes")
    
    # Combine all dates
    print("Combining results across all dates...")
    dist_df = pd.concat(dist_df, axis=0, ignore_index=True)
    
    return dist_df

# Main execution
if __name__ == "__main__":
    # Set distance thresholds (in yards)
    dist_list = [100, 200, 500, 1000]
    
    print("Starting bank branch microarea analysis...")
    bank_microarea_data = gen_surrounding_business_banks(dist_list)
    
    # Add time variables
    bank_microarea_data['month'] = bank_microarea_data.date_range_start.str.extract('^\d{4}-(\d{2})-\d{2}').astype(int)
    bank_microarea_data['year'] = bank_microarea_data.date_range_start.str.extract('^(\d{4})-\d{2}-\d{2}').astype(int)
    
    # Shorten column names to fit Stata limits
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('wholesale_retail','whsale_ret')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('realestate_profservice','re_prosvc')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('finance_excbank','finance_xbk')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('medical_healthcare','medhealth')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('hotel_restaurant','hotel_rest')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('other_service','othersvc')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('nonbrandpoi_','ind_')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('brandpoi_','brn_')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('retailpoi_','ret_')
    bank_microarea_data.columns = bank_microarea_data.columns.str.replace('remainpoi_','rem_')
    
    # Check for long column names
    long_cols = bank_microarea_data.columns[bank_microarea_data.columns.str.len() > 32]
    if len(long_cols) > 0:
        print("Warning: Some column names are longer than 32 characters:")
        print(long_cols.tolist())
    
    # Save results
    print("Saving results...")
    
    # Save as parquet
    bank_microarea_data.to_parquet(
        'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses.parquet',
        index=False, compression='zstd'
    )
    
    # Save as Stata
    bank_microarea_data.to_stata(
        'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses.dta',
        write_index=False, version=118
    )
    
    # Save 2019 data separately
    bank_microarea_data_19 = bank_microarea_data[bank_microarea_data.year == 2019]
    bank_microarea_data_19.to_stata(
        'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses19.dta',
        write_index=False, version=118
    )
    
    print(f"Analysis complete! Generated microarea features for {len(bank_microarea_data.bank_branch_id.unique())} unique bank branches")
    print(f"Data shape: {bank_microarea_data.shape}")
    print(f"Date range: {bank_microarea_data.date_range_start.min()} to {bank_microarea_data.date_range_start.max()}")


# Combine bank_microarea_data with tract assignment from SOD 2019
bank_microarea_data = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses.parquet')
#
relevant_cols = ['rssdid', 'brnum', 'namefull', 'cb', 'latitude', 'longitude','zipbr','GEOID10']
sod = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10_new.xlsx', 
                       usecols=relevant_cols, dtype=str)
sod.rename(columns={'GEOID10':'tract', 'zipbr':'zip'}, inplace=True)
sod['rssdid'] = pd.to_numeric(sod['rssdid'])
# sod['brnum'] = pd.to_numeric(sod['brnum'])
bank_microarea_data = bank_microarea_data.merge(sod, on=['rssdid','brnum'], how='left', indicator=True)
assert bank_microarea_data[bank_microarea_data._merge!='both'].shape[0] == 0
bank_microarea_data = bank_microarea_data.drop('_merge', axis=1)
bank_microarea_data.to_stata(
        'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses.dta',
        write_index=False, version=118
    )


bank_microarea_data = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_branch_nearby_businesses.parquet')