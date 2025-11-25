import requests, pandas as pd, math, time, numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm

sba_dispensed = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_ctys.csv')
sba_dispensed['BorrName'] = sba_dispensed['BorrName'].str.replace('\t','')
sba_dispensed['BorrName_std'] = sba_dispensed['BorrName'].str.upper()
sba_dispensed['BorrName_std'] = sba_dispensed['BorrName_std'].str.replace('[^\w\s]','',regex=True)
sba_dispensed['BorrName_std'] = sba_dispensed['BorrName_std'].str.replace('  ',' ')
sba_dispensed["comp_id"] = sba_dispensed.groupby(["BorrName_std", "BorrZip"]).ngroup()
sba_dispensed['ApprovalDate'] = pd.to_datetime(sba_dispensed['ApprovalDate'])
sba_dispensed = sba_dispensed.drop('fips',axis=1)
sba_dispensed.rename(columns = {'GEOID':'fips'}, inplace=True)
sba_dispensed['fips'] = sba_dispensed['fips'].astype(str).str.zfill(5)
sba_dispensed['NAICSCode'] = sba_dispensed['NAICSCode'].astype(str).str.zfill(6)
sba_dispensed.rename(columns= {'cty':'fips'}, inplace=True)

# SOD
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
#
sodzip = sod.groupby('zip').agg({'mega_asset':{'sum','max'},'nonmega_asset':{'sum','max'},'cb_asset':{'sum','max'}}).reset_index()
sodzip.columns = sodzip.columns.get_level_values(0) + ' ' + sodzip.columns.get_level_values(1)
sodzip.columns = sodzip.columns.str.strip()
# Rename columns with distance suffix
sodzip.rename(columns={
    'mega_asset max': f'mega_zip', 
    'mega_asset sum': f'nmega_zip', 
    'nonmega_asset max': f'nonmega_zip', 
    'nonmega_asset sum': f'nnonmega_zip', 
    'cb_asset max': f'cb_zip', 
    'cb_asset sum': f'ncb_zip'
}, inplace=True) # 19217 zips with bank branches
#
sod['fips'] = sod.tract.str[:5]
sodcty = sod.groupby('fips').agg({'mega_asset':{'sum','max'},'nonmega_asset':{'sum','max'},'cb_asset':{'sum','max'}}).reset_index()
sodcty.columns = sodcty.columns.get_level_values(0) + ' ' + sodcty.columns.get_level_values(1)
sodcty.columns = sodcty.columns.str.strip()
# Rename columns with distance suffix
sodcty.rename(columns={
    'mega_asset max': f'mega_cty', 
    'mega_asset sum': f'nmega_cty', 
    'nonmega_asset max': f'nonmega_cty', 
    'nonmega_asset sum': f'nnonmega_cty', 
    'cb_asset max': f'cb_cty', 
    'cb_asset sum': f'ncb_cty'
}, inplace=True) # 19217 zips with bank branches

#####################################
sba1922 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2019-01-01','2022-12-31'))].drop('_merge',axis=1)
sba_pharmacy1922 = sba1922[sba1922.NAICSCode=='446110.0']
sba_pharmacy1922['yq'] = sba_pharmacy1922['ApprovalDate'].dt.to_period('Q').dt.to_timestamp(how='start')
#
sbazip_pharmacy = sba_pharmacy1922.groupby(['BorrZip','yq']).agg({'GrossApproval':{'sum','count'}}).reset_index()
sbacty_pharmacy = sba_pharmacy1922.groupby(['fips','yq']).agg({'GrossApproval':{'sum','count'}}).reset_index()
#
sbazip_pharmacy.columns = [
    "_".join(col).rstrip("_") if isinstance(col, tuple) else col
    for col in sbazip_pharmacy.columns.values
]
sbacty_pharmacy.columns = [
    "_".join(col).rstrip("_") if isinstance(col, tuple) else col
    for col in sbacty_pharmacy.columns.values
]
sbazip_pharmacy.rename(columns = {'GrossApproval_sum':'GrossApproval','GrossApproval_count':'NoLoan'}, inplace=True)
sbazip_pharmacy['BorrZip'] = sbazip_pharmacy['BorrZip'].astype(str).str.zfill(5)
#
sbacty_pharmacy.rename(columns = {'GrossApproval_sum':'GrossApproval','GrossApproval_count':'NoLoan'}, inplace=True)
sbacty_pharmacy['fips'] = sbacty_pharmacy['fips'].astype(str).str.zfill(5)

# All pharmacies in 2019 from Advan
yq_df = pd.DataFrame({
    'yq': pd.period_range('2019Q1', '2022Q4', freq='Q').to_timestamp(how='start')
})
#
cols = ['placekey','location_name','d_brand','street_address','city','region','zip','tract']
drug19 = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta', columns = cols)
drug19.drop_duplicates(inplace=True)
drug19.placekey.duplicated().sum()
drug19['fips'] = drug19.tract.str[:5]
#
ind19 = drug19[drug19.d_brand==0]
#
indzip = ind19.groupby('zip').agg({'placekey':'count'}).reset_index()
indzip.columns = ['zip','nind19']
indzip['zip'] = indzip['zip'].astype(str).str.zfill(5)
indzip = indzip.merge(sodzip, left_on='zip', right_on='zip', how='left', indicator=True) # Zips with pharmacies
indzip._merge.value_counts()
for c in ['mega_zip', 'nmega_zip', 'nonmega_zip', 'nnonmega_zip', 'cb_zip', 'ncb_zip']:
    indzip[c] = indzip[c].fillna(0)

indzip = indzip.drop(['_merge'],axis=1)
indzip = indzip.merge(yq_df, how='cross').sort_values(['zip', 'yq'])
indzip = indzip.merge(sbazip_pharmacy, left_on=['zip','yq'], right_on =['BorrZip','yq'], how='left', indicator=True) # zips with pharmacies (either SBA loan or not)
indzip._merge.value_counts()
indzip.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/indzip_pharmacy_reg_2019_2022.dta', write_index=False, version=118)


#
indcty = ind19.groupby('fips').agg({'placekey':'count'}).reset_index()
indcty.columns = ['fips','nind19']
indcty['fips'] = indcty['fips'].astype(str).str.zfill(5)
indcty = indcty.merge(sodcty, left_on='fips', right_on='fips', how='left', indicator=True)
assert indcty[indcty._merge=='right_only'].shape[0] == 0 # Some borrowers located in zips without bank
indcty._merge.value_counts()
for c in ['mega_cty', 'nmega_cty', 'nonmega_cty', 'nnonmega_cty', 'cb_cty', 'ncb_cty']:
    indcty[c] = indcty[c].fillna(0)

indcty = indcty.drop(['_merge'],axis=1)
indcty = indcty.merge(yq_df, how='cross').sort_values(['fips', 'yq'])
indcty = indcty.merge(sbacty_pharmacy, left_on=['fips','yq'], right_on=['fips','yq'], how='left', indicator=True)
assert indcty[indcty._merge=='right_only'].shape[0] == 0 # Some borrowers located in ctys without bank
indcty._merge.value_counts()
indcty.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/indcty_pharmacy_reg_2019_2022.dta', write_index=False, version=118)

indzip._merge.value_counts()
indcty._merge.value_counts()

# AmtLoan vs. NoLoan raw; adjusted by # of independent pharmacies in 2019
