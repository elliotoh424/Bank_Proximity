import pandas as pd, os, re, time, gc, numpy as np
import pyarrow as pa
import shutil
import geopandas as gpd

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

# HK CBD MSA 
hk_cbd = pd.read_excel('D:/pharmacy/holian_kahn_cbd_geocodes.xlsx', sheet_name='Holian_and_Kahn')
# CBD coordinates determined by google earth search for principal city
hk_cbd['CBSA_code'] = hk_cbd['CBSA_code'].astype(str)
assert hk_cbd[hk_cbd.CBSA_code.str.len()!=5].shape[0] ==0

# Use CBSA-FIPS mapping Feb 2013 file
cbsa_fips = pd.read_csv('D:/pharmacy/cbsa/cbsa_fips_feb2013.csv', skiprows=2, dtype=str)
cbsa_fips = cbsa_fips[~cbsa_fips['CBSA Code'].str.contains('[A-z]', na=False)]
cbsa_fips = cbsa_fips[~cbsa_fips['CBSA Code'].isnull()]
assert cbsa_fips[cbsa_fips['FIPS State Code'].str.len()!=2].shape[0] == 0
assert cbsa_fips[cbsa_fips['FIPS County Code'].str.len()!=3].shape[0] == 0
cbsa_fips['fips'] = cbsa_fips['FIPS State Code'] + cbsa_fips['FIPS County Code']

hk_cbd = hk_cbd.merge(cbsa_fips, left_on='CBSA_code', right_on=['CBSA Code'], how='outer', indicator=True)
hk_cbd._merge.value_counts() # 12 left only CBSAs in HK only are based on pre-2013 MSA definition (somewhere between 2003 and 2009). 760 right only CBSA include the 12 left only CBSA according to 2013 definitions and newly created CBSAs. 
hk_cbd = hk_cbd[hk_cbd._merge=='both'].drop('_merge',axis=1) 
hk_cbd['CBSA Name'] = hk_cbd['CBSA Title'] + ' ' + hk_cbd['Metropolitan/Micropolitan Statistical Area']
hk_cbd['CentralCountyDummy'] = (hk_cbd['Central/Outlying County']=='Central').astype(int)
keep_cols = ['CBSA Name', 'CBSA_code', 'PrincipleCity', 'PrincipleCityStateFIPS', 'unique place code', 'CBDlat', 'CBDlon', 'Central County', 'Central County Fips', 'fips']
hk_cbd = hk_cbd[keep_cols]

# Match with 2010 tract file to calculate distance to CBD for each tract. 
tract10 = gpd.read_file('C:/Users/elliotoh/Box/lodes_shared/data/US_tract_2019.shp')
tract10 = tract10.to_crs(epsg=4326)
tract10['centroid_lat'] = tract10.to_crs('+proj=cea').centroid.to_crs(tract10.crs).y
tract10['centroid_long'] = tract10.to_crs('+proj=cea').centroid.to_crs(tract10.crs).x
tract10.columns = tract10.columns.str.lower()
tract10.sort_values('geoid', inplace=True)
tract10.reset_index(drop=True, inplace=True)
tract10['fips'] = tract10.geoid.str[:5]
tract10['state'] = tract10.geoid.str[:2]
tract10 = tract10[tract10.state != '72']
tract_cols = ['geoid','centroid_lat','centroid_long','fips','state']
tract10 = tract10[tract_cols]
# 
hk_cbd = hk_cbd.merge(tract10, on=['fips'], how='outer', indicator=True)
hk_cbd._merge.value_counts() # one left only drop this.
hk_cbd = hk_cbd[hk_cbd._merge!='left_only']
# 2021 counties without CBSA and 1121 counties with CBSA
hk_cbd[hk_cbd._merge=='right_only'].fips.drop_duplicates().shape
hk_cbd[hk_cbd._merge=='both'].fips.drop_duplicates().shape

# Calculate distance from CBD
hk_cbd['dist_from_cbd'] = distance(hk_cbd.CBDlat, hk_cbd.CBDlon, hk_cbd.centroid_lat, hk_cbd.centroid_long)
hk_cbd['CBSADummy'] = (hk_cbd._merge=='both').astype(int)
hk_cbd.dist_from_cbd.describe()
hk_cbd[hk_cbd['CBSA Name'].str.contains('Metropolitan',na=False)].dist_from_cbd.describe()
hk_cbd[hk_cbd['CBSA Name'].str.contains('Micropolitan',na=False)].dist_from_cbd.describe()

final_cols = ['geoid', 'fips', 'dist_from_cbd', 'CBSA Name', 'CBSA_code', 'PrincipleCity', 'PrincipleCityStateFIPS', 'Central County', 'Central County Fips', 'CBSADummy']
hk_cbd = hk_cbd[final_cols]
hk_cbd.rename(columns = {'geoid':'tract'}, inplace=True)
hk_cbd.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/dist_from_cbd.dta', write_index=False, version=118)
