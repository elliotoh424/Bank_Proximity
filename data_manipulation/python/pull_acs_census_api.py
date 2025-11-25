from census import Census
from us import states
import pandas as pd
import geopandas as gpd

# Replace with your actual Census API key
API_KEY = 'c20c357b82dd40558061cf0235339ff0337c591b'

# Initialize the Census API client
c = Census(API_KEY)

# Variables
# 'B01001_001E': 'Total population With Age',
# 'B02001_001E': 'Total population With Race',
# 'B02001_003E': 'Black or African American alone',
# 'B02001_004E': 'American Indian and Alaska Native alone',
# 'B02001_005E': 'Asian alone',
# 'B02001_006E': 'Native Hawaiian and Other Pacific Islander alone',
# 'B08134_001E': 'Total workers 16 years and over',
# 'B08134_011E': 'Car, truck, or van',
# 'B08134_061E': 'Public transportation (excluding taxicab)',
# 'B08134_101E': 'Walked',
# 'B08134_111E': 'Other',
# 'B08303_001E': 'Total workers 16 years and over who did not work at home',
# 'B08303_002E': 'Less than 5 minutes',
# 'B08303_003E': '5 to 9 minutes',
# 'B08303_004E': '10 to 14 minutes',
# 'B08303_005E': '15 to 19 minutes',
# 'B08303_006E': '20 to 24 minutes',
# 'B08303_007E': '25 to 29 minutes',
# 'B08303_008E': '30 to 34 minutes',
# 'B08303_009E': '35 to 39 minutes',
# 'B08303_010E': '40 to 44 minutes',
# 'B08303_011E': '45 to 59 minutes',
# 'B08303_012E': '60 to 89 minutes',
# 'B08303_013E': '90 or more minutes',
# 'B15003_001E': 'Total population 25 years and over',
# 'B15003_022E': "Bachelor's degree",
# 'B15003_023E': "Master's degree",
# 'B15003_024E': 'Professional school degree',
# 'B15003_025E': 'Doctorate degree',
# 'B19013_001E': 'Median household income in the past 12 months (in 2012 inflation-adjusted dollars)',
# 'B25077_001E': 'Median home value (dollars)',
# 'B28001_001E': 'Total households Computer',
# 'B28001_002E': 'Owns Computer',
# 'B28001_005E': 'Owns Smartphone',
# 'B28001_009E': 'Owns No Computer',
# 'B28002_001E': 'Total households Internet',
# 'B28002_013E': 'No Internet access'

variables = {
    'B01001_001E': 'tot_pop',
    'B02001_001E': 'race_tot_pop',
    'B02001_003E': 'race_black',
    'B02001_004E': 'race_native',
    'B02001_005E': 'race_asian',
    'B02001_006E': 'race_hawaii',
    #
    'B08134_001E': 'travelwork_tot_pop',
    'B08134_011E': 'travelwork_car',
    'B08134_061E': 'travelwork_public',
    'B08134_101E': 'travelwork_walk',
    'B08134_111E': 'travelwork_other',
    #
    'B08303_001E': 'travelwork_tot_pop',
    'B08303_002E': 'travelwork_time_0to5',
    'B08303_003E': 'travelwork_time_5to9',
    'B08303_004E': 'travelwork_time_10to14',
    'B08303_005E': 'travelwork_time_15to19',
    'B08303_006E': 'travelwork_time_20to24',
    'B08303_007E': 'travelwork_time_25to29',
    'B08303_008E': 'travelwork_time_30to34',
    'B08303_009E': 'travelwork_time_35to39',
    'B08303_010E': 'travelwork_time_40to44',
    'B08303_011E': 'travelwork_time_45to59',
    'B08303_012E': 'travelwork_time_60to89',
    'B08303_013E': 'travelwork_time_90_above',
    #
    'B15003_001E': 'edu_tot_pop',
    'B15003_022E': "edu_bachelor",
    'B15003_023E': "edu_master",
    'B15003_024E': 'edu_professional',
    'B15003_025E': 'edu_doctorate',
    #
    'B19013_001E': 'income_median',
    #
    'B25077_001E': 'median_homevalue',
}

for i in range(len(variables)):
    # Retrieve data for all counties in all states
    data = c.acs5.get(
        fields= [ list(variables.keys())[i] ] + ['NAME'],
            geo={'for': 'tract:*', 'in': 'state:* county:*'},
            year=2010
    )
    data = pd.DataFrame(data)
    if i == 0:
        df = data
    else:  
        df = df.merge(data, on =['NAME','state','county'], how='outer', indicator=True)
        assert df[df._merge!='both'].shape[0] == 0
        df = df.drop('_merge',axis=1)
        print(i)

assert df[df.B08134_001E != df.B08303_001E].shape[0] == 0
df = df.drop('B08303_001E',axis=1)
df = df.rename(columns=variables)
df['pct_minority'] = (df.race_black + df.race_asian + df.race_native + df.race_hawaii)/df.race_tot_pop
df['pct_car'] = (df.travelwork_car)/df.travelwork_tot_pop
df['pct_public'] = (df.travelwork_public)/df.travelwork_tot_pop
df['pct_walk'] = (df.travelwork_walk)/df.travelwork_tot_pop
df['pct_othertrans'] = (df.travelwork_walk)/df.travelwork_tot_pop
#
df['pct_0to10'] = (df.travelwork_time_0to5 + df.travelwork_time_5to9)/df.travelwork_tot_pop
df['pct_10to30'] = (df.travelwork_time_10to14 + df.travelwork_time_15to19 + df.travelwork_time_20to24 + df.travelwork_time_25to29)/df.travelwork_tot_pop
df['pct_30to60'] = (df.travelwork_time_30to34 + df.travelwork_time_35to39 + df.travelwork_time_40to44 + df.travelwork_time_45to59)/df.travelwork_tot_pop
df['pct_60above'] = (df.travelwork_time_60to89 + df.travelwork_time_90_above)/df.travelwork_tot_pop
#
df['pct_collegeup'] = (df.edu_bachelor + df.edu_master + df.edu_professional + df.edu_doctorate)/df.edu_tot_pop
#
#
assert df[df.fips.str.len()!=5].shape[0] == 0

# Combine with county size
# County shapefile
gdf = gpd.read_file('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_tract_2010.shp')
gdf.columns = gdf.columns.str.lower()
gdf.columns = gdf.columns.str.replace('10','')
gdf['cty_aland'] = gdf.aland*(3.8610215854*10**(-7)) # square meter to square miles
gdf.rename(columns={'geoid':'ctyid'}, inplace=True)
gdf = gdf[['ctyid','cty_aland']].drop_duplicates()
assert gdf[gdf.ctyid.duplicated()].shape[0] == 0
#
df['ctyid'] = df.fips
df = df[~df.ctyid.str.contains('^72',na=False)]
df = df.merge(gdf, on='ctyid', how='outer', indicator=True)
df._merge.value_counts()
df = df[df._merge=='both'].drop('_merge',axis=1)
df['pop_density'] = df.tot_pop/df.cty_aland
df.pop_density.describe()
#
cols = [
    'fips',
    'tot_pop', 'pct_minority', 'pop_density',
    'pct_car', 'pct_public', 'pct_walk', 'pct_othertrans',
    'pct_0to10', 'pct_10to30', 'pct_30to60', 'pct_60above',
    'pct_collegeup',
    'income_median', 'median_homevalue',
    'pct_computer', 'pct_smartphone', 'pct_no_computer', 'pct_no_internet'
]
df = df[cols]
cols = [cols[0]] + [col + '_cty' for col in cols[1:]]
df.columns = cols
df.to_stata('C:/Users/elliotoh/Box/lodes_shared/JMP/data/cbp/acs_cty_2019.dta', write_index=False, version=118)

