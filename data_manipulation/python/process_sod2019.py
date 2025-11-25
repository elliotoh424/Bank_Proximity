#########################################################################################################################################
# Calculate average nvisits in Jan 2020 for stores within 200 or 500 yards (Drug store or any naics based restricted stata file)
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

naics = pd.read_excel('C:/Users/elliotoh/Box/analysis/2017_NAICS_Descriptions.xlsx',dtype=str)
naics = naics[naics.Code.str.contains('^4[45]|^7[12]|^81',na=False)]
retail_naics = naics.Code.tolist()

# retail_cols = ['placekey', 'location_name', 'date_range_start','street_address', 'city', 'region', 'zip', 'latitude', 'longitude', 'brands', 'naics_code','nvisits', 'tract', 'nvisits', 'nvisits_std1', 'nvisits_jan2020', 'nvisits_2019']
filename = 'adv_drug_stores_final'
distlist = [x*100 for x in range(1,26)]

# Bank branches as of June 2019
bank = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/community_bank_2019_2023.csv') # Community banking reference data FDIC report
bank.columns = bank.columns.str.lower()
bank = bank[bank.callym==201906] # as of june 2019
assert bank[bank[['cert']].duplicated()].shape[0] == 0
bank_cols = ['namehcr', 'rssdhcr', 'cert', 'namefull', 'cb', 'city','stalp']
bank = bank[bank_cols]
bank = bank[bank.cb==1] # Community bank only
sod = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2019.csv') # Bank branch data as of June 2019
sod.columns = sod.columns.str.lower()
assert sod[sod[['uninumbr','rssdid']].duplicated()].shape[0] == 0 # Unique for within institution branch number and institution
assert sod[sod[['brnum','rssdid']].duplicated()].shape[0] == 0 # Unique for within institution branch number and institution
sod_cols = ['namebr', 'addresbr', 'citybr', 'stalpbr', 'zipbr', 'stcntybr', 'tract', 'bkmo', 'uninumbr', 'brnum', 'brsertyp', 'sims_established_date', 'sims_latitude', 'sims_longitude', # branch vars
            'namefull', 'address', 'city', 'stalp', 'zip', 'stcnty', 'cert',  'asset', 'cntryna', 'rssdid', 'usa', 'below_cra_minimum', 'insured', # institution vars
            'namehcr', 'rssdhcr', 'stalphcr' # holding company vars 
            ]
sod = sod[sod_cols]
sod.rename(columns = {'zip':'zipbank','tract':'tractbr'}, inplace=True)
sod['cb'] = sod.cert.isin(bank.cert).astype(int)
sod.columns = sod.columns.str.replace('^sims_','',regex=True)
sod = sod[~sod.stalpbr.str.contains('^PR$|^VI$|^AS$|^FM$|^GU$|^MH$|^MP$|^PW$',na=False)]
sod['zipbr'] = sod.zipbr.astype(str)
sod.loc[sod.zipbr.str.len()==4,'zipbr'] = '0' + sod.loc[sod.zipbr.str.len()==4,'zipbr']
assert sod[sod.zipbr.str.len()!=5].shape[0] == 0
sod['stcntybr'] = sod.stcntybr.astype(str)
sod.loc[sod.stcntybr.str.len()==4,'stcntybr'] = '0' + sod.loc[sod.stcntybr.str.len()==4,'stcntybr']
assert sod[sod.stcntybr.str.len()!=5].shape[0] == 0
assert sod[sod.namebr=='Broadway National Bank'].shape[0] == 1
assert sod[sod.namebr=='M&i Bank Of Mayville Branch'].shape[0] == 1
# Summary stats at bank level
sod_stat = sod.groupby(['cert','asset','namefull','cb']).agg({'namebr':'count','stalpbr':'nunique','zipbr':'nunique'}).reset_index()
sod_stat[sod_stat.cb==1][['namebr','zipbr','stalpbr']].describe()
sod_stat[sod_stat.asset<200*10**6][['namebr','zipbr','stalpbr']].describe()
# Manual fixes for incorrect coordinates
sod.loc[sod.namebr=='Broadway National Bank',['latitude','longitude']] = [29.517157138123398,-98.4532981447092]
sod.loc[sod.namebr=='M&i Bank Of Mayville Branch',['latitude','longitude']] = [43.493728517357646, -88.54660692830039]
sod.loc[sod.namebr=='Dowagic Office Branch',['latitude','longitude']] = [41.98309140015563, -86.10934802901485]
sod.loc[sod.namebr=='First National Bank Of South Carolina',['latitude','longitude']] = [33.32006497274788, -80.411810250694]
sod.loc[sod.namebr=='New York Abbot Downing Trust Office Branch',['latitude','longitude']] = [40.75340964645199, -73.97908535789003]
sod.loc[sod.namebr=='Santana Row',['latitude','longitude']] = [37.31966061152748, -121.94975520217476]
sod.loc[(sod.namebr=='Chatham Branch') & (sod.zipbr=='60620'),['latitude','longitude']] = [41.743443264021614, -87.63248561738381]
sod.loc[(sod.namebr=='Lumberton Branch') & (sod.addresbr=='1636 - 61 Route 38 & Eayrestown Road'),['latitude','longitude']] = [39.98182545963397, -74.78378337586221]
sod.loc[sod.namebr=='Fairhaven',['latitude','longitude']] = [48.72012667058587, -122.50492662878439]
sod.loc[(sod.namebr=='Midtown Branch') & (sod.addresbr=='330 Madison Avenue'),['latitude','longitude']] = [40.75340964645199, -73.97913900207064]
sod.loc[sod.namebr=='1688 Victory Blvd Branch',['latitude','longitude']] = [40.61320969487872, -74.11996347323893]
sod.loc[sod.namebr=='Main Office Administration Branch',['latitude','longitude']] = [42.24142536934825, -70.88858697318716]
sod.loc[(sod.namebr=='Rainelle Branch') & (sod.addresbr=='114 James River & Kanawha Turnpike'),['latitude','longitude']] = [37.96870611689543, -80.76686397331962]
sod.loc[(sod.namebr=='Buena Park Branch') & (sod.addresbr=='5141 Beach Blvd., Units E & F, Building 2'),['latitude','longitude']] = [33.88648068486121, -117.99567778287997]
sod.loc[sod.namebr=='New Castle Drive-In Branch',['latitude','longitude']] = [39.66408881194962, -75.56700111374336]
sod.loc[(sod.namebr=='Boston Branch') & (sod.addresbr=='50 Rowes Wharf, Floor 4'),['latitude','longitude']] = [42.356791848751186, -71.05019512900284]
sod.loc[(sod.namebr=='New Castle Branch') & (sod.addresbr=="One Penn's Way"),['latitude','longitude']] = [39.68695582917523, -75.60693114865894]
sod.loc[sod.namebr=='Tappahannock Office',['latitude','longitude']] = [37.910132838292256, -76.86358980215743]
sod.loc[(sod.namebr=='Bluffton Branch') & (sod.addresbr=="7 Arley Way"),['latitude','longitude']] = [32.276162266334275, -80.87255451580494]
sod.loc[sod.namebr=='Broadway And Houghton Bkg. Ctr. Branch',['latitude','longitude']] = [32.22150002873744, -110.77425245578253]
sod.loc[(sod.namebr=='Reynolda Road Branch') & (sod.addresbr=="2801 Reynolda Rd"),['latitude','longitude']] = [36.14358406825187, -80.29767625802803]
sod.loc[sod.namebr=='North Monroe Street',['latitude','longitude']] = [30.469622423798754, -84.28679224468628]
# Check that coordinates are correct
assert sod[(sod.latitude.isnull()) | (sod.longitude.isnull())].shape[0] == 0
assert sod[(~sod.latitude.between(24,49)) & (~sod.stalpbr.str.contains('^AK$|^HI$'))].shape[0] == 0
assert sod[(~sod.longitude.between(-125,-66)) & (~sod.stalpbr.str.contains('^AK$|^HI$'))].shape[0] == 0
assert sod[(~sod.longitude.between(-167,-131)) & (sod.stalpbr.str.contains('^AK$|^HI$'))].shape[0] == 0
relevant_cols = ['namebr', 'addresbr', 'citybr', 'stalpbr', 'zipbr', 'stcntybr', 'brnum','uninumbr', 'latitude', 'longitude','cb','namefull','rssdid','below_cra_minimum']
sod[relevant_cols].to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_gis.csv',index=False)
relevant_cols = ['brnum', 'rssdid','GEOID10']
sod_tract = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10_new.xlsx', usecols = relevant_cols, dtype=str)
# sod_tract = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10.xlsx', usecols = relevant_cols, dtype=str) # Same branch coverage in new file as old file.
assert sod.shape[0] == sod_tract.shape[0] 
assert sod_tract[sod_tract.GEOID10.isnull()].shape[0] == 0
sod_tract.rename(columns = {'GEOID10':'tractbr'}, inplace=True)
sod_tract['brnum'] = pd.to_numeric(sod_tract['brnum'])
sod_tract['rssdid'] = pd.to_numeric(sod_tract['rssdid'])
assert (sod_tract.tractbr.str.len()!=11).sum() == 0
sod = sod.merge(sod_tract, on = relevant_cols[:-1], how='outer', indicator=True)
assert sod[sod._merge!='both'].shape[0] == 0
sod = sod.drop(['tractbr_x','_merge'],axis=1)
sod.rename(columns = {'tractbr_y':'tractbr'}, inplace=True)
sod['stcntybr'] = sod.tractbr.str[:5]
sod['established_date']  = pd.to_datetime(sod.established_date)
sod['cb_established_date'] = sod.established_date
sod.loc[sod.cb==0, 'cb_established_date'] = np.nan
sod['noncb_established_date'] = sod.established_date
sod.loc[sod.cb==1, 'noncb_established_date'] = np.nan
#
drug_merged.to_parquet(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_{esttype}.parquet', index=False, compression='zstd')
drug_merged.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_{esttype}.dta', write_index=False, version=118)

