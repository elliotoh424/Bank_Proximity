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

def gen_bankbranch_nearby_drug(esttype, filename, distlist):
    # Sample drug stores.
    # drug_cols = ['placekey', 'location_name', 'street_address', 'city', 'region', 'zip', 'latitude', 'longitude', 'brands', 'tract']
    drug_cols = ['placekey', 'zip', 'latitude', 'longitude', 'brands', 'tract', 'd_brand']
    drug_jan2020 = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/{filename}.dta')
    drug_jan2020 = drug_jan2020[drug_jan2020.date_range_start == '2020-01-01']
    drug_jan2020 = drug_jan2020[(~drug_jan2020.zip.isnull()) ]
    drug_jan2020['latitude'] = pd.to_numeric(drug_jan2020['latitude'])
    drug_jan2020['longitude'] = pd.to_numeric(drug_jan2020['longitude'])
    drug_jan2020.loc[drug_jan2020.zip.str.len()==4,'zip'] = '0' + drug_jan2020.loc[drug_jan2020.zip.str.len()==4,'zip']
    assert drug_jan2020[drug_jan2020.zip.str.len()!=5].shape[0] == 0
    drug_jan2020 = drug_jan2020[drug_cols]
    drug_jan2020['county'] = drug_jan2020.tract.str[:5]
    assert drug_jan2020[drug_jan2020.county.str.len()!=5].shape[0] == 0
    # Bank branches as of June 2019
    bank = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/community_bank_2019_2023.csv') # Community banking reference data FDIC report
    bank.columns = bank.columns.str.lower()
    bank = bank[bank.callym==201906] # as of june 2019
    assert bank[bank[['cert']].duplicated()].shape[0] == 0
    bank_cols = ['namehcr', 'rssdhcr', 'cert', 'namefull', 'cb', 'city','stalp']
    bank = bank[bank_cols]
    bank = bank[bank.cb==1] # Community bank only
    # CRA reporting threshold for 2019: Asset size is $1.284 billion in 2017 and 2018.
    sod17 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2017.csv')[['RSSDID','ASSET']].drop_duplicates()
    sod18 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2018.csv')[['RSSDID','ASSET']].drop_duplicates()
    sod17.columns = sod17.columns.str.lower()
    sod18.columns = sod18.columns.str.lower()
    assert sod17[sod17.rssdid.duplicated()].shape[0] == 0
    assert sod18[sod18.rssdid.duplicated()].shape[0] == 0
    sod17.columns = ['rssdid','asset17']
    sod18.columns = ['rssdid','asset18']
    sod17['asset17'] = sod17['asset17'] * 1000 # in units of USD
    sod18['asset18'] = sod18['asset18'] * 1000 # in units of USD
    # 
    sod = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2019.csv') # Bank branch data as of June 2019
    sod.columns = sod.columns.str.lower()
    assert sod[sod[['uninumbr','rssdid']].duplicated()].shape[0] == 0 # Unique for within institution branch number and institution
    assert sod[sod[['brnum','rssdid']].duplicated()].shape[0] == 0 # Unique for within institution branch number and institution
    sod = sod.merge(sod17, on ='rssdid', how='left', indicator=True)
    sod._merge.value_counts() # Checked that unmatched banks did not report in CRA 2019. One bank above 2019 threshold didn't exist (checked call report)
    left1 = sod[sod._merge=='left_only'].rssdid.drop_duplicates()
    sod = sod.drop('_merge',axis=1)
    #
    sod = sod.merge(sod18, on ='rssdid', how='left', indicator=True)
    sod._merge.value_counts() # Checked that unmatched banks did not report in CRA 2019.
    left2 = sod[sod._merge=='left_only'].rssdid.drop_duplicates()
    sod = sod.drop('_merge',axis=1)
    # 
    sod['below_cra_minimum'] = ( ((sod.asset17 < 1.284*10**9) | (sod.asset17.isnull())) ) & ( ((sod.asset18 < 1.284*10**9) | (sod.asset18.isnull())) )
    sod['below_cra_minimum'] = sod['below_cra_minimum'].astype(int)
    # Check that banks not in SOD 2017 don't report in 2019 CRA.
    assert sod[(sod.rssdid.isin(left1)) & (~sod.asset17.isnull())].shape[0] == 0
    # Check that banks not in SOD 2018 don't report in 2019 CRA.
    assert sod[(sod.rssdid.isin(left2)) & (~sod.asset18.isnull())].shape[0] == 0
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
    drug_merged = drug_jan2020.merge(sod, left_on=['county'], right_on = ['stcntybr'])
    # calculate distance
    drug_merged['dist_from_branch'] = distance(drug_merged.latitude_x, drug_merged.longitude_x, drug_merged.latitude_y, drug_merged.longitude_y)
    drug_merged['dist_from_branch_yd'] = drug_merged['dist_from_branch']*1760 # Convert from miles to yard
    drug_merged['dist_from_cb_branch'] = drug_merged['dist_from_branch']
    drug_merged.loc[drug_merged.cb!=1,'dist_from_cb_branch'] = np.nan
    drug_merged['dist_from_cb_branch_yd'] = drug_merged['dist_from_cb_branch']*1760 # Convert from miles to yard
    drug_merged['dist_from_noncb_branch'] = drug_merged['dist_from_branch']
    drug_merged.loc[drug_merged.cb==1,'dist_from_noncb_branch'] = np.nan
    drug_merged['dist_from_noncb_branch_yd'] = drug_merged['dist_from_noncb_branch']*1760 # Convert from miles to yard
    #
    drug_merged['dist_from_noncra_branch'] = drug_merged['dist_from_branch']
    drug_merged.loc[drug_merged.below_cra_minimum==0,'dist_from_noncra_branch'] = np.nan
    drug_merged['dist_from_noncra_branch_yd'] = drug_merged['dist_from_noncra_branch']*1760 # Convert from miles to yard
    pd.options.display.float_format = '{:.3f}'.format
    # Dummies for bank branch and community bank branch within X yards and Y miles
    # No of branches within X yards, branch dummy within X yards
    # No of CB branches within X yards, CB branch dummy within X yards
    ndist = len(distlist)
    for i in range(ndist): 
        yard = distlist[i]
        if i == 0:
            drug_merged[f'bank_branch_{yard}yd_exc'] = (drug_merged.dist_from_branch_yd<=yard).astype(int)
            drug_merged[f'cb_branch_{yard}yd_exc'] = ((drug_merged.dist_from_cb_branch_yd<=yard) & (~drug_merged.dist_from_cb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncb_branch_{yard}yd_exc'] = ((drug_merged.dist_from_noncb_branch_yd<=yard) & (~drug_merged.dist_from_noncb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncra_branch_{yard}yd_exc'] = ((drug_merged.dist_from_noncra_branch_yd<=yard) & (~drug_merged.dist_from_noncra_branch_yd.isnull())).astype(int)
            drug_merged[f'bank_branch_{yard}yd'] = (drug_merged.dist_from_branch_yd<=yard).astype(int)
            drug_merged[f'cb_branch_{yard}yd'] = ((drug_merged.dist_from_cb_branch_yd<=yard) & (~drug_merged.dist_from_cb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncb_branch_{yard}yd'] = ((drug_merged.dist_from_noncb_branch_yd<=yard) & (~drug_merged.dist_from_noncb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncra_branch_{yard}yd'] = ((drug_merged.dist_from_noncra_branch_yd<=yard) & (~drug_merged.dist_from_noncra_branch_yd.isnull())).astype(int)
        else:
            yard_prev = distlist[i-1]
            drug_merged = drug_merged.copy()
            drug_merged[f'bank_branch_{yard}yd_exc'] = ((drug_merged.dist_from_branch_yd>yard_prev) & (drug_merged.dist_from_branch_yd<=yard)).astype(int)
            drug_merged[f'cb_branch_{yard}yd_exc'] = ((drug_merged.dist_from_cb_branch_yd>yard_prev) & (drug_merged.dist_from_cb_branch_yd<=yard) & (~drug_merged.dist_from_cb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncb_branch_{yard}yd_exc'] = ((drug_merged.dist_from_noncb_branch_yd>yard_prev) & (drug_merged.dist_from_noncb_branch_yd<=yard) & (~drug_merged.dist_from_noncb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncra_branch_{yard}yd_exc'] = ((drug_merged.dist_from_noncra_branch_yd>yard_prev) & (drug_merged.dist_from_noncra_branch_yd<=yard) & (~drug_merged.dist_from_noncra_branch_yd.isnull())).astype(int)
            drug_merged[f'bank_branch_{yard}yd'] = ((drug_merged.dist_from_branch_yd<=yard)).astype(int)
            drug_merged[f'cb_branch_{yard}yd'] = ((drug_merged.dist_from_cb_branch_yd<=yard) & (~drug_merged.dist_from_cb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncb_branch_{yard}yd'] = ((drug_merged.dist_from_noncb_branch_yd<=yard) & (~drug_merged.dist_from_noncb_branch_yd.isnull())).astype(int)
            drug_merged[f'noncra_branch_{yard}yd'] = ((drug_merged.dist_from_noncra_branch_yd<=yard) & (~drug_merged.dist_from_noncra_branch_yd.isnull())).astype(int)
    assert drug_merged[(drug_merged.tract.str.len() != 11) | (drug_merged.tractbr.str.len() != 11)].shape[0] == 0
    # All exclusive variables should add to be 0 or 1
    check_cols = [x for x in drug_merged.columns.tolist() if re.search('^bank.*_exc',x)]
    test = drug_merged[check_cols].sum(axis=1)
    assert test[(test!=1) & (test!=0)].shape[0] == 0
    check_cols = [x for x in drug_merged.columns.tolist() if re.search('^cb.*_exc',x)]
    test = drug_merged[check_cols].sum(axis=1)
    assert test[(test!=1) & (test!=0)].shape[0] == 0
    check_cols = [x for x in drug_merged.columns.tolist() if re.search('^noncb.*_exc',x)]
    test = drug_merged[check_cols].sum(axis=1)
    assert test[(test!=1) & (test!=0)].shape[0] == 0
    check_cols = [x for x in drug_merged.columns.tolist() if re.search('^noncra.*_exc',x)]
    test = drug_merged[check_cols].sum(axis=1)
    assert test[(test!=1) & (test!=0)].shape[0] == 0
    #
    drug_merged['bank_branch_tract'] = (drug_merged.tract == drug_merged.tractbr).astype(int)
    drug_merged['cb_branch_tract'] = ((drug_merged.tract == drug_merged.tractbr) & (drug_merged.cb==1)).astype(int)
    drug_merged['noncb_branch_tract'] = ((drug_merged.tract == drug_merged.tractbr) & (drug_merged.cb!=1)).astype(int)
    drug_merged['noncra_branch_tract'] = ((drug_merged.tract == drug_merged.tractbr) & (drug_merged.below_cra_minimum==1)).astype(int)
    drug_merged['yescra_branch_tract'] = ((drug_merged.tract == drug_merged.tractbr) & (drug_merged.below_cra_minimum==0)).astype(int)
    drug_merged.to_parquet(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_{esttype}.parquet', index=False, compression='zstd')
    drug_merged.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_{esttype}.dta', write_index=False, version=118)

    # Initialize the aggregation dictionary
    agg_dict = {
        'dist_from_branch_yd': 'min',
        'dist_from_cb_branch_yd': 'min',
        'dist_from_noncb_branch_yd': 'min',
        'dist_from_noncra_branch_yd': 'min',
        'bank_branch_tract': {'max', 'sum'},
        'cb_branch_tract': {'max', 'sum'},
        'noncb_branch_tract': {'max', 'sum'},
        'noncra_branch_tract': {'max', 'sum'},
        'yescra_branch_tract': {'max', 'sum'},
        'established_date': {'max', 'min'},
        'cb_established_date': {'max', 'min'},
        'noncb_established_date': {'max', 'min'}
    }
    # Use a loop to add the yard distance keys to agg_dict
    for yd in distlist:
        agg_dict[f'bank_branch_{yd}yd'] = {'max', 'sum'}
        agg_dict[f'cb_branch_{yd}yd'] = {'max', 'sum'}
        agg_dict[f'noncb_branch_{yd}yd'] = {'max', 'sum'}
        agg_dict[f'noncra_branch_{yd}yd'] = {'max', 'sum'}
        #
        agg_dict[f'bank_branch_{yd}yd_exc'] = {'max', 'sum'}
        agg_dict[f'cb_branch_{yd}yd_exc'] = {'max', 'sum'}
        agg_dict[f'noncb_branch_{yd}yd_exc'] = {'max', 'sum'}
        agg_dict[f'noncra_branch_{yd}yd_exc'] = {'max', 'sum'}
    # Perform the groupby and aggregation
    drug_dist = drug_merged.groupby(['placekey', 'd_brand']).agg(agg_dict).reset_index()
    # Rename the columns as required
    drug_dist.columns = drug_dist.columns.get_level_values(0) + ' ' + drug_dist.columns.get_level_values(1)
    drug_dist.columns = ['n' + x if re.search(' sum$', x) else x for x in drug_dist.columns.tolist() ]
    drug_dist.columns = drug_dist.columns.str.replace(' sum$','', regex=True)
    drug_dist.columns = ['min' + x if re.search(' min$', x) else x for x in drug_dist.columns.tolist() ]
    drug_dist.columns = drug_dist.columns.str.replace(' min$','', regex=True)
    drug_dist.columns = ['max' + x if re.search(' max$', x) and re.search('date',x) else x for x in drug_dist.columns.tolist() ]
    drug_dist.columns = drug_dist.columns.str.replace(' max$','', regex=True)
    drug_dist.columns = drug_dist.columns.str.strip()
    #
    drug_dist.columns = drug_dist.columns.str.replace('mindist_from_branch','mindist_from_bank_branch')
    check_cols = [x for x in drug_dist.columns.tolist() if re.search('^bank.*|^cb.*|^noncb.*|^noncra.*|^yescra.*',x)]
    for c in check_cols:
        assert drug_dist[~drug_dist[c].between(0,1)].shape[0] == 0
    assert drug_dist.columns[drug_dist.columns.str.len()>32].shape[0] == 0
    drug_dist.to_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_{esttype}_bankbranch_dist.dta',write_index=False, version=118)
    return(drug_dist)

distlist = [x*100 for x in range(1,26)]
# distlist = [200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]
drug = gen_bankbranch_nearby_drug('drug','adv_drug_stores_final', distlist) # Create manual thresholds for mutually exclusive bank distance thresholds for 0-200, 201-500, 501-1000 in stata code.



# drug[['cb_branch_200yd','cb_branch_500yd','cb_branch_1000yd','cb_branch_1500yd','cb_branch_2000yd']].describe()
# drug[['bank_branch_200yd','bank_branch_500yd','bank_branch_1000yd','bank_branch_1500yd','bank_branch_2000yd']].describe()

# # rest200 = gen_store_nearby('restaurant', 'adv_restaurant_final', 200)

# drug.describe()
# drug[drug.d_brand==0].describe()
# drug[drug.d_brand==1].describe()