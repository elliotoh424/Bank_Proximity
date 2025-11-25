import pandas as pd, re, os, numpy as np

# Variables of interest
pd.options.display.float_format = '{:.3f}'.format


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

# Dataset 1 : Store-bank containing store, county, bank (ID_RSSD, asset, CB status), and distance between store and branch (store coordinates and branch coordinates)
# Left join drug store data with SOD.
# Final variables: placekey, d_brand, fips, ID_RSSD, asset, CB status, store coordinates, branch coordinates, distance between store and branch. 
drug_vars = ['placekey','d_brand','tract','latitude','longitude','zip']
drugstore = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", columns = drug_vars).drop_duplicates()
assert drugstore[drugstore.tract.str.len()!=11].shape[0] == 0
drugstore['fips'] = drugstore.tract.str[:5]
drugstore = drugstore.drop('tract',axis=1)
for c in ['latitude','longitude']:
    drugstore[c] = pd.to_numeric(drugstore[c])
#
relevant_cols = ['rssdid', 'brnum', 'namefull', 'cb', 'latitude', 'longitude','zipbr','GEOID10']
sod = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10_new.xlsx', usecols = relevant_cols, dtype=str)
sod.rename(columns = {'GEOID10':'tract', 'zipbr':'zip'}, inplace=True)
assert sod[sod.brnum.isnull()].shape[0] == 0
sod_vars = ['RSSDID','BRNUM','ASSET','SIMS_ESTABLISHED_DATE','ADDRESBR','CITYBR','STALPBR','ZIPBR','UNINUMBR']
sod19 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2019.csv', 
                    usecols = sod_vars, dtype=str)
sod20 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2020.csv', 
                    usecols = sod_vars, dtype=str)
sod21 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2021.csv', 
                    usecols = sod_vars, dtype=str)
sod22 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2022.csv', 
                    usecols = sod_vars, dtype=str)
sod19['closed1920'] = (~sod19.UNINUMBR.isin(sod20.UNINUMBR)).astype(int)
sod19['closed2021'] = (~sod19.UNINUMBR.isin(sod21.UNINUMBR)).astype(int)
sod19['closed2122'] = (~sod19.UNINUMBR.isin(sod22.UNINUMBR)).astype(int)
sod19['closed'] = sod19[['closed1920','closed2021','closed2122']].max(axis=1)

sod19.columns = sod19.columns.str.lower()
sod19 = sod19[~sod19.stalpbr.str.contains('^PR$|^VI$|^AS$|^FM$|^GU$|^MH$|^MP$|^PW$',na=False)]
sod19.rename(columns = {'sims_established_date':'date_established'}, inplace=True)
sod = sod.merge(sod19 , on=['rssdid','brnum'], how='outer', indicator=True)
assert sod[sod._merge!='both'].shape[0] == 0
sod = sod.drop('_merge',axis=1)
for c in ['latitude', 'longitude', 'asset', 'rssdid', 'cb','asset']:
	sod[c] = pd.to_numeric(sod[c]) # lat and long at 3 decimal point accuracy

sod['stcntybr'] = sod.tract.str[:5]
final_cols = ['rssdid', 'zip', 'stcntybr','brnum', 'asset', 'cb', 'date_established', 'latitude', 'longitude', 'namefull','addresbr','citybr','stalpbr','zipbr', 'closed1920', 'closed2021', 'closed2122', 'closed']
sod = sod[final_cols]
# Merge drug store with SOD
drug_sod = drugstore.merge(sod, left_on=['fips'], right_on=['stcntybr'], how='left', indicator=True)
drug_sod._merge.value_counts()
# drug_sod[drug_sod._merge!='both']
for c in ['latitude_x','longitude_x','latitude_y','longitude_y']:
	drug_sod[c] = pd.to_numeric(drug_sod[c])

drug_sod['dist_store_branch'] = distance(drug_sod.latitude_x, drug_sod.longitude_x, drug_sod.latitude_y, drug_sod.longitude_y)
drug_sod['dist_store_branch'] = drug_sod.dist_store_branch * 1760 
assert drug_sod[drug_sod[['rssdid','brnum','placekey']].duplicated()].shape[0] ==0
# final_cols = ['placekey', 'd_brand', 'fips', 'rssdid', 'brnum', 'dist_store_branch', 'asset', 'cb', 'date_established', 'namefull']
# drug_sod = drug_sod[final_cols]
drug_sod.rename(columns={'rssdid':'ID_RSSD'}, inplace=True)
# CB: FDIC, nonmega: nonCB below 100 billion, large banks: above 100, GSIB: 6 largest BHCs (or top 4)
drug_sod['asset'] = drug_sod.asset/1000 # assets in million dollars now
#
drug_sod['big4_asset'] = ((drug_sod.ID_RSSD == 852218) | (drug_sod.ID_RSSD == 480228) | (drug_sod.ID_RSSD == 476810) | (drug_sod.ID_RSSD == 451965)).astype(int) # Jp Morgan Chase, Bank of America, Citibank, Wells Fargo
# drug_sod['gsib_asset'] = ((drug_sod.ID_RSSD == 852218) | (drug_sod.ID_RSSD == 480228) | (drug_sod.ID_RSSD == 476810) | (drug_sod.ID_RSSD == 451965) | (drug_sod.ID_RSSD == 504713) | (drug_sod.ID_RSSD == 817824)).astype(int) # Jp Morgan Chase, Bank of America, Citibank, Wells Fargo, US Bank, PNC Bank
drug_sod['large4_asset'] = ((drug_sod.asset > 100*10**3) & (drug_sod.big4_asset == 0) & (~drug_sod.asset.isnull())).astype(int) # above 100 billion excluding 
drug_sod['mega_asset'] = ((drug_sod.asset > 100*10**3) & (~drug_sod.asset.isnull())).astype(int)  # Above 100 billion
drug_sod['cb_asset'] = ((drug_sod['cb'] == 1) & (~drug_sod.asset.isnull())).astype(int)  # FDIC designation (10 billion + other criteria)
drug_sod['nonmega_asset'] = ((drug_sod.asset <= 100*10**3) & (drug_sod.cb_asset == 0) & (~drug_sod.asset.isnull())).astype(int)  # Below 100 billion and not in cb
# drug_sod['largecb_asset'] = ((drug_sod.asset > 1.284*10**3) & (drug_sod.asset <= 10*10**3) & (~drug_sod.asset.isnull())).astype(int) # 1.284 - 10 billions
drug_sod['test'] = drug_sod.mega_asset + drug_sod.nonmega_asset + drug_sod.cb_asset
assert drug_sod[(drug_sod.test!=1) & (~drug_sod.asset.isnull())].shape[0] == 0
drug_sod = drug_sod.drop('test',axis=1)
# -------------------------
# Distance masks by class (fix: no 'branch_asset' reference)
# -------------------------
for c in ['branch','big4','large4','mega','nonmega','cb']:
    drug_sod[f'dist_store_{c}'] = drug_sod['dist_store_branch']
    if c != 'branch':
        drug_sod.loc[drug_sod[f'{c}_asset'] == 0, f'dist_store_{c}'] = np.nan

# -------------------------
# Distance filters: OPEN branches only (remove closed)
# -------------------------
for c in ['branch','big4','large4','mega','nonmega','cb']:
    drug_sod[f'open_dist_store_{c}'] = drug_sod[f'dist_store_{c}']
    drug_sod.loc[drug_sod.closed == 1, f'open_dist_store_{c}'] = np.nan

# -------------------------
# Binary flags and ring counts: ALL branches and OPEN branches
# -------------------------
for c in ['branch','big4','large4','mega','nonmega','cb']:
    # All branches
    drug_sod[f'{c}_200yd'] = ((drug_sod[f'dist_store_{c}'] <= 200) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'{c}_500yd'] = ((drug_sod[f'dist_store_{c}'] <= 500) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'{c}_1000yd'] = ((drug_sod[f'dist_store_{c}'] <= 1000) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
	#
    # Rings (disjoint except we also keep 200yd core as a 'ring' per your preference)
    drug_sod[f'{c}_200yd_ring']  = ((drug_sod[f'dist_store_{c}'] <= 200) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'{c}_500yd_ring']  = ((drug_sod[f'dist_store_{c}'] > 200) & (drug_sod[f'dist_store_{c}'] <= 500) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'{c}_1000yd_ring'] = ((drug_sod[f'dist_store_{c}'] > 500) & (drug_sod[f'dist_store_{c}'] <= 1000) & (~drug_sod[f'dist_store_{c}'].isnull())).astype(int)
	#
    # OPEN branches
    drug_sod[f'open_{c}_200yd'] = ((drug_sod[f'open_dist_store_{c}'] <= 200) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'open_{c}_500yd'] = ((drug_sod[f'open_dist_store_{c}'] <= 500) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'open_{c}_1000yd'] = ((drug_sod[f'open_dist_store_{c}'] <= 1000) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)
	#
    drug_sod[f'open_{c}_200yd_ring']  = ((drug_sod[f'open_dist_store_{c}'] <= 200) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'open_{c}_500yd_ring']  = ((drug_sod[f'open_dist_store_{c}'] > 200) & (drug_sod[f'open_dist_store_{c}'] <= 500) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)
    drug_sod[f'open_{c}_1000yd_ring'] = ((drug_sod[f'open_dist_store_{c}'] > 500) & (drug_sod[f'open_dist_store_{c}'] <= 1000) & (~drug_sod[f'open_dist_store_{c}'].isnull())).astype(int)

for yd in [200, 500, 1000]:
    # Non-open identities
    drug_sod['test'] = drug_sod[[f'mega_{yd}yd',f'nonmega_{yd}yd',f'cb_{yd}yd']].sum(axis=1)
    assert drug_sod[drug_sod[f'branch_{yd}yd']!=drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'mega_{yd}yd_ring',f'nonmega_{yd}yd_ring',f'cb_{yd}yd_ring']].sum(axis=1)
    assert drug_sod[drug_sod[f'branch_{yd}yd_ring']!=drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'big4_{yd}yd',f'large4_{yd}yd',f'nonmega_{yd}yd',f'cb_{yd}yd']].sum(axis=1)
    assert drug_sod[drug_sod[f'branch_{yd}yd']!=drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'big4_{yd}yd_ring',f'large4_{yd}yd_ring',f'nonmega_{yd}yd_ring',f'cb_{yd}yd_ring']].sum(axis=1)
    assert drug_sod[drug_sod[f'branch_{yd}yd_ring']!=drug_sod.test].shape[0] == 0
	#
    # OPEN identities (parallel)
    drug_sod['test'] = drug_sod[[f'open_mega_{yd}yd', f'open_nonmega_{yd}yd', f'open_cb_{yd}yd']].sum(axis=1)
    assert drug_sod[drug_sod[f'open_branch_{yd}yd'] != drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'open_mega_{yd}yd_ring', f'open_nonmega_{yd}yd_ring', f'open_cb_{yd}yd_ring']].sum(axis=1)
    assert drug_sod[drug_sod[f'open_branch_{yd}yd_ring'] != drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'open_big4_{yd}yd', f'open_large4_{yd}yd', f'open_nonmega_{yd}yd', f'open_cb_{yd}yd']].sum(axis=1)
    assert drug_sod[drug_sod[f'open_branch_{yd}yd'] != drug_sod.test].shape[0] == 0
    drug_sod['test'] = drug_sod[[f'open_big4_{yd}yd_ring', f'open_large4_{yd}yd_ring', f'open_nonmega_{yd}yd_ring', f'open_cb_{yd}yd_ring']].sum(axis=1)
    assert drug_sod[drug_sod[f'open_branch_{yd}yd_ring'] != drug_sod.test].shape[0] == 0

drug_sod = drug_sod.drop(columns='test')
drug_sod.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/drugstore_sod_banksize.dta', write_index = False, version=118)

banksize = drug_sod[['ID_RSSD','mega_asset', 'cb_asset', 'nonmega_asset']].drop_duplicates()
banksize.rename(columns = {'ID_RSSD':'rssdid'}, inplace=True)
banksize.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/banksize_sod.dta', write_index=False, version=118)

######
# Dataset 1-1: Store level data containing bank proximity by bank size.
# -------------------------
# Store-level aggregation: include BOTH existing and OPEN proximities
# -------------------------
cats = ['branch','big4','large4','mega','nonmega','cb']
yards = [200, 500, 1000]
suffixes = ['', '_ring']

agg_spec = {}
for c in cats:
    for yd in yards:
        for s in suffixes:
            col = f'{c}_{yd}yd{s}'
            agg_spec[f'{col}']  = (col, 'max')   # existence within radius
            agg_spec[f'n{col}'] = (col, 'sum')   # counts within radius
			#
            col_open = f'open_{c}_{yd}yd{s}'
            agg_spec[f'{col_open}']  = (col_open, 'max')
            agg_spec[f'n{col_open}'] = (col_open, 'sum')

drugstore_sod = (
    drug_sod
      .groupby(['placekey','d_brand'])
      .agg(**agg_spec)
      .reset_index()
)

# Optional store-level identity checks for counts
for yd in yards:
    test = drugstore_sod[[f'nbig4_{yd}yd', f'nlarge4_{yd}yd', f'nnonmega_{yd}yd', f'ncb_{yd}yd']].sum(axis=1)
    assert (drugstore_sod[f'nbranch_{yd}yd'] == test).all()
    test = drugstore_sod[[f'nbig4_{yd}yd_ring', f'nlarge4_{yd}yd_ring', f'nnonmega_{yd}yd_ring', f'ncb_{yd}yd_ring']].sum(axis=1)
    assert (drugstore_sod[f'nbranch_{yd}yd_ring'] == test).all()
	#
    test = drugstore_sod[[f'nopen_big4_{yd}yd', f'nopen_large4_{yd}yd', f'nopen_nonmega_{yd}yd', f'nopen_cb_{yd}yd']].sum(axis=1)
    assert (drugstore_sod[f'nopen_branch_{yd}yd'] == test).all()
    test = drugstore_sod[[f'nopen_big4_{yd}yd_ring', f'nopen_large4_{yd}yd_ring', f'nopen_nonmega_{yd}yd_ring', f'nopen_cb_{yd}yd_ring']].sum(axis=1)
    assert (drugstore_sod[f'nopen_branch_{yd}yd_ring'] == test).all()

# Column selection: keep both existing and open (existence + counts)
exist_cols = [c for c in drugstore_sod.columns
              if re.match(r'^(big4|large4|mega|nonmega|cb|branch|open_).*(yd|yd_ring)$', c)]
count_cols = [c for c in drugstore_sod.columns
              if c.startswith('n') and re.search(r'(yd|yd_ring)$', c)]

final_cols = ['placekey', 'd_brand'] + exist_cols + count_cols
final_cols = list(set(final_cols))
drugstore_sod = drugstore_sod[final_cols]

drugstore_sod.to_stata(
    'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/drugstore_dist_banksize.dta',
    write_index=False, version=118
)

