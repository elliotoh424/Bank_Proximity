import requests, pandas as pd, math, time, numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm

def get_county(lat, lon):
    url = (
        f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?"
        f"x={lon}&y={lat}&benchmark=Public_AR_Current&vintage=Census2010_Current&format=json"
    )
    r = requests.get(url).json()
    try:
        fips = r['result']['geographies']['Counties'][0]['GEOID']
        return fips
    except:
        return None

def load_cbp_pharmacy(year, path_prefix="C:/Users/elliotoh/Box/Chenyang-Elliot/CBP/"):
    # build file path
    filepath = f"{path_prefix}cbp{year}co.txt"
    
    # load
    cbp = pd.read_csv(filepath)
    cbp.columns = cbp.columns.str.lower()
    # create fips
    cbp["fipstate"] = cbp["fipstate"].astype(str)
    cbp["fipscty"] = cbp["fipscty"].astype(str)
    cbp["fips"] = cbp.fipstate.str.zfill(2) + cbp.fipscty.str.zfill(3)
    assert cbp[cbp.fips.str.len()!=5].shape[0] == 0
    
    # filter NAICS 446110 (pharmacies) and select needed columns
    cty_pharmacy = cbp[cbp.naics == "446110"][["fips", "est"]]
    
    return [cbp,cty_pharmacy]


def summarize_sba_by_cty(
    pharmacy_df: pd.DataFrame,
    cty_df: pd.DataFrame,
    tag: str,
    out_path: str,
    years: tuple[int, int],
    drop_fips_prefix: str = r'^78',
    log_merge_counts: bool = False,
    normalize_by_years: bool = True,
) -> pd.DataFrame:
    """
    Build county-level SBA aggregates for pharmacies and export to Stata.
    Inputs must include:
      pharmacy_df: columns ['fips','GrossApproval','ApprovalDate','comp_id']
      cty_df     : columns ['fips','est'] and optionally 'year'
    years: (start_year, end_year), inclusive. Example: (2000, 2019) -> 20 years.
    normalize_by_years: divide count-like variables by number of years.
    """
    start_year, end_year = years
    n_years = end_year - start_year + 1
    if n_years <= 0:
        raise ValueError("years must be (start_year, end_year) with start_year <= end_year")
    #
    # ---- Filter SBA to year window and prep keys
    df = pharmacy_df.copy()
    df['fips'] = df['fips'].astype(str).str.zfill(5)
    df = df[(df['ApprovalDate'] >= f'{start_year}-01-01') & (df['ApprovalDate'] <= f'{end_year}-12-31')]
    #
    # ---- Prep county establishments; average across the year window if a 'year' col exists
    cty = cty_df.copy()
    cty['fips'] = cty['fips'].astype(str).str.zfill(5)
    #
    # ---- Outer merge so counties with zero loans appear
    df = df.merge(cty, on='fips', how='outer', indicator=True)
    if log_merge_counts:
        print(df['_merge'].value_counts(dropna=False))
    df['GrossApproval'] = df['GrossApproval'].fillna(0)
    #
    # ---- Aggregate loans at county level
    out = (
        df.groupby('fips', as_index=False)
          .agg(
              LoanAmt=('GrossApproval', 'mean'),   # mean loan size over the window (kept as-is)
              NoLoan=('ApprovalDate', 'count'),    # number of loans over the window
              NoCompany=('comp_id', 'nunique')     # unique firms over the window
    )
    )
    #
    # Sanity check: no positive loan counts with zeroed amounts (given mean-based LoanAmt)
    assert out[(out.LoanAmt == 0) & (out.NoLoan != 0)].shape[0] == 0
    #
    # ---- Optional per-year normalization (only for counts; dividing a mean by years is not meaningful)
    if normalize_by_years:
        out['LoanAmt'] = out['LoanAmt'] / n_years
        out['NoLoan'] = out['NoLoan'] / n_years
        out['NoCompany'] = out['NoCompany'] / n_years
    #
    # ---- Drop territories by FIPS prefix if requested
    if drop_fips_prefix:
        out = out[~out['fips'].str.contains(drop_fips_prefix, na=False)]
    #
    # ---- Bring back est and tag merge status
    out = out.merge(cty[['fips', 'est']], on='fips', how='left', indicator=True)
    if log_merge_counts:
        print(out['_merge'].value_counts(dropna=False))
    out = out.rename(columns={'_merge': 'sba_cbp_match'})
    #
    # ---- Per-establishment rates (safe divide)
    out['est'] = out['est'].replace({0: np.nan})
    out['NoLoan_est'] = out['NoLoan'] / out['est']
    out['LoanAmt_est'] = out['LoanAmt'] / out['est']
    out['NoCompany_est'] = out['NoCompany'] / out['est']
    #
    # base columns for per-establishment calculation
    # ---- Final names with tag
    rename_map = {'fips': 'cty'}
    for c in out.columns:
        if c != 'fips':
            rename_map[c] = f'{c}{tag}'
    #
    out = out.rename(columns=rename_map)
    #
    # ---- Export
    out.to_stata(out_path, write_index=False, version=118)
    return out


# read CBP
cbp00 = load_cbp_pharmacy('00')[0]
cty_pharmacy00 = load_cbp_pharmacy('00')[1]
cty_retail00 = cbp00[cbp00.naics.str.contains('^44--|^45\d//')]
cty_retail00 = cty_retail00.groupby('fips').est.sum().reset_index()
cty_all00 = cbp00[cbp00.naics.str.contains('^--')]
cty_all00[['n1_4','n5_9','n10_19']].dtypes
cty_all00['est_small'] = cty_all00.n1_4 + cty_all00.n5_9 + cty_all00.n10_19
#
cbp05 = load_cbp_pharmacy('05')[0]
cty_pharmacy05 = load_cbp_pharmacy('05')[1]
cty_retail05 = cbp05[cbp05.naics.str.contains('^44--|^45\d//')]
cty_retail05 = cty_retail05.groupby('fips').est.sum().reset_index()
cty_all05 = cbp05[cbp05.naics.str.contains('^--')]
cty_all05[['n1_4','n5_9','n10_19']].dtypes
cty_all05['est_small'] = cty_all05.n1_4 + cty_all05.n5_9 + cty_all05.n10_19
#
cbp10 = load_cbp_pharmacy('10')[0]
cty_pharmacy10 = load_cbp_pharmacy('10')[1]
cty_retail10 = cbp10[cbp10.naics.str.contains('^44--|^45\d//')]
cty_retail10 = cty_retail10.groupby('fips').est.sum().reset_index()
cty_all10 = cbp10[cbp10.naics.str.contains('^--')]
cty_all10[['n1_4','n5_9','n10_19']].dtypes
cty_all10['est_small'] = cty_all10.n1_4 + cty_all10.n5_9 + cty_all10.n10_19
#
cbp13 = load_cbp_pharmacy('13')[0]
cty_pharmacy13 = load_cbp_pharmacy('13')[1]
cty_retail13 = cbp13[cbp13.naics.str.contains('^44--|^45\d//')]
cty_retail13 = cty_retail13.groupby('fips').est.sum().reset_index()
cty_all13 = cbp13[cbp13.naics.str.contains('^--')]
cty_all13[['n1_4','n5_9','n10_19']].dtypes
cty_all13['est_small'] = cty_all13.n1_4 + cty_all13.n5_9 + cty_all13.n10_19
#
cbp15 = load_cbp_pharmacy('15')[0]
cty_pharmacy15 = load_cbp_pharmacy('15')[1]
cty_retail15 = cbp15[cbp15.naics.str.contains('^44--|^45\d//')]
cty_retail15 = cty_retail15.groupby('fips').est.sum().reset_index()
cty_all15 = cbp15[cbp15.naics.str.contains('^--')]
cty_all15[['n1_4','n5_9','n10_19']].dtypes
cty_all15['est_small'] = cty_all15.n1_4 + cty_all15.n5_9 + cty_all15.n10_19
#
cbp19 = load_cbp_pharmacy('19')[0]
cty_pharmacy19 = load_cbp_pharmacy('19')[1]
cty_retail19 = cbp19[cbp19.naics.str.contains('^44--|^45\d//')]
cty_retail19 = cty_retail19.groupby('fips').est.sum().reset_index()
cty_all19 = cbp19[cbp19.naics.str.contains('^--')]
for c in ['n<5','n5_9','n10_19']:
    cty_all19[c] = pd.to_numeric(cty_all19[c], errors='coerce')

cty_all19[['n<5','n5_9','n10_19']].dtypes
cty_all19['est_small'] = cty_all19['n<5'].astype(float) + cty_all19.n5_9.astype(float) + cty_all19.n10_19.astype(float)
# #
# sba0008 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2000-fy2009-asof-250630.csv')
# sba0919 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2009-fy2019-asof-250630.csv')
# sba2025 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2020-present-asof-250630.csv')
# #
# sba0025 = pd.concat([sba0008, sba0919, sba2025], axis=0)
# sba_dispensed = sba0025[sba0025.LoanStatus.str.contains('PIF|CHGOFF|EXEMPT',na=False)]
# sba_dispensed = sba_dispensed[sba_dispensed.BorrState!='PR']
# sba_dispensed.to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019.csv',index=False)
# #
# sba_dispensed = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_geocoded.csv')
# sba_dispensed.columns = sba_dispensed.columns.str.replace('USER_','')
# sba_dispensed.rename(columns = {'Y':'lat','X':'lon'}, inplace=True)
# sba_dispensed['BorrName'] = sba_dispensed['BorrName'].str.replace('"','')
# sba_dispensed['BorrZip'] = sba_dispensed['BorrZip'].astype(str).str.zfill(5)
# sba_dispensed = sba_dispensed[(~sba_dispensed.BorrState.str.contains('^AE$|^AS$|^FM$|^GU$|^MH$|^MP$|^PR$|^PW$|^VI$',na=False))]
# sba_dispensed[sba_dispensed.lat==0][['BorrName','BorrStreet','BorrCity','BorrState','BorrZip']].drop_duplicates().to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_recode.csv',index=False)

# pts = gpd.GeoDataFrame(
#     sba_dispensed.copy(),
#     geometry=gpd.points_from_xy(sba_dispensed["lon"], sba_dispensed["lat"]),
#     crs="EPSG:4326",
# )
# counties = gpd.read_file(r"C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_county_2010.shp")
# if pts.crs != counties.crs:
#     pts = pts.to_crs(counties.crs)

# joined = gpd.sjoin(pts, counties[["GEOID", "geometry"]], how="left", predicate="within")
# joined[(joined["GEOID"].isna())][['BorrCity','BorrState']].drop_duplicates().to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_not_geocoded.csv',index=False)
# # Fill missing GEOID from Census geocoder
# assert joined.loc[(joined.BorrCity=='MOUNTAIN VILLAGE') & (joined.BorrState=='AK') & (joined.GEOID.isnull()),'GEOID'].shape[0] == 1
# joined.loc[(joined.BorrCity=='MOUNTAIN VILLAGE') & (joined.BorrState=='AK'),'GEOID']= '02270'
# assert joined.loc[(joined.BorrCity=='DUTCH HARBOR') & (joined.BorrState=='AK') & (joined.GEOID.isnull()) ,'GEOID'].shape[0] == 1
# joined.loc[(joined.BorrCity=='DUTCH HARBOR') & (joined.BorrState=='AK') & (joined.GEOID.isnull()),'GEOID'] = '02016'
# assert joined.loc[(joined.BorrCity=='Hooper Bay') & (joined.BorrState=='AK') & (joined.GEOID.isnull()) ,'GEOID'].shape[0] == 1
# joined.loc[(joined.BorrCity=='Hooper Bay') & (joined.BorrState=='AK') & (joined.GEOID.isnull()),'GEOID'] = '02270'
# assert joined.loc[(joined.BorrCity=='KETCHIKAN') & (joined.BorrState=='AK') & (joined.GEOID.isnull()) ,'GEOID'].shape[0] == 1
# joined.loc[(joined.BorrCity=='KETCHIKAN') & (joined.BorrState=='AK') & (joined.GEOID.isnull()),'GEOID'] = '02130'
# assert joined.loc[(joined.BorrCity=='Mountain Village') & (joined.BorrState=='AK') & (joined.GEOID.isnull()) ,'GEOID'].shape[0] == 1
# joined.loc[(joined.BorrCity=='Mountain Village') & (joined.BorrState=='AK') & (joined.GEOID.isnull()),'GEOID'] = '02270'
# #
# city_cty = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_not_geocoded_manual.csv',encoding="cp1252")
# city_cty = city_cty[city_cty.fips.notnull()]
# city_cty['fips'] = city_cty['fips'].astype(int).astype(str).str.zfill(5)
# city_cty = city_cty[['BorrCity','BorrState','fips']].drop_duplicates()
# joined = joined.merge(city_cty,how='left',on=['BorrCity','BorrState'], indicator=True)
# joined.loc[(joined._merge=='both') & (joined.GEOID.isnull()),'GEOID'] = joined.loc[(joined._merge=='both') & (joined.GEOID.isnull()),'fips']
# joined = joined.drop(['_merge','fips'],axis=1)
# joined[joined.GEOID.isnull()][['BorrStreet','BorrCity','BorrState','BorrZip','lat','lon','fips']].drop_duplicates().sort_values(['BorrState','BorrZip']).to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_not_ambg_city.csv',index=False)
# #
# address_manual = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_not_ambg_city_manual.csv')
# address_manual = address_manual[address_manual.BorrCity != 'SAINT THOMAS']
# address_manual['lat'] = address_manual.coordinates.str.extract('(^.+),').astype(float)
# address_manual['lon'] = address_manual.coordinates.str.extract(', (.+$)').astype(float)
# address_manual['fips'] = address_manual.apply(lambda row: get_county(row['lat'], row['lon']), axis=1)
# address_manual['BorrZip'] = address_manual['BorrZip'].astype(str).str.zfill(5)
# address_manual = address_manual[['BorrStreet','BorrCity','BorrState','BorrZip','fips']]
# #
# joined = joined.merge(address_manual,how='left',on=['BorrStreet','BorrCity','BorrState','BorrZip'], indicator=True)
# joined._merge.value_counts()
# joined.loc[(joined._merge=='both') & (joined.GEOID.isnull()),'GEOID'] = joined.loc[(joined._merge=='both') & (joined.GEOID.isnull()),'fips']
# joined = joined[joined.BorrCity != 'SAINT THOMAS']
# assert joined[joined.GEOID.isnull()].shape[0] == 0
# joined.columns
#  # Only AK, CO, VA had county border changes between 2000 and today.
# joined[(joined.BorrStreet.str.contains('Box')) | (joined.BorrStreet.isnull())][['BorrStreet','BorrCity','BorrState','BorrZip','Match_addr']]
# joined.to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_dispensed_2000_2019_ctys.csv',index=False)
# #
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
#
sba_dispensed[(sba_dispensed.ApprovalDate.between('2000-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].TerminMonths.describe() # 84-120-120-112 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].TerminMonths.describe() # 84-120-120-119 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].TerminMonths.describe() # 84-120-120-121 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2000-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].TerminMonths.describe() # 60-84-120-110 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].TerminMonths.describe()  # 84-119-120-129 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].TerminMonths.describe() # 84-120-120-132 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2000-01-01','2019-12-31'))].TerminMonths.describe() # 60-84-120-107 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31'))].TerminMonths.describe()  # 84-85-120-122 (25, 50, 75, mean)
sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31'))].TerminMonths.describe() # 84-120-120-125 (25, 50, 75, mean)

#####################################
pharmacy_0019 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2000-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_0019.TerminMonths.describe()
cty_pharmacy0019 = pd.concat([cty_pharmacy00, cty_pharmacy19], axis=0)
cty_pharmacy0019 = cty_pharmacy0019.groupby('fips').est.mean().reset_index()
pharmacy_0019 = summarize_sba_by_cty(
    pharmacy_0019,
    cty_pharmacy0019,
    tag='0019',
    years=(2000, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0019_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
retail_0019 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2000-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].drop("_merge",axis=1)
retail_0019.TerminMonths.describe()
cty_retail0019 = pd.concat([cty_retail00, cty_retail19], axis=0)
cty_retail0019 = cty_retail0019.groupby('fips').est.mean().reset_index()
retail_0019 = summarize_sba_by_cty(
    retail_0019,
    cty_retail0019,
    tag='0019',
    years=(2000, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0019_retail_cty.dta',
    normalize_by_years=True,
    log_merge_counts=True
)
cty_all0019 = pd.concat([cty_all00, cty_all19], axis=0)
cty_all0019 = cty_all0019.groupby('fips').agg({'est':'mean','est_small':'mean'}).reset_index()
cty_all0019.rename(columns = {'est':'allest0019','est_small':'smest_0019'}, inplace=True)
cty_all0019.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0019_all_cty.dta', write_index=False, version=118)
#
#
pharmacy_0519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2005-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_0519.TerminMonths.describe()
cty_pharmacy0519 = pd.concat([cty_pharmacy05, cty_pharmacy19], axis=0)
cty_pharmacy0519 = cty_pharmacy0519.groupby('fips').est.mean().reset_index()
pharmacy_0519 = summarize_sba_by_cty(
    pharmacy_0519,
    cty_pharmacy0519,
    tag='0519',
    years=(2005, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0519_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
retail_0519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2005-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].drop("_merge",axis=1)
retail_0519.TerminMonths.describe()
cty_retail0519 = pd.concat([cty_retail05, cty_retail19], axis=0)
cty_retail0519 = cty_retail0519.groupby('fips').est.mean().reset_index()
retail_0519 = summarize_sba_by_cty(
    retail_0519,
    cty_retail0519,
    tag='0519',
    years=(2005, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0519_retail_cty.dta',
    normalize_by_years=True,
    log_merge_counts=True
)
cty_all0519 = pd.concat([cty_all05, cty_all19], axis=0)
cty_all0519 = cty_all0519.groupby('fips').agg({'est':'mean','est_small':'mean'}).reset_index()
cty_all0519.rename(columns = {'est':'allest0519','est_small':'smest_0519'}, inplace=True)
cty_all0519.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0519_all_cty.dta', write_index=False, version=118)
#
pharmacy_1019 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_1019.TerminMonths.describe()
cty_pharmacy1019 = pd.concat([cty_pharmacy10, cty_pharmacy19], axis=0)
cty_pharmacy1019 = cty_pharmacy1019.groupby('fips').est.mean().reset_index()
pharmacy_1019 = summarize_sba_by_cty(
    pharmacy_1019,
    cty_pharmacy1019,
    tag='1019',
    years=(2010, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1019_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
retail_1019 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].drop("_merge",axis=1)
retail_1019.TerminMonths.describe()
cty_retail1019 = pd.concat([cty_retail10, cty_retail19], axis=0)
cty_retail1019 = cty_retail1019.groupby('fips').est.mean().reset_index()
retail_1019 = summarize_sba_by_cty(
    retail_1019,
    cty_retail1019,
    tag='1019',
    years=(2010, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1019_retail_cty.dta',
    normalize_by_years=True,
    log_merge_counts=True
)
cty_all1019 = pd.concat([cty_all10, cty_all19], axis=0)
cty_all1019 = cty_all1019.groupby('fips').agg({'est':'mean','est_small':'mean'}).reset_index()
cty_all1019.rename(columns = {'est':'allest1019','est_small':'smest_1019'}, inplace=True)
cty_all1019.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1019_all_cty.dta', write_index=False, version=118)
#
pharmacy_1319 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2013-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop("_merge",axis=1)
cty_pharmacy1319 = pd.concat([
    cty_pharmacy13,
    cty_pharmacy19,
], axis=0, ignore_index=True)
cty_pharmacy1319 = cty_pharmacy1319.groupby('fips').est.mean().reset_index()
pharmacy_1319 = summarize_sba_by_cty(
    pharmacy_1319,
    cty_pharmacy1319,
    tag='1319',
    years=(2013, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1319_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
retail_1319 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2013-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].drop("_merge",axis=1)
retail_1319.TerminMonths.describe()
cty_retail1319 = pd.concat([cty_retail00, cty_retail19], axis=0)
cty_retail1319 = cty_retail1319.groupby('fips').est.mean().reset_index()
retail_1319 = summarize_sba_by_cty(
    retail_1319,
    cty_retail1319,
    tag='1319',
    years=(2000, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1319_retail_cty.dta',
    normalize_by_years=True,
    log_merge_counts=True
)
cty_all1319 = pd.concat([cty_all13, cty_all19], axis=0)
cty_all1319 = cty_all1319.groupby('fips').agg({'est':'mean','est_small':'mean'}).reset_index()
cty_all1319.rename(columns = {'est':'allest1319','est_small':'smest_1319'}, inplace=True)
cty_all1319.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1319_all_cty.dta', write_index=False, version=118)
#
pharmacy_1519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_1519.TerminMonths.describe()
cty_pharmacy1519 = pd.concat([cty_pharmacy15, cty_pharmacy19], axis=0)
cty_pharmacy1519 = cty_pharmacy1519.groupby('fips').est.mean().reset_index()
pharmacy_1519_agg = summarize_sba_by_cty(
    pharmacy_1519,
    cty_pharmacy1519,
    tag='1519',
    years=(2015, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
retail_1519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31')) & (sba_dispensed.NAICSCode.str.contains('^4[45]'))].drop('_merge',axis=1)
retail_1519.TerminMonths.describe()
cty_retail1519 = pd.concat([cty_retail15, cty_retail19], axis=0)
cty_retail1519 = cty_retail1519.groupby('fips').est.mean().reset_index()
retail_1519_agg = summarize_sba_by_cty(
    retail_1519,
    cty_retail1519,
    tag='1519',
    years=(2015, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_retail_cty.dta',
    normalize_by_years=True,
    log_merge_counts=True
)
cty_all1519 = pd.concat([cty_all15, cty_all19], axis=0)
cty_all1519 = cty_all1519.groupby('fips').agg({'est':'mean','est_small':'mean'}).reset_index()
cty_all1519.rename(columns = {'est':'allest1519','est_small':'smest_1519'}, inplace=True)
cty_all1519.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_all_cty.dta', write_index=False, version=118)

bds = pd.read_csv('C:/Users/elliotoh/Downloads/bds2022_st_cty_sec.csv')

pd.set_option('display.float_format', '{:.5f}'.format)


