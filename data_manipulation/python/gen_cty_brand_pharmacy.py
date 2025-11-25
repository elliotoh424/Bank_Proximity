import pandas as pd, math, time, numpy as np, gc
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# cvs_words = ['^CVS PHARMAC','^CVS DRUG']
# riteaid_words = ['^RITE AIDE?$','^RITE AID PHARMAC','RITE AID DRUG','']

info_keys = pd.read_parquet('G:/WRDS_Columbia/infogroup_abi_ticker_map.parquet')
parent_abis = [400624805, 7531742, 7539588, 5136874, 433580917] # CVS, RITE AID, WALGREENS, DUANE READE (SUB + PARENT)

# df = []
# for y in range(1997,2020):
#     cols = ['abi', 'subsidiary_number', 'parent_number', 'company', 'address_line_1', 'city', 'state', 'zipcode', 'county_code',
#             'business_status_code', 'company_holding_status', # ownership, public/private
#             'match_code', 'fips_code', 'latitude','longitude']
#     info = pd.read_parquet(f'G:/WRDS_Columbia/infogroup/infogroup_enc_{y}.parquet', columns = cols)
#     info['year'] = y
#     info['parent_number'] = pd.to_numeric(info.parent_number, errors='coerce')
#     top4 = info[info.parent_number.isin(parent_abis)]
#     print(y, top4.shape[0])
#     df.append(top4)
#     del info
#     gc.collect()

df = pd.concat(df, axis=0)
# df.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/top4_pharmacy_infogroup.parquet',index=False)
# df[['abi','address_line_1','city','state','zipcode']].drop_duplicates().to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/top4_pharmacy_infogroup.csv',index=False)

# 
df = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/top4_pharmacy_infogroup.parquet')
df_geocoded = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/top4_pharmacy_infogroup_geocoded.csv')
df_geocoded.columns = df_geocoded.columns.str.replace('USER_','')
df['abi'] = df['abi'].astype(int)
df['zipcode'] = df['zipcode'].astype(int)
df['address_line_1'] = df['address_line_1'].fillna('')
df_geocoded['address_line_1'] = df_geocoded['address_line_1'].fillna('')
#
df_merged = df.merge(df_geocoded, on=['abi','address_line_1','city','state','zipcode'], how='outer', indicator=True)
df_merged._merge.value_counts()
cols = ['abi', 'subsidiary_number', 'parent_number', 'company', 'address_line_1', 'city', 'state', 'zipcode', 'county_code', 
        'business_status_code', 'company_holding_status', 'match_code', 'fips_code', 'year', 
        'Score', 'ShortLabel', 'Y', 'X']
df_merged = df_merged[cols]
df_merged.rename(columns = {'Y':'latitude','X':'longitude'}, inplace=True)
#
pts = gpd.GeoDataFrame(
    df_merged.copy(),
    geometry=gpd.points_from_xy(df_merged["longitude"], df_merged["latitude"]),
    crs="EPSG:4326",
)
counties = gpd.read_file(r"C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_county_2010.shp")
if pts.crs != counties.crs:
    pts = pts.to_crs(counties.crs)

joined = gpd.sjoin(pts, counties[["GEOID", "geometry"]], how="left", predicate="within")
assert joined[joined.GEOID.isna()].shape[0] == 0
cols = ['abi', 'subsidiary_number', 'parent_number', 'company', 'address_line_1', 'city', 'state', 'zipcode', 'business_status_code', 'company_holding_status', 'match_code',
       'year', 'Score', 'ShortLabel', 'latitude', 'longitude', 'GEOID']
joined = joined[cols]
joined.rename(columns = {'GEOID':'fips'}, inplace=True)
cty_top4 = joined.groupby(['fips','year'])['abi'].nunique().reset_index()


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

# read CBP
cty_pharmacy00 = load_cbp_pharmacy('00')[1]
cty_pharmacy00 = cty_pharmacy00.merge(cty_top4[cty_top4.year==2000], on ='fips', how='left', indicator=True)
cty_pharmacy00.loc[cty_pharmacy00._merge!='both','abi'] = 0
cty_pharmacy00['iest'] = cty_pharmacy00.est - cty_pharmacy00.abi
print(cty_pharmacy00.loc[cty_pharmacy00.iest<=0,'iest'].shape[0]/cty_pharmacy00.shape[0])
cty_pharmacy00.loc[cty_pharmacy00.iest<=0,'iest'] = 0
#
cty_pharmacy05 = load_cbp_pharmacy('05')[1]
cty_pharmacy05 = cty_pharmacy05.merge(cty_top4[cty_top4.year==2005], on ='fips', how='left', indicator=True)
cty_pharmacy05.loc[cty_pharmacy05._merge!='both','abi'] = 0
cty_pharmacy05['iest'] = cty_pharmacy05.est - cty_pharmacy05.abi
print(cty_pharmacy05.loc[cty_pharmacy05.iest<=0,'iest'].shape[0]/cty_pharmacy05.shape[0])
cty_pharmacy05.loc[cty_pharmacy05.iest<=0,'iest'] = 0
#
cty_pharmacy10 = load_cbp_pharmacy('10')[1]
cty_pharmacy10 = cty_pharmacy10.merge(cty_top4[cty_top4.year==2010], on ='fips', how='left', indicator=True)
cty_pharmacy10.loc[cty_pharmacy10._merge!='both','abi'] = 0
cty_pharmacy10['iest'] = cty_pharmacy10.est - cty_pharmacy10.abi
print(cty_pharmacy10.loc[cty_pharmacy10.iest<=0,'iest'].shape[0]/cty_pharmacy10.shape[0])
cty_pharmacy10.loc[cty_pharmacy10.iest<=0,'iest'] = 0
cty_est10 = load_cbp_pharmacy('10')[0]

#
cty_pharmacy13 = load_cbp_pharmacy('13')[1]
cty_pharmacy13 = cty_pharmacy13.merge(cty_top4[cty_top4.year==2013], on ='fips', how='left', indicator=True)
cty_pharmacy13.loc[cty_pharmacy13._merge!='both','abi'] = 0
cty_pharmacy13['iest'] = cty_pharmacy13.est - cty_pharmacy13.abi
print(cty_pharmacy13.loc[cty_pharmacy13.iest<=0,'iest'].shape[0]/cty_pharmacy13.shape[0])
cty_pharmacy13.loc[cty_pharmacy13.iest<=0,'iest'] = 0
#
cty_pharmacy15 = load_cbp_pharmacy('15')[1]
cty_pharmacy15 = cty_pharmacy15.merge(cty_top4[cty_top4.year==2015], on ='fips', how='left', indicator=True)
cty_pharmacy15.loc[cty_pharmacy15._merge!='both','abi'] = 0
cty_pharmacy15['iest'] = cty_pharmacy15.est - cty_pharmacy15.abi
print(cty_pharmacy15.loc[cty_pharmacy15.iest<=0,'iest'].shape[0]/cty_pharmacy15.shape[0])
cty_pharmacy15.loc[cty_pharmacy15.iest<=0,'iest'] = 0
#
cty_pharmacy19 = load_cbp_pharmacy('19')[1]
cty_pharmacy19 = cty_pharmacy19.merge(cty_top4[cty_top4.year==2019], on ='fips', how='left', indicator=True)
cty_pharmacy19.loc[cty_pharmacy19._merge!='both','abi'] = 0
cty_pharmacy19['iest'] = cty_pharmacy19.est - cty_pharmacy19.abi
print(cty_pharmacy19.loc[cty_pharmacy19.iest<=0,'iest'].shape[0]/cty_pharmacy19.shape[0])
cty_pharmacy19.loc[cty_pharmacy19.iest<=0,'iest'] = 0


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
    out = out.merge(cty[['fips', 'iest']], on='fips', how='left', indicator=True)
    if log_merge_counts:
        print(out['_merge'].value_counts(dropna=False))
    out = out.rename(columns={'_merge': 'sba_cbp_match'})
    #
    # ---- Per-establishment rates (safe divide)
    out['iest'] = out['iest'].replace({0: np.nan})
    out['NoLoan_iest'] = out['NoLoan'] / out['iest']
    out['LoanAmt_iest'] = out['LoanAmt'] / out['iest']
    out['NoCompany_iest'] = out['NoCompany'] / out['iest']
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
cty_pharmacy0019 = cty_pharmacy0019.groupby('fips').iest.mean().reset_index()
pharmacy_0019 = summarize_sba_by_cty(
    pharmacy_0019,
    cty_pharmacy0019,
    tag='0019',
    years=(2000, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0019_ind_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
pharmacy_0019[['LoanAmt_iest0019','NoLoan_iest0019']].describe()
#
pharmacy_0519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2005-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_0519.TerminMonths.describe()
cty_pharmacy0519 = pd.concat([cty_pharmacy05, cty_pharmacy19], axis=0)
cty_pharmacy0519 = cty_pharmacy0519.groupby('fips').iest.mean().reset_index()
pharmacy_0519 = summarize_sba_by_cty(
    pharmacy_0519,
    cty_pharmacy0519,
    tag='0519',
    years=(2005, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0519_ind_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
pharmacy_0519[['LoanAmt_iest0519','NoLoan_iest0519']].describe()
#
pharmacy_1019 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2010-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_1019.TerminMonths.describe()
pharmacy_1019['LoanAmt']
pharmacy_1019.columns


cty_pharmacy1019 = pd.concat([cty_pharmacy10, cty_pharmacy19], axis=0)
cty_pharmacy1019 = cty_pharmacy1019.groupby('fips').iest.mean().reset_index()
pharmacy_1019 = summarize_sba_by_cty(
    pharmacy_1019,
    cty_pharmacy1019,
    tag='1019',
    years=(2010, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1019_ind_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
pharmacy_1019[['LoanAmt_iest1019','NoLoan_iest1019']].describe()
#
pharmacy_1319 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2013-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop("_merge",axis=1)
cty_pharmacy1319 = pd.concat([
    cty_pharmacy13,
    cty_pharmacy19,
], axis=0, ignore_index=True)
cty_pharmacy1319 = cty_pharmacy1319.groupby('fips').iest.mean().reset_index()
pharmacy_1319 = summarize_sba_by_cty(
    pharmacy_1319,
    cty_pharmacy1319,
    tag='1319',
    years=(2013, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1319_ind_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
pharmacy_1319[['LoanAmt_iest1319','NoLoan_iest1319']].describe()
#
pharmacy_1519 = sba_dispensed[(sba_dispensed.ApprovalDate.between('2015-01-01','2019-12-31')) & (sba_dispensed.NAICSCode=='446110.0')].drop('_merge',axis=1)
pharmacy_1519.TerminMonths.describe()
cty_pharmacy1519 = pd.concat([cty_pharmacy15, cty_pharmacy19], axis=0)
cty_pharmacy1519 = cty_pharmacy1519.groupby('fips').iest.mean().reset_index()
pharmacy_1519 = summarize_sba_by_cty(
    pharmacy_1519,
    cty_pharmacy1519,
    tag='1519',
    years=(2015, 2019),  # inclusive; 20 years
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_ind_cty.dta',
    log_merge_counts=True,
    normalize_by_years=True
)
pharmacy_1519[['LoanAmt_iest1519','NoLoan_iest1519']].describe()

