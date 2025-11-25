import pandas as pd, numpy as np
import requests

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

def summarize_sba_by_cty(pharmacy_merged: pd.DataFrame,
                         cty_pharmacy19: pd.DataFrame,
                         tag: str,
                         out_path: str,
                         drop_fips_prefix: str = r'^78',
                         log_merge_counts: bool = False) -> pd.DataFrame:
    """
    Build county-level SBA aggregates for pharmacies and export to Stata.
    Required columns:
      pharmacy_merged: ['fips','GrossApproval']
      cty_pharmacy19 : ['fips','est']
    """
    # Ensure consistent fips typing
    df = pharmacy_merged.copy()
    df['fips'] = df['fips'].astype(str).str.zfill(5)
    cty = cty_pharmacy19.copy()
    cty['fips'] = cty['fips'].astype(str).str.zfill(5)
    # Aggregate loans
    out = (
        df.groupby('fips', as_index=False)['GrossApproval']
          .agg(sum='sum', count='count')
          .rename(columns={'sum':'LoanAmt', 'count':'NoLoan'})
    )
    #
    # Drop territories by prefix if needed
    if drop_fips_prefix:
        out = out[~out['fips'].str.contains(drop_fips_prefix, na=False)]
    #
    # Merge to get establishment counts
    out = out.merge(cty[['fips','est']], on='fips', how='left', indicator=True)
    if log_merge_counts:
        print(out['_merge'].value_counts(dropna=False))
    out.rename(columns = {'_merge':'sba_cbp_match'}, inplace=True)
    # out = out[out['_merge'] == 'both'].drop(columns='_merge')
    #
    # Safe per-establishment rates
    est = out['est'].replace({0: np.nan})
    out['NoLoan_per_est']  = out['NoLoan']  / est
    out['LoanAmt_per_est'] = out['LoanAmt'] / est
    #
    # Final column names
    cols = out.columns.tolist()
    out.columns = ['cty'] + [f'{c}{tag}' for c in cols if c != 'fips']
    # Export
    out.to_stata(out_path, write_index=False, version=118)
    return out

sba0008 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2000-fy2009-asof-250630.csv')
sba0919 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2009-fy2019-asof-250630.csv')
sba2025 = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2020-present-asof-250630.csv')
#
sba0025 = pd.concat([sba0008, sba0919, sba2025], axis=0)

# read CBP
cbp19 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/CBP/cbp19co.txt')
cbp19['fipstate'] = cbp19['fipstate'].astype(str)
cbp19['fipscty'] = cbp19['fipscty'].astype(str)
cbp19['fips'] = cbp19.fipstate.str.zfill(2) + cbp19.fipscty.str.zfill(3)
assert cbp19[cbp19.fips.str.len()!=5].shape[0] == 0
cty_pharmacy19 = cbp19[cbp19.naics=='446110']
cty_pharmacy19 = cty_pharmacy19[['fips','est']] # counties without data don't have pharmacies or have less than 3 pharmacies.
#
sba_pharmacy = sba0025[(sba0025.NAICSCode==446110)]
sba_pharmacy = sba_pharmacy[sba_pharmacy.LoanStatus.str.contains('PIF|CHGOFF|EXEMPT',na=False)]
sba_pharmacy = sba_pharmacy[sba_pharmacy.BorrState!='PR']
sba_pharmacy['BorrName'] = sba_pharmacy['BorrName'].str.replace('"','')
sba_pharmacy['BorrZip'] = sba_pharmacy['BorrZip'].astype(str).str.zfill(5)
sba_pharmacy.to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_pharmacy_2000_2019.csv',index=False)
#
sba_pharmacy = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba_pharmacy_2000_2019_geocoded.csv')
sba_pharmacy.columns = sba_pharmacy.columns.str.replace('USER_','')
sba_pharmacy.rename(columns = {'Y':'lat','X':'lon'}, inplace=True)
sba_pharmacy['fips'] = sba_pharmacy.apply(lambda row: get_county(row['lat'], row['lon']), axis=1)
sba_pharmacy[(sba_pharmacy.BorrStreet.str.contains('Box')) | (sba_pharmacy.BorrStreet.isnull())][['BorrStreet','BorrCity','BorrState','BorrZip','Match_addr','fips']] # Only AK, CO, VA had county border changes between 2000 and today.
sba_pharmacy['ApprovalDate'] = pd.to_datetime(sba_pharmacy['ApprovalDate'])
#
pharmacy_0019 = sba_pharmacy[sba_pharmacy.ApprovalDate.between('2000-01-01','2019-12-31')]
pharmacy_0019 = summarize_sba_by_cty(
    pharmacy_0019,
    cty_pharmacy19,
    tag='0019',
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba0019_cty.dta',
    log_merge_counts=True
)
#
pharmacy_1019 = sba_pharmacy[sba_pharmacy.ApprovalDate.between('2010-01-01','2019-12-31')]
pharmacy_1019 = summarize_sba_by_cty(
    pharmacy_1019,
    cty_pharmacy19,
    tag='1019',
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1019_cty.dta',
    log_merge_counts=True
)
#
pharmacy_1519 = sba_pharmacy[sba_pharmacy.ApprovalDate.between('2015-01-01','2019-12-31')]
pharmacy_1519 = summarize_sba_by_cty(
    pharmacy_1519,
    cty_pharmacy19,
    tag='1519',
    out_path='C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_cty.dta',
    log_merge_counts=True
)
# County: no of loans, total Gross Approvalloan amount, no. of loans per pharmacies, loan amount per pharmacy (3 year and 5 year average)
# Number of pharmacies by size can  also suppressed for confidentiality reasons. 72% of counties suppress info on # of small pharmacies.
# BusinessAge: from 2018
# megaXRecovery3XHSBA_loans

#
#
