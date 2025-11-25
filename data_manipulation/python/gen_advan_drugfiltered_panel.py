import pathlib as path, pandas as pd, numpy as np, pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import pyarrow.dataset as ds
from datetime import timedelta
from pyarrow import csv
import os, re, gc, time

##################################################################################################################################
# Analysis of Filter drug store sample to exclude shared polygons, grocery pharmacy chains, and colocation.
##################################################################################################################################
# Analysis of difference between balanced and full drug panel. nplacekey: 112064 (full) -> 98277 (balanced). Key difference   
drug_bal = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_balanced.parquet')
drug_full = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_full.parquet')
drug_local = pd.read_parquet('D:/MD_Opportunity/data/advan/drugpoi_local_traffic.parquet')
print('balanced panel nobs, nplacekey:',drug_bal.shape[0], drug_bal.placekey.drop_duplicates().shape[0])
print('Full panel (has obs in Jan 2020) nobs, nplacekey:',drug_full.shape[0], drug_full.placekey.drop_duplicates().shape[0])
print('drug stores with local info (has poi_cbg and home_cbg) nobs, nplacekey:',drug_local.shape[0], drug_local.placekey.drop_duplicates().shape[0])

# nobs and nplacekey in full and not balanced: 482545 and 13787.
# 84+% of difference due to missing nvisits in Jan 2020. Rest due to nvisits jan 2020 < 10.
print('nobs and nplacekey in full and not balanced:', drug_full[(~drug_full.placekey.isin(drug_bal.placekey))].shape[0], drug_full[(~drug_full.placekey.isin(drug_bal.placekey))].placekey.drop_duplicates().shape[0])
null_cond = (~drug_full.placekey.isin(drug_bal.placekey)) & (drug_full.nvisits_jan2020.isnull())
print('nobs and nplacekey that are null in jan 2020 nvisits:', drug_full[null_cond].shape[0], drug_full[null_cond].placekey.drop_duplicates().shape[0])
notnull_cond = (~drug_full.placekey.isin(drug_bal.placekey)) & (~drug_full.nvisits_jan2020.isnull() & (drug_full.nvisits_jan2020<10))
print('nobs and nplacekey jan 2020 nvisits < 10:', drug_full[notnull_cond].shape[0], drug_full[notnull_cond].placekey.drop_duplicates().shape[0])

##################################################################################################################################
# Filter drug store sample to exclude shared polygons, grocery pharmacy chains, and colocation.
##################################################################################################################################
# Manually identify grocery pharmacy chains based on brands from full data.
cols = ['brands']
drug_full = ds.dataset('D:/MD_Opportunity/data/advan/all_foottraffic_monthly_201901_202211_full.parquet')
drug_full = drug_full.to_table(filter=(((ds.field('naics_code')=='446110'))), columns = cols).to_pandas().drop_duplicates()
drug_full = drug_full[drug_full.brands.str.len()>0]
drug_full.sort_values(['brands'], inplace=True)

grocery_brands = ["ACME Sav-on Pharmacy", "Albertson", "Costco Pharmacy", "Food Lion Pharmacy", "Fred Meyer Pharmacy", "GIANT Pharmacy", "Giant Food Pharmacy", "H-E-B Pharmacy", "Haggen Pharmacy", "Hannaford Pharmacy", "Harris Teeter Pharmacy", "Harvey's Supermarket Pharmacy", "Homeland Pharmacy", "Ingles Pharmacy", "Jewel-Osco Pharmacy", "King Sooper", "Kmart Pharmacy", "Kroger Pharmacy", "Kroger Specialty Pharmacy", "Martins Pharmacy", "Meijer Pharmacy", "Publix Pharmacy", "QFC Pharmacy", "Safeway Pharmacy", "ShopRite Pharmacy", "Stop & Shop Pharmacy", "Super 1 Pharmacy", "Super Saver", "Tom Thumb Pharmacy", "Tops Pharmacy", "Vons Pharmacy", "Walmart Pharmacy", "Wegmans Pharmacy", "Winn Dixie Pharmacy", "Yokes Pharmacy"]

# Colocated with grocery stores.
cols = ['placekey','street_address','city','region','zip','location_name','brands']
grocery_full = ds.dataset('D:/MD_Opportunity/data/advan/all_foottraffic_monthly_201901_202211_full.parquet')
grocery_full = grocery_full.to_table(filter=(((ds.field('naics_code').isin(['445110','452311'])))), columns = cols).to_pandas().drop_duplicates()
grocery_full['streetno'] = grocery_full.street_address.str.extract('(^[A-z0-9]+) ')
grocery_full['place9'] = grocery_full.placekey.str.extract('@(.{3}-.{3}-.{3}$)')
grocery_full['stadd'] = grocery_full.street_address.str.replace(' Ste.+','',regex=True)
#
drug_full = ds.dataset('D:/MD_Opportunity/data/advan/all_foottraffic_monthly_201901_202211_full.parquet')
drug_full = drug_full.to_table(filter=(((ds.field('naics_code')=='446110'))), columns = cols).to_pandas().drop_duplicates()
drug_full['streetno'] = drug_full.street_address.str.extract('(^[A-z0-9]+) ')
drug_full['place9'] = drug_full.placekey.str.extract('@(.{3}-.{3}-.{3}$)')
drug_full['stadd'] = drug_full.street_address.str.replace(' Ste.+','',regex=True)

# Grocery name matches
grocery_brands = ["ACME Sav-on Pharmacy", "Albertson", "Costco Pharmacy", "Food Lion Pharmacy", "Fred Meyer Pharmacy", "GIANT Pharmacy", "Giant Food Pharmacy", "H-E-B Pharmacy", "Haggen Pharmacy", "Hannaford Pharmacy", "Harris Teeter Pharmacy", "Harvey's Supermarket Pharmacy", "Homeland Pharmacy", "Ingles Pharmacy", "Jewel-Osco Pharmacy", "King Sooper", "Kmart Pharmacy", "Kroger Pharmacy", "Kroger Specialty Pharmacy", "Martins Pharmacy", "Meijer Pharmacy", "Publix Pharmacy", "QFC Pharmacy", "Safeway Pharmacy", "ShopRite Pharmacy", "Stop & Shop Pharmacy", "Super 1 Pharmacy", "Super Saver", "Tom Thumb Pharmacy", "Tops Pharmacy", "Vons Pharmacy", "Walmart Pharmacy", "Wegmans Pharmacy", "Winn Dixie Pharmacy", "Yokes Pharmacy"]
grocery_brands = '|'.join(grocery_brands)
grocery_match = drug_full[drug_full.brands.str.contains(grocery_brands, na=False, flags=re.IGNORECASE)].placekey.drop_duplicates()

# Street-city-state-zip match
exact_match = drug_full.merge(grocery_full, on =['street_address','city','region','zip'])
exact_match.columns = exact_match.columns.str.replace('_x$','',regex=True)
exact_match = exact_match.placekey.drop_duplicates()
# Street no-city-state-zip-place9 match
place9_match = drug_full.merge(grocery_full, on =['streetno','place9','city','region','zip'])
place9_match.columns = place9_match.columns.str.replace('_x$','',regex=True)
place9_match = place9_match[~place9_match.placekey.isin(exact_match)].placekey.drop_duplicates()
# Modified Street-city-state-zip-place9 match
mod_match = drug_full.merge(grocery_full, on =['stadd','city','region','zip'])
mod_match = mod_match[~mod_match.placekey_x.isin(exact_match)]
mod_match = mod_match[~mod_match.placekey_x.isin(place9_match)]
mod_match.columns = mod_match.columns.str.replace('_x$','',regex=True)
mod_match = mod_match.placekey.drop_duplicates()
# Shared polygon placekeys
shared_polygon = pd.read_parquet('D:/MD_Opportunity/data/advan/shared_polygon.parquet')
shared_polygon.columns = shared_polygon.columns.str.lower()
shared_match = drug_full[(drug_full.placekey.isin(shared_polygon.placekey_primary)) | (drug_full.placekey.isin(shared_polygon.placekey_shared))]
shared_match = shared_match[(~shared_match.placekey.isin(exact_match)) & (~shared_match.placekey.isin(place9_match)) & (~shared_match.placekey.isin(mod_match))].placekey
print('exact match:',exact_match.shape[0])
print('place9 match:',place9_match.shape[0])
print('modified match:',mod_match.shape[0])
print('shared match:',shared_match.shape[0])
filter_placekeys = list(set(grocery_match.tolist() + exact_match.tolist() + place9_match.tolist() + mod_match.tolist() + shared_match.tolist() ))
assert len(filter_placekeys) == len(set(filter_placekeys))
print('Initial placekeys:', drug_full.placekey.drop_duplicates().shape[0]) # 112064 drug stores starting
print('Remaining placekeys:', drug_full.placekey.drop_duplicates().shape[0] - len(filter_placekeys)) # 24860 drug stores remaining

# Save excluded placekeys separately.
pd.DataFrame(grocery_match).to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/grocery_drug_placekeys.dta', write_index=False, version=118)
pd.DataFrame(exact_match).to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/colocate_exact_drug_placekeys.dta', write_index=False, version=118)
pd.DataFrame(place9_match).to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/colocate_place9_drug_placekeys.dta', write_index=False, version=118)
pd.DataFrame(mod_match).to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/colocate_mod_drug_placekeys.dta', write_index=False, version=118)
pd.DataFrame(shared_match).to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/shared_polygon_drug_placekeys.dta', write_index=False, version=118)
# Save original balanced store sample
drug_bal = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_balanced.parquet')
drug_bal.to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_balanced.dta', write_index=False, version=118)

# Read and write final filtered drug panel. 
drug_bal = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_balanced.parquet', filters= [('placekey','not in', filter_placekeys)])
#
keep_cols = ['placekey', 'date_range_start', 'nvisitor_from_home_total', 'nvisitor_from_home_ingeo_total', 'nvisitor_from_home_in_2', 'nvisitor_from_home_in_2_ntract', 'nvisitor_from_home_in_5', 'nvisitor_from_home_in_5_ntract', 'nvisitor_from_home_in_10', 'nvisitor_from_home_in_10_ntract', 'nvisitor_from_home_in_15', 'nvisitor_from_home_in_15_ntract', 'nvisitor_from_home_in_20', 'nvisitor_from_home_in_20_ntract',  'nvisitor_from_home_ratio', 'nvisitor_from_home_ingeo_ratio']
drug_local = pd.read_parquet('D:/MD_Opportunity/data/advan/drugpoi_local_traffic.parquet', columns = keep_cols)
drug_local['date_range_start'] = pd.to_datetime(drug_local['date_range_start'])
drug_local['month'] = drug_local.date_range_start.dt.month
for d in [2,5,10,15,20]:
    drug_local[f'nvisitor_from_home_out_{d}'] = drug_local.nvisitor_from_home_total - drug_local[f'nvisitor_from_home_in_{d}']
#
drug_local2019 = drug_local[drug_local.date_range_start.dt.year==2019]
drop_cols = [x for x in drug_local2019.columns.tolist() if re.search('ntract|date_range_start',x)]
drug_local2019 = drug_local2019.drop(drop_cols, axis=1)
drug_local2019.rename(columns = lambda x: x+'_2019' if re.search('nvisitor',x) else x, inplace=True)
#
drug_local = drug_local.merge(drug_local2019, on =['placekey','month'], how='outer', indicator=True)
assert drug_local[drug_local._merge!='both'].shape[0] == 0 
for d in [2,5,10,15,20]:
    drug_local[f'nvisitor_from_home_in_{d}_std1'] = drug_local[f'nvisitor_from_home_in_{d}']/drug_local[f'nvisitor_from_home_in_{d}_2019']
    drug_local.loc[~np.isfinite(drug_local[f'nvisitor_from_home_in_{d}_std1']), f'nvisitor_from_home_in_{d}_std1'] = np.nan
    drug_local[f'nvisitor_from_home_out_{d}_std1'] = drug_local[f'nvisitor_from_home_out_{d}']/drug_local[f'nvisitor_from_home_out_{d}_2019']
    drug_local.loc[~np.isfinite(drug_local[f'nvisitor_from_home_out_{d}_std1']), f'nvisitor_from_home_out_{d}_std1'] = np.nan
drop_cols = [x for x in drug_local.columns.tolist() if re.search('2019|merge|month',x)]
drug_local = drug_local.drop(drop_cols,axis=1)
#
drug_bal = drug_bal.merge(drug_local, on=['placekey','date_range_start'], how='left', indicator=True)
assert drug_bal[drug_bal._merge!='both'].shape[0] == 0
drug_bal = drug_bal.drop('_merge',axis=1)
drug_bal['date_range_start'] = drug_bal.date_range_start.dt.date.astype(str)
drug_bal['date_range_end'] = drug_bal.date_range_end.astype(str)

# Obtain 2020 Tract information
gis_cols = ['placekey', 'd_brand', 'location_name', 'street_address', 'city', 'region', 'zip', 'latitude', 'longitude']
assert drug_bal[gis_cols].drop_duplicates().shape[0] == drug_bal.placekey.drop_duplicates().shape[0]
drug_bal[gis_cols].drop_duplicates().to_csv('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drug_stores_final_gis.csv',index=False)
#
read_cols = ['placekey','Join_Count','GEOID']
drug_tract2020 = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/data/gis/advan_drug_stores_final_gis2020.xlsx', usecols = read_cols, dtype=str)
print('One store in Canada:',drug_tract2020[drug_tract2020.Join_Count!='1']) 
drug_tract2020 = drug_tract2020.drop('Join_Count',axis=1)
drug_tract2020 = drug_tract2020.rename(columns = {'GEOID':'tract2020'})
assert drug_tract2020[(drug_tract2020.tract2020.str.len()!=11) & (~drug_tract2020.tract2020.isnull())].shape[0] ==0 # One Canadian store with incorrect address attributed to US.
#
drug_bal = drug_bal.merge(drug_tract2020, on='placekey',how='outer', indicator=True)
assert drug_bal[drug_bal._merge!='both'].shape[0] ==0
drug_bal = drug_bal.drop('_merge',axis=1)
drug_bal.to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drug_stores_final.dta',write_index=False, version=118)
drug_bal.to_stata('D:/MD_Opportunity/data/advan/adv_drug_stores_final.dta',write_index=False, version=118)

# Read and write full drug panel that existed since Jan 2020.
drug_full = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drugstores_full.parquet')
# drug_full['closed'] = (~drug_full.closed_on.isnull()).astype(int)
drug_full = drug_full.merge(drug_local, on=['placekey','date_range_start'], how='left', indicator=True)
assert drug_full[drug_full._merge!='both'].shape[0] == 0
drug_full = drug_full.drop('_merge',axis=1)
drug_full['date_range_start'] = drug_full.date_range_start.dt.date.astype(str)
drug_full['date_range_end'] = drug_full.date_range_end.astype(str)
drug_full.columns = drug_full.columns.str.replace('normalized_','norm_')
drug_full.columns = drug_full.columns.str.replace('_region_naics_','_reg_nc_')
drug_full.to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/adv_drug_stores_full.dta',write_index=False, version=118)
drug_full.to_stata('D:/MD_Opportunity/data/advan/adv_drug_stores_full.dta',write_index=False, version=118)


#######################################################################################################
# Checks on drug store data
#######################################################################################################
small_pharm = ['Adams Drugs', 'Apple Discount Drugs', 'Aurora Pharmacy', 'Avita Pharmacy', 'Bartell Drugs', 'Best Value Pharmacies', 'Boone Drugs', 'Community Pharmacy', 'Discount Drug Mart', 'Drug Emporium West Virginia', 'Eaton Apothecary', 'Express Rx', 'Fruth Pharmacy', 'Good Day Pharmacy', 'GuidePoint Pharmacy', 'Hartig Drug', 'Hi-School Pharmacy', 'HomeTown Pharmacy', 'HomeTown Pharmacy RX', 'Jordan Drug', 'Kinney Drugs', "Klingensmith's", 'Lewis Drug', 'Maxor Pharmacy', "Moye's Pharmacy", 'Navarro', 'Northside Pharmacies', 'NuCara', 'Owens Healthcare', 'Pharmaca', 'Realo Discount Drug', 'Seip Drug', 'Shield Healthcare', 'Sterling Pharmacy', 'Super Health Pharmacy', 'Thrifyway Pharmacy', 'U Save Pharmacy', 'U-Save-It Pharmacy', 'William Bros Health Care', # In balanced filtered sample
'AllCare Pharmacy', 'Concord Rx', 'Family Pharmacy', "Fred's Pharmacy",  'Pharmabox' # Other non-grocery small pharmacies in full sample
]

drug_bal['d_brand'] = (drug_bal.brands.str.len()>0).astype(int)
drug_bal['d_top3'] = (drug_bal.brands.str.contains('CVS|WALGREENS|RITE AID',na=False, flags=re.IGNORECASE)).astype(int)
drug_bal['d_franchise'] = (drug_bal.brands.str.contains('HEALTH MART|MEDICINE SHOPPE|MEDICAP PHARMACY|PHARMACY EXPRESS',na=False, flags=re.IGNORECASE)).astype(int)
drug_bal['d_small'] = (drug_bal.brands.str.contains('|'.join(small_pharm), na=False, flags=re.IGNORECASE)).astype(int)
drug_bal['d_grocery'] = ((drug_bal.d_brand==1) & (drug_bal.d_top3==0) & (drug_bal.d_small==0) & (drug_bal.d_franchise==0)).astype(int)
assert drug_bal[drug_bal.d_grocery>0].shape[0] == 0
drug_bal = drug_bal.drop([''])

drug_bal.d_brand.value_counts()/drug_bal.shape[0]
drug_bal.d_top3.value_counts()/drug_bal.shape[0]
drug_bal.d_franchise.value_counts()/drug_bal.shape[0]
drug_bal.d_grocery.value_counts()/drug_bal.shape[0]
drug_bal.d_small.value_counts()/drug_bal.shape[0]



filter = full[(~full.placekey.isin(shared_polygon.placekey_primary)) & (~full.placekey.isin(shared_polygon.placekey_shared))]

filter.d_brand.value_counts()/filter.shape[0]
filter.d_top3.value_counts()/filter.shape[0]
filter.d_franchise.value_counts()/filter.shape[0]
filter.d_grocery.value_counts()/filter.shape[0]
filter.d_small.value_counts()/filter.shape[0]

full[full.d_small==1].brands.drop_duplicates().tolist()
full[full.brands=='Drug Emporium West Virginia'][['street_address','city','region','zip']]


