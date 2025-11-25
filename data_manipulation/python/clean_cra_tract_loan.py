import pandas as pd, re, os, numpy as np

# 2019-2021 uses 2010 census tract definition. Only 2022 uses 2020 tract definition.
# Generate variables from data
cra_panel = []
flat_panel = []
for y in range(2019,2023):
	# Small business loan at tract-level
	a = pd.read_csv(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/tract/raw/cra{y}_Aggr_A11.dat', sep="\"", header=None)
	a.columns = ['raw']
	a['tableID'] = a['raw'].str[:5]
	a = a[a.tableID == 'A1-1 ']
	a['Activity Year'] = a['raw'].str[5:9]
	a['Loan Type'] = a['raw'].str[9]
	a['Action Taken Type'] = a['raw'].str[10]
	a['State'] = a['raw'].str[11:13]
	a['County'] = a['raw'].str[13:16]
	a['MSA/MD'] = a['raw'].str[16:21]
	a['Census Tract'] = a['raw'].str[21:28]
	a['Split County Indicator'] = a['raw'].str[28]
	a['Population Classification'] = a['raw'].str[29]
	a['Income Group Total'] = a['raw'].str[30:33]
	a['Report Level'] = a['raw'].str[33:36]
	a['NoSBL100k'] = a['raw'].str[36:46]
	a['AmtSBL100k'] = a['raw'].str[46:56]
	a['NoSBL100_250k'] = a['raw'].str[56:66]
	a['AmtSBL100_250k'] = a['raw'].str[66:76]
	a['NoSBL250k_1mil'] = a['raw'].str[76:86]
	a['AmtSBL250k_1mil'] = a['raw'].str[86:96]
	a['NoSBL_rev_0_1mil'] = a['raw'].str[96:106]
	a['AmtSBL_rev_0_1mil'] = a['raw'].str[106:116]
	a['filler'] = a['raw'].str[116:145]
	#
	for c in a.columns.tolist():
		a[c] = a[c].str.strip()
	#
	assert a[a['tableID']!='A1-1'].shape[0] == 0
	assert a[a['Activity Year'] == y].shape[0] == 0
	assert a[a['Loan Type'] != '4'].shape[0] == 0
	assert a[a['Action Taken Type']!='1'].shape[0] == 0
	assert a[a['filler']!=''].shape[0] == 0
	drop_cols = ['tableID','Loan Type','Action Taken Type','filler']
	# print('extract passed')
	a = a.drop(drop_cols, axis=1)
	a['Census Tract'] = a['Census Tract'].str.replace('.','',regex=False)
	a['tract'] = a.State + a.County + a['Census Tract']
	num_cols = ['Activity Year', 'Income Group Total', 'Report Level', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']
	for c in num_cols:
		a[c] = pd.to_numeric(a[c])
	col_order = ['Activity Year', 'tract', 'Report Level', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil', 'State', 'County', 'MSA/MD', 'Census Tract', 'Split County Indicator', 'Population Classification', 'Income Group Total', 'raw']
	a = a[col_order]
	a['tractlen'] = a.tract.str.len()
	# a = a[~a.State.str.contains('^60$|^66$|^69$|^78$')] # Exclude all territories outside 50 states + DC + PR
	# Check whether census tract sums match MSA/MD totals, County totals, Income group totals. If not the same
	msa_tot = a[(a['Report Level']==210) & (a['MSA/MD']!='NA')][['MSA/MD','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']]
	state_tot = a[(a['Report Level']==210) & (a['MSA/MD']=='NA')][['State','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']]
	cty_tot = a[a['Report Level']==200][['State','County','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']]
	inc_tot = a[a['Report Level']==100][['State','County','Income Group Total','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']]
	#
	agg_cols = ['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']
	# At MSA level, tract sum is almost equal to MSA total. Difference is due to some loans that cannot be attributed to tracts.
	msa_cols = ['MSA/MD', 'tract'] + agg_cols
	msa_agg = a[(a['Report Level'].isnull()) & (a['MSA/MD']!='NA')][msa_cols]
	assert msa_agg[msa_agg.tract.str.len()!=11].shape[0] == 0
	msa_agg = msa_agg.groupby('MSA/MD')[agg_cols].sum().reset_index()
	msa_agg = msa_agg.merge(msa_tot, on=['MSA/MD'], how='outer', indicator=True)
	assert msa_agg[msa_agg._merge!='both'].shape[0] == 0
	msa_agg = msa_agg.drop('_merge',axis=1)
	for c in agg_cols:
		msa_agg[c+'_diff'] = (msa_agg[c+'_x']/msa_agg[c+'_y'] - 1).abs()
		print(c, y, msa_agg.shape[0], "# MSA where diff >= 5%", msa_agg[msa_agg[c+'_diff'] >= 0.05].shape[0], "# MSA where diff >= 10%", msa_agg[msa_agg[c+'_diff'] >= 0.1].shape[0])
	# At county level, tract sum is almost equal to county total. Less than 1% of counties have differences exceeding 10%. 
	cty_cols = ['State','County', 'tract'] + agg_cols
	cty_agg = a[(a['Report Level'].isnull())][cty_cols]
	assert cty_agg[cty_agg.tract.str.len()!=11].shape[0] == 0
	cty_agg = cty_agg.groupby(['State','County'])[agg_cols].sum().reset_index()
	cty_agg = cty_agg.merge(cty_tot, on=['State','County'], how='outer', indicator=True)
	cty_agg = cty_agg[(cty_agg.State!='60') & (cty_agg.State!='69')]
	assert cty_agg[cty_agg._merge!='both'].shape[0] == 0
	cty_agg = cty_agg.drop('_merge',axis=1)
	for c in agg_cols:
		cty_agg[c+'_diff'] = (cty_agg[c+'_x']/cty_agg[c+'_y'] - 1).abs()
		print(c, y, cty_agg.shape[0], "# cty where diff >= 5%", cty_agg[cty_agg[c+'_diff'] >= 0.05].shape[0], "# cty where diff >= 10%", cty_agg[cty_agg[c+'_diff'] >= 0.1].shape[0])
	# At county-income group level, tract sum is exactly equal. 
	inc_cols = ['State','County', 'Income Group Total', 'tract'] + agg_cols
	inc_agg = a[(a['Report Level'].isnull())][inc_cols]
	assert inc_agg[inc_agg.tract.str.len()!=11].shape[0] == 0
	inc_agg = inc_agg.groupby(['State','County','Income Group Total'])[agg_cols].sum().reset_index()
	inc_agg = inc_agg.merge(inc_tot, on=['State','County','Income Group Total'], how='outer', indicator=True)
	inc_agg = inc_agg[(inc_agg['Income Group Total']!=106) & (inc_agg['Income Group Total']!=15)]
	assert inc_agg[inc_agg._merge!='both'].shape[0] == 0
	inc_agg = inc_agg.drop('_merge',axis=1)
	for c in agg_cols:
		assert inc_agg[inc_agg[c+'_x']!=inc_agg[c+'_y']].shape[0] == 0
	#
	final_cols = ['Activity Year', 'tract', 'State', 'County', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil', 'Income Group Total']
	tract_cra = a[(a['Report Level'].isnull())][final_cols]
	assert tract_cra[(tract_cra['Income Group Total'].isnull())].shape[0] == 0
	assert tract_cra[tract_cra.tract.duplicated()].shape[0] == 0
	# No Unknown tract.
	assert tract_cra[tract_cra['Income Group Total'] == 15].shape[0] == 0
	assert tract_cra[tract_cra['Income Group Total'] == 106].shape[0] == 0
	tract_cra['lmi'] = ((tract_cra['Income Group Total'].between(1,8)) | (tract_cra['Income Group Total'].between(101,102)) ).astype(int) # 1-8: MFI 0-80%, 101-102: Below 50% and 50-80%
	for unknown_tract in [14, 105]: # replace with missing value for tracts with unknown income
		tract_cra.loc[tract_cra['Income Group Total']==unknown_tract, 'lmi'] = np.nan
	# #
	# if y != 2022:
	# 	flat = pd.read_csv(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/tract/flat/CensusFlatFile{y}.csv', header=None, dtype=str)
	# 	# MFI information (first columns), Tract indicators (second columns). Same format for 2019-2021
	# 	keep_cols = [x for x in range(14)] + [580] # + [x for x in range(1204,1212)] 
	# 	flat = flat[keep_cols]
	# else:
	# 	flat = pd.read_csv(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/tract/flat/CensusFlatFile{y}.csv', header=None, dtype=str)
	# 	# MFI information (first columns), Tract indicators (second columns). New format for 2022
	# 	keep_cols = [x for x in range(14)] + [585] # + [x for x in range(14,22)] 
	# 	flat = flat[keep_cols]
	# colnames = ['Activity Year',' msa_md','state','county','tract','d_principalcity','d_smallcounty','d_split_tract','d_demographic_data','urban_rural','mfi_msa','mhi_msa','pct_mfi','mfi_msa_ffiec'] + ['mfi'] #+ ['income_indicator','poverty_distressed','unemp_distressed','pop_distressed','remote_rural','prioryear_distressed','prioryear_underserved','prioryear_distressed_underserved']
	# flat.columns = colnames
	# flat['tract'] = flat.state + flat.county + flat.tract
	# assert flat[flat.tract.str.len()!=11].shape[0] == 0
	# for c in ['mfi_msa', 'mhi_msa', 'pct_mfi', 'mfi_msa_ffiec', 'mfi']:
	# 	flat[c] = pd.to_numeric(flat[c])
	# flat['pct_mfi_msa'] = flat.mfi/flat.mfi_msa
	# flat['pct_diff'] = (flat.pct_mfi_msa - flat.pct_mfi/100).abs()
	# assert flat[flat.pct_diff>0.01].shape[0] == 0
	# keep_cols = ['Activity Year','tract','mfi_msa','mfi','pct_mfi','mhi_msa']
	# flat = flat[keep_cols]
	# flat['Activity Year'] = pd.to_numeric(flat['Activity Year'])
	# # flat.to_excel(f'E:/pharmacy/cra/tract/processed/cra_tract_lmi{y}.xlsx', index=False)
	# # Merge with existing
	# tract_cra = tract_cra.merge(flat, on = ['Activity Year','tract'], how='left', indicator=True)
	# # print('tract-flat merged:')
	# assert tract_cra[tract_cra._merge!='both'].shape[0] == 0
	# # .value_counts())
	# tract_cra = tract_cra.drop('_merge',axis=1)
	#####################################################################
	# if y == 2022:
		# # Tract 2020 -> Tract 2010 crosswalk
		# tract_cols = ['tr2020ge', 'tr2010ge', 'wt_hh']
		# tract_cw = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/LODES_PROCESSED/nhgis_tr2010_tr2020.csv', dtype=str, usecols = tract_cols)
		# tract_cw.rename(columns = {'tr2020ge':'tract20','tr2010ge':'tract10','wt_hh':'household_weight'}, inplace=True)
		# tract_cw['household_weight'] = pd.to_numeric(tract_cw.household_weight)
		# # tract_cw = tract_cw[tract_cw.household_weight>0]
		# tract_cw['test'] = tract_cw.groupby(['tract10'])['household_weight'].transform('sum')
		# tract_cw['test'] = tract_cw.test.round(decimals=6)
		# assert tract_cw[tract_cw.test != 1].shape[0] == 0
		# tract_cw = tract_cw.drop('test',axis=1)
		# #
		# tract_cra = tract_cra.merge(tract_cw, left_on = ['tract'], right_on = ['tract20'], how='left', indicator=True)
		# tract_cra = tract_cra[~tract_cra.tract.str.contains('^78|^60|^66|^69')] # Exclude any US territories
		# assert tract_cra[tract_cra._merge!='both'].shape[0] == 0
		# tract_cra = tract_cra.drop(['_merge','tract'],axis=1)
		# for c in ['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']:
		# 	tract_cra[c] = tract_cra[c] * tract_cra.household_weight
		# tract_cra.rename(columns = {'tract10':'tract'}, inplace=True)
		# tract_cra = tract_cra[tract_cra.household_weight>0]
		# tract_cra = tract_cra.groupby(['Activity Year', 'tract'])[['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']].sum().reset_index()
		# tract_cra['State'] = tract_cra.tract.str[:2]
		# tract_cra['County'] = tract_cra.tract.str[2:5]
		# tract_cra = tract_cra[final_cols]
	print(y, tract_cra.shape[0])
	tract_cra.to_excel(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/tract/processed/cra_tract{y}.xlsx', index=False)
	cra_panel.append(tract_cra)
	# flat_panel.append(flat)

# Upto 2019, Loans below 100k county total differs from tract sum by at least 10% in 278/3223 counties.  
# Check how well-behaved the difference is.
cra_panel = pd.concat(cra_panel, axis=0)
for c in ['AmtSBL100k', 'AmtSBL100_250k', 'AmtSBL250k_1mil', 'AmtSBL_rev_0_1mil']:
	cra_panel[c] = cra_panel[c] / 1000 # Convert to units of mil dollars
#
cra_panel['NoSBL'] = cra_panel.NoSBL100k + cra_panel.NoSBL100_250k + cra_panel.NoSBL250k_1mil
cra_panel['NoSBL250k'] = cra_panel.NoSBL100k + cra_panel.NoSBL100_250k
cra_panel['AmtSBL'] = cra_panel.AmtSBL100k + cra_panel.AmtSBL100_250k + cra_panel.AmtSBL250k_1mil
cra_panel['AmtSBL250k'] = cra_panel.AmtSBL100k + cra_panel.AmtSBL100_250k
cra_panel['AvgSBL'] = cra_panel.AmtSBL/cra_panel.NoSBL
cra_panel = cra_panel[~cra_panel.State.str.contains('^60$|^66$|^69$|^72$|^78$')]
assert cra_panel[cra_panel.AvgSBL.isnull()].shape[0] == cra_panel[(cra_panel.NoSBL==0) & (cra_panel.AmtSBL==0)].shape[0]
cra_panel.loc[cra_panel.AvgSBL.isnull(),'AvgSBL'] = 0 # Missing value is because of no SBL loans
cra_panel.rename(columns = {'Activity Year':'year'}, inplace=True)
assert cra_panel[cra_panel[['tract','year']].duplicated()].shape[0] == 0
cra_panel.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract_2019_2022.parquet', index=False, compression='zstd')

cra_panel = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract_2008_2022.parquet')


# Flat panel with LMI informatio
flat_panel = pd.concat(flat_panel, axis=0)
flat_panel.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_lmi_tract_2019_2022.parquet', index=False, compression='zstd')

# Counties with independent pharmacies
drug_vars = ['placekey','d_brand','tract','latitude','longitude','zip']
drugstore = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_stores_final.dta", columns = drug_vars).drop_duplicates()
assert drugstore[drugstore.tract.str.len()!=11].shape[0] == 0
drugstore = drugstore[drugstore.d_brand==0]
drugstore['fips'] = drugstore.tract.str[:5]
drugstore = drugstore.drop('tract',axis=1)
for c in ['latitude','longitude']:
    drugstore[c] = pd.to_numeric(drugstore[c])
#
drugcty = drugstore.fips.drop_duplicates()
#
# Variables of interest
pd.options.display.float_format = '{:.3f}'.format
cra_panel = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract_2019_2022.parquet')
assert cra_panel[cra_panel.tract.str.len()!=11].shape[0] == 0
final_cols = ['year', 'tract', 'lmi', 'Income Group Total', 'NoSBL100k', 'AmtSBL100k', 'NoSBL250k', 'AmtSBL250k', 'NoSBL', 'AmtSBL', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil', 'AvgSBL', 'mfi_msa', 'mfi', 'pct_mfi', 'mhi_msa']
cra_panel = cra_panel[final_cols]
cra_panel['fips'] = cra_panel.tract.str[:5]
# 
cra_panel19 = cra_panel[cra_panel.year==2019]
cra_panel22 = cra_panel[cra_panel.year==2022]
keep_cols = ['tract', 'fips', 'lmi', 'mfi_msa', 'mfi', 'pct_mfi', 'mhi_msa', 'Income Group Total', 'NoSBL100k', 'AmtSBL100k', 'NoSBL250k', 'AmtSBL250k', 'NoSBL', 'AmtSBL', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']
cra_panel19 = cra_panel19[keep_cols]
cra_panel22 = cra_panel22[keep_cols]
cra_panel22.rename(columns = {'tract':'tract2020'} , inplace=True)
cra_panel19.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract19.dta', write_index=False, version=118)
cra_panel22.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract22.dta', write_index=False, version=118)

# Restrict to counties with independent pharmacies
cra_panel19 = cra_panel19[cra_panel19.fips.isin(drugcty)]
cra_panel22 = cra_panel22[cra_panel22.fips.isin(drugcty)]

# Combine with SOD2019 and ZBP
relevant_cols = ['namebr', 'addresbr', 'citybr', 'stalpbr', 'zipbr', 'brnum', 'rssdid','GEOID10']
sod = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/sod2019_tract10_new.xlsx', usecols = relevant_cols, dtype=str)
sod.rename(columns = {'GEOID10':'tract', 'zipbr':'zip'}, inplace=True)
assert sod[sod.brnum.isnull()].shape[0] == 0
sod_vars = ['RSSDID','BRNUM','DEPSUMBR','STALPBR','ASSET']
sod1 = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/SOD_CW/sod_data/sod_2019.csv', usecols=sod_vars) # Bank branch data as of June 2019
sod1.columns = sod1.columns.str.lower()
sod1['rssdid'] = sod1['rssdid'].astype(str)
sod1['brnum'] = sod1['brnum'].astype(str)
sod1 = sod1[~sod1.stalpbr.str.contains('^PR$|^VI$|^AS$|^FM$|^GU$|^MH$|^MP$|^PW$',na=False)]
sod = sod.merge(sod1, on=['rssdid','brnum','stalpbr'], how='outer', indicator=True)
assert sod[sod._merge!='both'].shape[0] == 0
sod = sod.drop('_merge',axis=1)
sod = sod.groupby(['tract']).agg({'brnum':'count', 'depsumbr':'sum'}).reset_index()
sod.columns = ['tract','trnbranch','trdeposit']
sod['fips'] = sod.tract.str[:5]
sod = sod[sod.fips.isin(drugcty)]
#
# Merge with CRA 2019 (Tracts with branches only)
sodcra19 = sod.merge(cra_panel19, on =['tract','fips'], how='left', indicator=True)
print(sodcra19._merge.value_counts()) 
for c in ['NoSBL100k', 'AmtSBL100k', 'NoSBL250k', 'AmtSBL250k', 'NoSBL', 'AmtSBL', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']:
	sodcra19.loc[sodcra19._merge=='left_only',c] = 0
	assert sodcra19[sodcra19[c].isnull()].shape[0] == 0
sodcra19[['NoSBL100k', 'AmtSBL100k', 'NoSBL250k', 'AmtSBL250k', 'NoSBL', 'AmtSBL', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']].describe()
sodcra19 = sodcra19.drop('_merge',axis=1)
final_cols = ['tract', 'trnbranch', 'trdeposit', 'fips', 'lmi', 'Income Group Total', 'NoSBL100k', 'AmtSBL100k', 'NoSBL250k', 'AmtSBL250k', 'NoSBL', 'AmtSBL', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']
sodcra19 = sodcra19[final_cols]
sodcra19.columns = sodcra19.columns.str.replace(' ','')
sodcra19.columns = [ col + '_19' if not re.search('tract|trnbranch|trdeposit|fips',col) else col for col in sodcra19.columns]
sodcra19.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/sod_cra_tract19.dta', write_index=False, version=118)

# Obtain stores with LMI information from 2019 and 2022.
flat_panel = pd.read_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_lmi_tract_2019_2022.parquet')
flat_panel19 = flat_panel[flat_panel['Activity Year']==2019]
flat_panel19['lmi19'] = ((flat_panel19.pct_mfi < 80) & (~flat_panel19.pct_mfi.isnull())).astype(int)
flat_panel19.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_lmi_tract19.dta', write_index=False, version=118)

flat_panel22 = flat_panel[flat_panel['Activity Year']==2022]
flat_panel22.rename(columns = {'tract':'tract2020'}, inplace=True)
flat_panel22['lmi22'] = ((flat_panel22.pct_mfi < 80) & (~flat_panel22.pct_mfi.isnull())).astype(int)

cra_lmitract19 = cra_panel19[(cra_panel19.lmi==1)].tract
cra_lmitract22 = cra_panel22[(cra_panel22.lmi==1)].tract2020
cra_nonlmitract19 = cra_panel19[(cra_panel19.lmi==0)].tract
cra_nonlmitract22 = cra_panel22[(cra_panel22.lmi==0)].tract2020

# Check that LMI definition in cra_panel is identical to flat_panel definition.
assert flat_panel19[(flat_panel19.tract.isin(cra_lmitract19)) & (flat_panel19.pct_mfi>=80) & (~flat_panel19.pct_mfi.isnull())].shape[0] == 0
assert flat_panel22[(flat_panel22.tract2020.isin(cra_lmitract22)) & (flat_panel22.pct_mfi>=80) & (~flat_panel19.pct_mfi.isnull())].shape[0] == 0
assert flat_panel19[(flat_panel19.tract.isin(cra_nonlmitract19)) & (flat_panel19.pct_mfi<80) & (~flat_panel19.pct_mfi.isnull())].shape[0] == 0
assert flat_panel22[(flat_panel22.tract2020.isin(cra_nonlmitract22)) & (flat_panel22.pct_mfi<80) & (~flat_panel19.pct_mfi.isnull())].shape[0] == 0
#
drug_vars = ['placekey','d_brand','tract','tract2020']
drugstore = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_stores_final.dta", columns = drug_vars).drop_duplicates()
assert drugstore[drugstore.tract.str.len()!=11].shape[0] == 0
assert drugstore[(drugstore.tract2020.str.len()!=11)].shape[0] == 1
drugstore = drugstore.merge(flat_panel19, on='tract', how='left', indicator=True)
assert drugstore[drugstore._merge!='both'].shape[0] == 1 # One mismatch due to incorrect tract (starts with 00)
drugstore = drugstore.drop(['_merge','Activity Year'],axis=1)
for c in ['mfi_msa','mfi','pct_mfi','mhi_msa']:
	assert drugstore[drugstore[c].isnull()].shape[0] == 1
	drugstore.rename(columns = {c:c+'19'}, inplace=True)
#
drugstore = drugstore.merge(flat_panel22, on='tract2020', how='left', indicator=True)
assert drugstore[drugstore._merge!='both'].shape[0] == 1 # One mismatch due to incorrect tract (starts with 00)
drugstore = drugstore.drop(['_merge','Activity Year'],axis=1)
for c in ['mfi_msa','mfi','pct_mfi','mhi_msa']:
	assert drugstore[drugstore[c].isnull()].shape[0] == 1
	drugstore.rename(columns = {c:c+'22'}, inplace=True)
drugstore['lmi19'] = ((drugstore.pct_mfi19 < 80) & (~drugstore.pct_mfi19.isnull())).astype(int)
drugstore['lmi22'] = ((drugstore.pct_mfi22 < 80) & (~drugstore.pct_mfi22.isnull())).astype(int)
#
drugstore['tract_chg'] = (drugstore.tract != drugstore.tract2020).astype(int)
drugstore['chg_lmi'] = ((drugstore.lmi19 == 0) & (drugstore.lmi22 == 1) & (~drugstore.lmi19.isnull())).astype(int)
drugstore['chg_Nlmi'] = ((drugstore.lmi19 == 1) & (drugstore.lmi22 == 0) & (~drugstore.lmi19.isnull())).astype(int)
drugstore['stay_lmi'] = ((drugstore.lmi19 == 1) & (drugstore.lmi19 == drugstore.lmi22) & (~drugstore.lmi19.isnull())).astype(int)
drugstore['stay_Nlmi'] = ((drugstore.lmi19 == 0) & (drugstore.lmi19 == drugstore.lmi22) & (~drugstore.lmi19.isnull())).astype(int)
drugstore['same_lmi'] = ((drugstore.lmi19 == drugstore.lmi22) & (~drugstore.lmi19.isnull())).astype(int)
keep_cols = ['placekey', 'pct_mfi19', 'pct_mfi22', 'tract_chg', 'lmi19', 'lmi22', 'chg_lmi', 'chg_Nlmi', 'stay_lmi','stay_Nlmi','same_lmi']
drugstore = drugstore[keep_cols]
#
for c in ['tract_chg', 'lmi19', 'lmi22', 'chg_lmi', 'chg_Nlmi', 'stay_lmi','stay_Nlmi','same_lmi']:
	drugstore.loc[drugstore.placekey=='zzy-222@665-tsf-vj9',c] = np.nan
drugstore.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/adv_drug_lmi_tract_chg1922.dta', write_index=False, version=118)




t = sod[(sod.tract.isin(check))]
t = t[~t.rssdid.isin(crayes)].tract.drop_duplicates()

cracty[cracty.ID_RSSD.isin(t.rssdid)].ID_RSSD.drop_duplicates().shape
crayes = cracty[cracty.ID_RSSD.isin(t.rssdid)].ID_RSSD.drop_duplicates()

# sodcra3yr = sod.merge(cra_panel3yr, on ='tract', how='outer', indicator=True)
# print(sodcra3yr._merge.value_counts()) # large number of tracts with CRA loans but no branch. 
# sodcra3yr.loc[sodcra3yr._merge=='right_only','trnbranch'] = 0
# for c in ['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']:
# 	sodcra3yr.loc[sodcra3yr._merge=='left_only',c] = 0
# 	assert sodcra3yr[sodcra3yr[c].isnull()].shape[0] == 0
# sodcra3yr[['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']].describe()
# # Tracts with branches lend more. More loans and larger loans.
# sodcra3yr[sodcra3yr._merge=='right_only'][['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']].describe()
# sodcra3yr[sodcra3yr._merge=='both'][['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']].describe()
# sodcra3yr = sodcra3yr.drop('_merge',axis=1)
# # sod-cra in retailer store sample only
# sodcra3yr = sodcra3yr.merge(retailerstore, on ='tract', how='outer',indicator=True)
# sodcra3yr._merge.value_counts()
# sodcra3yr = sodcra3yr[sodcra3yr._merge!='left_only'] # Very few tracts only in SOD-CRA but not in retailer file.
# # Right only: only in retailer store sample but not in CRA nor in SOD
# for c in ['trnbranch', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']:
#     sodcra3yr.loc[sodcra3yr._merge=='right_only',c] = 0
# sodcra3yr = sodcra3yr.drop('_merge',axis=1)
# #
# sodcra3yr = sodcra3yr.merge(zbp, on ='zip', how='left', indicator=True)
# sodcra3yr = sodcra3yr[~sodcra3yr.tract.str.contains('^7[28]|^6[609]|^00')]
# sodcra3yr._merge.value_counts() # Don't fill in zeros for zips without ZBP data.  
# sodcra3yr = sodcra3yr.drop('_merge',axis=1)
# assert sodcra3yr[sodcra3yr[['tract','zip']].duplicated()].shape[0] == 0
# final_cols = ['tract', 'zip','trnbranch', 'emp0_4', 'emp0_9', 'emp0_19', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']
# sodcra3yr = sodcra3yr[final_cols]
# sodcra3yr.columns = [ col + '_3yr' if not re.search('tract|zip|trnbranch|emp',col) else col for col in sodcra3yr.columns]
# sodcra3yr.to_stata('E:/pharmacy/cra/sod_cra_tract3yr.dta', write_index=False, version=118)
# sodcra3yr.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/sod_cra_tract3yr.dta', write_index=False, version=118)



# # # Cumulative change since 2019
# # cra2019 = cra_panel[cra_panel['Activity Year'] == 2019]
# # keep_cols = ['tract', 'NoSBL', 'AmtSBL', 'AvgSBL', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k',
# #        'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']
# # cra2019 = cra2019[keep_cols]
# # cra2019.columns = [col + '_19' if col != 'tract' else col for col in cra2019.columns]
# # cra_panel = cra_panel.merge(cra2019, on =['tract'], how='left', indicator= True)
# # assert cra_panel[(cra_panel._merge!='both') & (cra_panel.balanced==1)].shape[0] == 0
# # cra_panel = cra_panel[cra_panel._merge=='both'].drop('_merge',axis=1) # unmatched are tracts with all years of data
# # for c in ['NoSBL', 'AmtSBL', 'AvgSBL', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil']:
# #     cra_panel[f'dln_{c}'] = np.log(cra_panel[f'{c}']) - np.log(cra_panel[f'{c}_19'])
# #     cra_panel.loc[~np.isfinite(cra_panel[f'dln_{c}']), f'dln_{c}'] = np.nan 

# # Differences seem not too crazy
# cra_panel[cra_panel['Activity Year']==2020][[
# 	'dln_NoSBL','dln_AmtSBL', 'dln_AvgSBL', 
# 	'dln_NoSBL100k', 'dln_AmtSBL100k','dln_NoSBL100_250k', 'dln_AmtSBL100_250k', 'dln_NoSBL250k_1mil','dln_AmtSBL250k_1mil', 'dln_NoSBL_rev_0_1mil', 'dln_AmtSBL_rev_0_1mil',
# 	'NoSBL', 'AmtSBL', 'AvgSBL','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil'
# 	]].describe().to_csv('/Users/elliotoh/Downloads/test.csv', mode='w')
# cra_panel[cra_panel['Activity Year']==2021][[
# 	'dln_NoSBL','dln_AmtSBL', 'dln_AvgSBL', 
# 	'dln_NoSBL100k', 'dln_AmtSBL100k','dln_NoSBL100_250k', 'dln_AmtSBL100_250k', 'dln_NoSBL250k_1mil','dln_AmtSBL250k_1mil', 'dln_NoSBL_rev_0_1mil', 'dln_AmtSBL_rev_0_1mil',
# 	'NoSBL', 'AmtSBL', 'AvgSBL','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil'
# 	]].describe().to_csv('/Users/elliotoh/Downloads/test.csv', mode='a')
# cra_panel[cra_panel['Activity Year']==2022][[
# 	'dln_NoSBL','dln_AmtSBL', 'dln_AvgSBL', 
# 	'dln_NoSBL100k', 'dln_AmtSBL100k','dln_NoSBL100_250k', 'dln_AmtSBL100_250k', 'dln_NoSBL250k_1mil','dln_AmtSBL250k_1mil', 'dln_NoSBL_rev_0_1mil', 'dln_AmtSBL_rev_0_1mil',
# 	'NoSBL', 'AmtSBL', 'AvgSBL','NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL_rev_0_1mil', 'AmtSBL_rev_0_1mil'
# 	]].describe().to_csv('/Users/elliotoh/Downloads/test.csv', mode='a')
# drop_cols = [x for x in cra_panel.columns if re.search('_19',x)]
# cra_panel = cra_panel.drop(drop_cols, axis=1)
# cra_panel.to_stata('E:/pharmacy/cra/cra_1922_final.dta', write_index=False, version=118)
#
# cra_panel3yr = cra_panel[cra_panel.year.between(2017,2019)]
# cra_panel3yr = cra_panel3yr.groupby('tract')[['NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']].mean().reset_index()


# zbp = pd.read_csv('C:/Users/elliotoh/Box/Chenyang-Elliot/ZBP_CW/zbp_detail/zbp19detail.txt',sep = ',', encoding='latin-1', dtype=str)
# zbp = zbp[zbp.naics.str.contains('^-+$')]
# assert zbp[zbp.zip.str.len()!=5].shape[0] == 0
# for c in ['n<5','n5_9','n10_19']:
#     zbp[c] = zbp[c].str.replace('N','')
#     zbp[c] = pd.to_numeric(zbp[c])
# zbp['emp0_4'] = zbp['n<5']
# zbp['emp0_9'] = zbp['n<5'] + zbp['n5_9']
# zbp['emp0_19'] = zbp['n<5'] + zbp['n5_9'] + zbp['n10_19']
# zbp_cols = ['zip','emp0_4','emp0_9','emp0_19']
# zbp = zbp[zbp_cols]

# # Merge with CRA 3 year rolling. Restrict to areas where retailers are located in.
# retailer_vars = ['tract','zip']
# retailerstore = pd.read_parquet('F:/MD_Opportunity/data/advan/adv_retailer_full.parquet', filters = [('geometry_type','!=','POINT')], columns = retailer_vars).drop_duplicates()
# retailerstore = retailerstore[~retailerstore.zip.isnull()]
# retailerstore['zip'] = retailerstore.zip.str.zfill(5)
# assert retailerstore[retailerstore.zip.str.len()!=5].shape[0] == 0
# assert retailerstore[retailerstore.tract.str.len()!=11].shape[0] == 0

# sodcra19 = sodcra19.merge(retailerstore, on ='tract', how='outer',indicator=True)
# sodcra19._merge.value_counts()
# sodcra19 = sodcra19[sodcra19._merge!='left_only'] # Very few tracts only in SOD-CRA but not in retailer file.
# # Right only: only in retailer store sample but not in CRA nor in SOD
# for c in ['trnbranch', 'NoSBL100k', 'AmtSBL100k', 'NoSBL100_250k', 'AmtSBL100_250k', 'NoSBL250k_1mil', 'AmtSBL250k_1mil', 'NoSBL', 'NoSBL250k', 'AmtSBL', 'AmtSBL250k']:
#     sodcra19.loc[sodcra19._merge=='right_only',c] = 0
# sodcra19 = sodcra19.drop('_merge',axis=1)
# #
# sodcra19 = sodcra19.merge(zbp, on ='zip', how='left', indicator=True)
# sodcra19._merge.value_counts()
# sodcra19 = sodcra19.drop('_merge',axis=1)
# assert sodcra19[sodcra19[['tract','zip']].duplicated()].shape[0] == 0
