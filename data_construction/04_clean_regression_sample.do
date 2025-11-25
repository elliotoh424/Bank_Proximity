* ============================================================================
* REGRESSION DATASET PREPARATION: PHARMACY ANALYSIS
* ============================================================================
*
* DESCRIPTION: This do-file prepares the final regression dataset by cleaning, renaming, and transforming variables from the regression sample. The sample covers the same dataset as gen_drugstore_regression_sample.do. Only difference is variable transformation and renaming.


* INPUT: acs_drug_matched_month_radiusdef.dta (Master merged dataset)
* OUTPUT: acs_drug_matched_month_radiusdef_forreg.dta (Regression-ready dataset)
*
* KEY PROCESSING STEPS:
* 1. Variable renaming for consistency and brevity
* 2. Construction of spatial ring variables (0-500yd, 500-1000yd)
* 3. Creation of banking environment indicators
* 4. Generation of traffic change measures relative to March 2020
* 5. Data validation and consistency checks
* ============================================================================


cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", clear
unique placekey start
ren poi_cbg cbg
ren d_nonbrand d_nbrand

destring wkt_area_sq_meters, replace
ren wkt_area_sq_meters store_size

// g cty = substr(tract, 1, 5)
egen month_id = group(start)
egen cty_id = group(cty)
egen cty_ym_id = group(cty start)
egen zip_id = group(zip)
egen zip_ym_id = group(zip start)
egen tract_id = group(tract)
egen tract_ym_id = group(tract start)

xtset id_ad start

ren tract_aland tractsize
ren pdensity* pdsty*
ren pct_* pct* 

local controlvars "medage medinc pdsty pctuniv pctmnty wfh tractsize tot_pop"
local ctrl_dummy ""
foreach var of local controlvars{
	g l`var' = log(`var')
}
*

* Check that cb from bank_dist and cb from banksize are the same.
foreach yd of numlist 200 500 1000{
assert cb_`yd'yd == cb_branch_`yd'yd
}
drop cb_200yd* cb_500yd* cb_1000yd* ncb_200yd* ncb_500yd* ncb_1000yd*


* Modify name of bank branch variables.
ren *bank_branch_*yd *bank_*yd
ren *bank_branch_tract *bank_tract
ren *cb_branch_*yd *cb_*yd
ren *cb_branch_tract *cb_tract

* Check that microarea is inclusive
assert npoinm_xb_200yd <= npoinm_xb_500yd
assert npoinm_xb_500yd <= npoinm_xb_1000yd

ren *bank_branch_*yd_exc *bank*exc
ren *cb_branch_*yd_exc *cb*exc
local banksize "mega"
foreach bs of local banksize{
ren *`bs'_*yd_ring *`bs'*exc
}

* Construct ring variables: 0- 500 yards and 500-1000 yards
local vars "bank noncb cb"
foreach v of local vars{
	egen `v'500ring = rowmax(`v'100exc `v'200exc `v'300exc `v'400exc `v'500exc)
	g n`v'500ring = n`v'100exc + n`v'200exc + n`v'300exc + n`v'400exc + n`v'500exc

	egen `v'1000ring = rowmax(`v'600exc `v'700exc `v'800exc `v'900exc `v'1000exc)
	g n`v'1000ring = n`v'600exc + n`v'700exc + n`v'800exc + n`v'900exc + n`v'1000exc

	}

ren *bank_*yd *bank*
ren *cb_*yd *cb*
local banksize "mega"
foreach bs of local banksize{
ren *`bs'_*yd *`bs'*    
}
ren npoi_xb_*yd npoi*
ren brn_npoi_*yd brn_npoi*
ren ind_npoi_*yd ind_npoi*
ren rem_npoi_*yd rem_npoi*
ren npoi_pharmacy_*yd ndrug*
ren median_homevalue medhome

* Define dummy for stores with both community and non-community banks nearby
g both200 = (cb200) & (noncb200)
g both500 = (cb500) & (noncb500)
g both1000 = (cb1000) & (noncb1000)

* Define dummy for stores with multiple sized banks nearby
g mult200 = (mega200 & nonmega200) | (mega200 & cb200) | (nonmega200 & cb200)
g mult500 = (mega500 & nonmega500) | (mega500 & cb500) | (nonmega500 & cb500)
g mult1000 = (mega1000 & nonmega1000) | (mega1000 & cb1000) | (nonmega1000 & cb1000)

* Log variables
foreach var of varlist npoi* medhome{
g l`var' = ln(`var')        
}

* Change in microarea traffic relative to March 2020: all POIs, brand retailers, independent retailers, non-retailer POI
foreach yd of numlist 200 500 1000{
g dln_brn`yd' = ln(brn_avgnvisits_`yd'yd) - ln(brn_avgnvisits_`yd'yd_mar20)
g dln_ind`yd' = ln(ind_avgnvisits_`yd'yd) - ln(ind_avgnvisits_`yd'yd_mar20)
g dln_rem`yd' = ln(rem_avgnvisits_`yd'yd) - ln(rem_avgnvisits_`yd'yd_mar20)
}
drop yq 

g yq = qofd(start)
format yq %tq 

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", replace
