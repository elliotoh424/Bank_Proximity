*
use placekey d_nbrand tract using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
duplicates drop
unique placekey

preserve
keep tract
duplicates drop
g pharmacy = 1
tempfile all_pharm
save `all_pharm', replace
restore

preserve
keep if d_nbrand == 1
keep tract
duplicates drop
g ind_pharmacy = 1
tempfile ind_pharm
save `ind_pharm', replace
restore

* ACS 5 year data (From acs_5yr_process.py in code/upstairs/final/)
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/final/acs_5yr_2019_final.dta", clear
ren pop_density pdensity
ren pct_collegeup pct_univ
ren pct_minority pct_mnty
ren pct_age_* pct_*
ren pct_inc_100k* pct_100k*
ren pct_inc_200k* pct_200k*
// ren wfh_ind* wfh*
ren pct_unemployed pct_unemp
ren geoid tract
g strlen = strlen(tract)
tab strlen
assert strlen == 11
drop strlen
tempfile acs
save `acs', replace

* Microarea # POI, # out-of-sample drug stores as of Jan 2020. Microarea shock is sum so missing value doesn't matter. Average traffic will be calculated later 
* From gen_drug_nearby_businesses_radius_stores.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_full_tract.dta", clear
keep if year == 2020
keep if month == 1

keep tract npoi_tract npoi_pharmacy_tract
unique tract
tempfile tract_npoi
save `tract_npoi', replace

* Change in microarea traffic between 2019 and 2020 (April) 
* From gen_drug_nearby_businesses_radius_stores.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_full_tract.dta", clear
keep if year <= 2020
keep if month == 4

assert allpoi_totnvisits_tract !=.
assert allpoi_totnvisits_tract !=. & brn_totnvisits_tract != . & ind_totnvisits_tract != . & rem_totnvisits_tract != .

* Prior to exclude traffic from microarea bank fill in missing values. Missing value --> 0 
replace finance_totnvisits_tract = 0 if finance_totnvisits_tract ==.
replace finance_xb_totnvisits_tract = 0 if finance_xb_totnvisits_tract ==.
g bank_totnvisits_tract = finance_totnvisits_tract - finance_xb_totnvisits_tract
* Calculate change in tract and microarea traffic excluding microarea bank traffic
g allpoi_totnvisits_tract_exc = allpoi_totnvisits_tract -  bank_totnvisits_tract 
keep tract allpoi_totnvisits_tract_exc year

preserve
keep if year == 2019
drop year
ren * *19
ren tract19 tract
tempfile chg19
save `chg19', replace
restore

merge m:1 tract using `chg19', assert(3) nogen
g all_tract_chg_exc4 = log(allpoi_totnvisits_tract_exc) - log(allpoi_totnvisits_tract_exc19)

keep if year == 2020
keep tract *chg*
unique tract
tempfile chgapr
save `chgapr', replace

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract", clear

merge m:1 tract using `acs'
unique tract if _merge == 3
unique tract if _merge == 1
unique tract if _merge == 2
keep if _merge == 3
drop _merge

merge m:1 tract using `tract_npoi'
unique tract if _merge == 1
drop if _merge == 2
drop _merge

merge m:1 tract using `chgapr'
unique tract if _merge == 1
drop if _merge == 2
drop _merge

ren npoi_pharmacy_tract ndrug_tract
g lnpoi_tract = ln(npoi_tract)

foreach var of varlist medage medinc pdensity median_homevalue{
	g l`var' = ln(`var')
}

ren lpdensity lpdsty
ren lmedian_homevalue lmedhome

unique tract
unique tract if pct_mega == 1
unique tract if pct_mega == 0
su pct_exam, d

merge m:1 tract using `all_pharm', assert(1 3) nogen
merge m:1 tract using `ind_pharm', assert(1 3) nogen

g cty = substr(tract, 1,5)
egen tract_id = group(tract)
egen cty_yr_id = group(cty year)
ren *_rev_0_1mil *r

g mega = (nmega >= 1 & nmega != .)
g nonmega = (nnonmega >= 1 & nnonmega != .)
g cb = (ncb >= 1 & ncb != .)
g bank = (nbank >= 1 & nbank != .)

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged.dta", replace 