**********************************************************************************
* DESCRIPTION: This do-file constructs a comprehensive panel dataset by merging multiple data sources to generate an analysis sample used for regresion analysis.

* DATA SOURCES:
* 1. CRA 2019 - Small business lending data at tract level
* 2. ACS 2019 5-Year - Demographic and socioeconomic data
* 3. Advan - Pharmacy foot traffic and POI data
* 4. Bank Branch Data - Location and size information
* 5. Derived Microarea Metrics - Business density and traffic patterns
*
* KEY VARIABLE CATEGORIES:
* - Pharmacy characteristics (brand/independent, traffic patterns)
* - Microarea business environment around store (200yd/500yd/1000yd radii)
* - Branch proximity
* - CRA small business lending metrics
* - ACS demographic and economic indicators
* - COVID-19 period traffic shocks
*
* GEOGRAPHIC UNITS:
* - Store level: placekey
* - Tract level: tract
* - Microareas: 200yd, 500yd, 1000yd buffers
*
* OUTPUT: acs_drug_matched_month_radiusdef.dta
**********************************************************************************

* CRA 2019 tract data for all tracts with pharmacies 
* From clean_cra_tract_loan.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract19.dta", clear
unique tract
keep tract lmi pct_mfi Income_Group_Total NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBL_rev_0_1mil AmtSBL_rev_0_1mil
ren Income_Group_Total income_group
ren * *_all19
ren tract_all19 tract
tempfile tract_cra
save `tract_cra', replace

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

* Tract-level brand presence based on full drug store data in Jan 2020
* From gen_advan_drugfiltered_panel.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_full.dta", clear
g start3 = date(date_range_start, "YMD")
format start3 %td
ren *3 *
keep if start == date("2020-01-01","YMD")
unique placekey
bysort tract: egen nbrandtr_jan2020 = sum(d_brand) 
bysort tract: egen ndrugtr_jan2020 = count(placekey)
g nnbrandtr_jan2020 = ndrugtr_jan2020 - nbrandtr_jan2020
g pct_brandtr_jan2020 = nbrand/ndrug
keep tract ndrug* n*brand* pct_*
duplicates drop
unique tract
tempfile brand_count1
save `brand_count1', replace

* Calculate microarea variables (200, 500, 1000 yards): # POI (excluding banks), # banks, # POI (with non-missing foot traffic data excluding banks) as of Jan 2020.
* From gen_drug_nearby_businesses_radius_stores.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear
keep if year == 2020
keep if month == 1

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

* # POI: Remove focal store and bank branches from # POI.
* Create # POI for all POIs and POIs with non-missing traffic data (needed for average calculation later)
foreach yd of numlist 200 500 1000{
    replace npoi_`yd'yd = npoi_`yd'yd - 1 
    assert npoi_`yd'yd != . & npoi_`yd'yd >=0
	
    replace npoi_finance_`yd'yd = 0 if npoi_finance_`yd'yd ==.
    replace npoi_finance_xb_`yd'yd = 0 if npoi_finance_xb_`yd'yd ==.
    g nbank_`yd'yd = npoi_finance_`yd'yd - npoi_finance_xb_`yd'yd 
    g npoi_xb_`yd'yd = npoi_`yd'yd - nbank_`yd'yd
    assert npoi_xb_`yd'yd != . & npoi_xb_`yd'yd >=0

    replace allpoi_npoinm_`yd'yd = allpoi_npoinm_`yd'yd - 1 if nvisits != .
    assert allpoi_npoinm_`yd'yd != . & allpoi_npoinm_`yd'yd >=0
    replace finance_npoinm_`yd'yd = 0 if finance_npoinm_`yd'yd ==.
    replace finance_xb_npoinm_`yd'yd = 0 if finance_xb_npoinm_`yd'yd ==.
    g npoinm_bank_`yd'yd = finance_npoinm_`yd'yd - finance_xb_npoinm_`yd'yd 
    g npoinm_xb_`yd'yd = allpoi_npoinm_`yd'yd - npoinm_bank_`yd'yd
    assert npoinm_xb_`yd'yd != . & npoinm_xb_`yd'yd >=0
}

ren allpoi_npoinm_*yd npoinm_*yd

keep placekey npoi_xb_200yd nbank_200yd npoinm_xb_200yd npoinm_bank_200yd ///
     npoi_xb_500yd nbank_500yd npoinm_xb_500yd npoinm_bank_500yd ///
     npoi_xb_1000yd nbank_1000yd npoinm_xb_1000yd npoinm_bank_1000yd 
unique placekey
tempfile micro_jan20
save `micro_jan20', replace 

* Calculate microarea variables (200, 500, 1000 yards): # brand retail POI, # independent retail POI, # non-retail POI, # brand retail POI (with non-missing foot traffic data), # independent retail POI (with non-missing foot traffic data), # non-retail POI (with non-missing foot traffic data) as of Jan 2020.
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear
keep if year == 2020
keep if month == 1

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

foreach yd of numlist 200 500 1000{
    replace brandnpoi_`yd'yd = brandnpoi_`yd'yd - 1 if d_brand
    replace nonbrandnpoi_`yd'yd = nonbrandnpoi_`yd'yd - 1 if ~d_brand
    
    assert brandnpoi_`yd'yd != . & brandnpoi_`yd'yd >=0
    assert nonbrandnpoi_`yd'yd != . & nonbrandnpoi_`yd'yd >=0
    assert remainnpoi_`yd'yd != . & remainnpoi_`yd'yd >=0

    g brn_npoi_`yd'yd = brandnpoi_`yd'yd
    g ind_npoi_`yd'yd = nonbrandnpoi_`yd'yd

    assert npoi_`yd'yd != . & npoi_`yd'yd >=0
    replace npoi_finance_`yd'yd = 0 if npoi_finance_`yd'yd ==.
    replace npoi_finance_xb_`yd'yd = 0 if npoi_finance_xb_`yd'yd ==.
    g nbank_`yd'yd = npoi_finance_`yd'yd - npoi_finance_xb_`yd'yd 
    assert nbank_`yd'yd != . & nbank_`yd'yd >= 0
    g rem_npoi_`yd'yd = remainnpoi_`yd'yd  - nbank_`yd'yd
    assert rem_npoi_`yd'yd != . & rem_npoi_`yd'yd >= 0

    replace brn_npoinm_`yd'yd = brn_npoinm_`yd'yd - 1 if d_brand
    replace ind_npoinm_`yd'yd = ind_npoinm_`yd'yd - 1 if ~d_brand
    replace allpoi_npoinm_`yd'yd = allpoi_npoinm_`yd'yd - 1 if nvisits != .

    assert brn_npoinm_`yd'yd != . & brn_npoinm_`yd'yd >=0
    assert ind_npoinm_`yd'yd != . & ind_npoinm_`yd'yd >=0
    assert rem_npoinm_`yd'yd != . & rem_npoinm_`yd'yd >=0

    replace finance_npoinm_`yd'yd = 0 if finance_npoinm_`yd'yd ==.
    replace finance_xb_npoinm_`yd'yd = 0 if finance_xb_npoinm_`yd'yd ==.
    g npoinm_bank_`yd'yd = finance_npoinm_`yd'yd - finance_xb_npoinm_`yd'yd 
    assert npoinm_bank_`yd'yd != . & npoinm_bank_`yd'yd >=0
    g rem_npoinm_`yd'yd_new = rem_npoinm_`yd'yd - npoinm_bank_`yd'yd
    assert rem_npoinm_`yd'yd != . & rem_npoinm_`yd'yd >=0
}

keep placekey brn_npoi_*yd ind_npoi_*yd rem_npoi_*yd brn_npoinm_*yd ind_npoinm_*yd rem_npoinm_*yd
unique placekey
tempfile micro_jan20_brandtype
save `micro_jan20_brandtype', replace /*Variables needed: placekey, *brandnpoi_*yd, *remainnpoi_*yd, brn_npoinm_*yd, ind_npoinm_*yd, rem_npoinm_*yd */

* Change in microarea total traffic between April 2019 and April 2020 (excluding focal store and banks)
* From gen_drug_nearby_businesses_radius_stores.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear
keep if year <= 2020
keep if month == 4

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

assert allpoi_totnvisits_200yd !=.
assert allpoi_totnvisits_500yd !=.
assert allpoi_totnvisits_1000yd !=.

* Fill missing traffic data with zero.  
foreach yd of numlist 200 500 1000 {
    replace finance_totnvisits_`yd'yd = 0 if finance_totnvisits_`yd'yd ==.
    replace finance_xb_totnvisits_`yd'yd = 0 if finance_xb_totnvisits_`yd'yd ==.
    g bank_totnvisits_`yd'yd = finance_totnvisits_`yd'yd - finance_xb_totnvisits_`yd'yd
    g allpoi_totnvisits_`yd'yd_new = allpoi_totnvisits_`yd'yd -  bank_totnvisits_`yd'yd 
}

* Calculate change in tract and microarea traffic excluding focal store and microarea bank traffic
g allpoi_totnvisits_200yd_exc = (allpoi_totnvisits_200yd_new - nvisits)
g allpoi_totnvisits_500yd_exc = (allpoi_totnvisits_500yd_new - nvisits)
g allpoi_totnvisits_1000yd_exc = (allpoi_totnvisits_1000yd_new - nvisits)

assert allpoi_totnvisits_200yd_exc  == . if nvisits == .
assert allpoi_totnvisits_200yd_exc  >= 0 if nvisits != .
assert allpoi_totnvisits_500yd_exc  == . if nvisits == .
assert allpoi_totnvisits_500yd_exc  >= 0 if nvisits != .
assert allpoi_totnvisits_1000yd_exc  == . if nvisits == .
assert allpoi_totnvisits_1000yd_exc  >= 0 if nvisits != .

keep placekey allpoi_totnvisits_200yd_exc allpoi_totnvisits_500yd_exc allpoi_totnvisits_1000yd_exc year

preserve
keep if year == 2019
drop year
ren * *19
ren placekey19 placekey
tempfile chg19
save `chg19', replace
restore

merge m:1 placekey using `chg19', assert(3) nogen
g all_200yd_chg_exc4 = log(allpoi_totnvisits_200yd_exc) - log(allpoi_totnvisits_200yd_exc19)
g all_500yd_chg_exc4 = log(allpoi_totnvisits_500yd_exc) - log(allpoi_totnvisits_500yd_exc19)
g all_1000yd_chg_exc4 = log(allpoi_totnvisits_1000yd_exc) - log(allpoi_totnvisits_1000yd_exc19)

keep if year == 2020
keep placekey *chg*
unique placekey
tempfile chgapr
save `chgapr', replace

* Change in microarea traffic between 2019 and 2020 brands, nonbrands, and remaining (April) 
* Change in microarea total traffic for brand retailers, independent retailers, and non-retail POI between April 2019 and April 2020 (excluding focal store and banks)

* From gen_drug_nearby_businesses_radius_brandtype.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear
keep if year <= 2020
keep if month == 4

assert allpoi_totnvisits_200yd !=. & brn_totnvisits_200yd != . & ind_totnvisits_200yd != . & rem_totnvisits_200yd != .
assert allpoi_totnvisits_500yd !=. & brn_totnvisits_500yd != . & ind_totnvisits_500yd != . & rem_totnvisits_500yd != .
assert allpoi_totnvisits_1000yd !=. & brn_totnvisits_1000yd != . & ind_totnvisits_1000yd != . & rem_totnvisits_1000yd != .

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

foreach yd of numlist 200 500 1000{
    assert npoi_`yd'yd == brandnpoi_`yd'yd + nonbrandnpoi_`yd'yd + remainnpoi_`yd'yd
    assert allpoi_totnvisits_`yd'yd == brn_totnvisits_`yd'yd + ind_totnvisits_`yd'yd + rem_totnvisits_`yd'yd
}

* Calculate microarea traffic excluding focal store for brands and nonbrands
g nvisits_brand = nvisits if d_brand == 1
replace nvisits_brand = 0 if d_brand == 0
assert nvisits_brand != . if nvisits != .

g nvisits_nonbrand = nvisits if d_brand == 0
replace nvisits_nonbrand = 0 if d_brand == 1
assert nvisits_nonbrand != . if nvisits != .

* Total microarea traffic for branded, independent, and non-retailer POI. 
foreach yd of numlist 200 500 1000{
    g brn_totnvisits_`yd'yd_exc = (brn_totnvisits_`yd'yd - nvisits_brand)
    assert brn_totnvisits_`yd'yd_exc == . if nvisits_brand == .
    assert brn_totnvisits_`yd'yd_exc >= 0

    g ind_totnvisits_`yd'yd_exc = (ind_totnvisits_`yd'yd - nvisits_nonbrand)
    assert ind_totnvisits_`yd'yd_exc == . if nvisits_nonbrand == .
    assert ind_totnvisits_`yd'yd_exc >= 0

    * For non-retail POI, exclude banks in microarea.
    replace finance_totnvisits_`yd'yd = 0 if finance_totnvisits_`yd'yd ==.
    replace finance_xb_totnvisits_`yd'yd = 0 if finance_xb_totnvisits_`yd'yd ==.
    g bank_totnvisits_`yd'yd = finance_totnvisits_`yd'yd - finance_xb_totnvisits_`yd'yd

    g rem_totnvisits_`yd'yd_exc = rem_totnvisits_`yd'yd -  bank_totnvisits_`yd'yd 
    assert rem_totnvisits_`yd'yd_exc != .
    assert rem_totnvisits_`yd'yd_exc >= 0
}

keep placekey year ///
    brn_totnvisits_200yd_exc brn_totnvisits_500yd_exc brn_totnvisits_1000yd_exc ///
    ind_totnvisits_200yd_exc ind_totnvisits_500yd_exc ind_totnvisits_1000yd_exc ///
    rem_totnvisits_200yd_exc rem_totnvisits_500yd_exc rem_totnvisits_1000yd_exc

preserve
keep if year == 2019
drop year
ren * *19
ren placekey19 placekey 
keep placekey *exc19
tempfile chg19
save `chg19', replace
restore

merge m:1 placekey using `chg19', assert(3) nogen

* Change in traffic between Aprils 2019 and 2020.
g brn_200yd_chg_exc4 = log(brn_totnvisits_200yd_exc) - log(brn_totnvisits_200yd_exc19)
g brn_500yd_chg_exc4 = log(brn_totnvisits_500yd_exc) - log(brn_totnvisits_500yd_exc19)
g brn_1000yd_chg_exc4 = log(brn_totnvisits_1000yd_exc) - log(brn_totnvisits_1000yd_exc19)

g ind_200yd_chg_exc4 = log(ind_totnvisits_200yd_exc) - log(ind_totnvisits_200yd_exc19)
g ind_500yd_chg_exc4 = log(ind_totnvisits_500yd_exc) - log(ind_totnvisits_500yd_exc19)
g ind_1000yd_chg_exc4 = log(ind_totnvisits_1000yd_exc) - log(ind_totnvisits_1000yd_exc19)

g rem_200yd_chg_exc4 = log(rem_totnvisits_200yd_exc) - log(rem_totnvisits_200yd_exc19)
g rem_500yd_chg_exc4 = log(rem_totnvisits_500yd_exc) - log(rem_totnvisits_500yd_exc19)
g rem_1000yd_chg_exc4 = log(rem_totnvisits_1000yd_exc) - log(rem_totnvisits_1000yd_exc19)

keep if year == 2020
keep placekey *chg*
unique placekey
tempfile chgapr_brandtype
save `chgapr_brandtype', replace

* Time-varying microarea average traffic levels 
* From gen_drug_nearby_businesses_radius_stores.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

* Calculate microarea traffic excluding traffic to banks. Restrict POIs with non-missing traffic data.
foreach yd of numlist 200 500 1000{
    assert allpoi_npoinm_`yd'yd != . & allpoi_npoinm_`yd'yd >=0
    replace finance_npoinm_`yd'yd = 0 if finance_npoinm_`yd'yd ==.
    replace finance_xb_npoinm_`yd'yd = 0 if finance_xb_npoinm_`yd'yd ==.
    g npoinm_bank_`yd'yd = finance_npoinm_`yd'yd - finance_xb_npoinm_`yd'yd 
    g npoinm_xb_`yd'yd = allpoi_npoinm_`yd'yd - npoinm_bank_`yd'yd
    *
    g npoinm_`yd'yd_exc = npoinm_xb_`yd'yd - 1
    assert nvisits == . if npoinm_`yd'yd_exc < 0    
    replace npoinm_`yd'yd_exc = 0 if npoinm_`yd'yd_exc < 0
    assert npoinm_`yd'yd_exc >= 0
    assert allpoi_totnvisits_`yd'yd != . & allpoi_totnvisits_`yd'yd >= 0
    *
    replace finance_totnvisits_`yd'yd = 0 if finance_totnvisits_`yd'yd ==.
    replace finance_xb_totnvisits_`yd'yd = 0 if finance_xb_totnvisits_`yd'yd ==.
    g bank_totnvisits_`yd'yd = finance_totnvisits_`yd'yd - finance_xb_totnvisits_`yd'yd
    * Exclude bank traffic in microarea
    g all_totnvisits_`yd'yd_new = allpoi_totnvisits_`yd'yd - bank_totnvisits_`yd'yd
    assert all_totnvisits_`yd'yd_new  != . & all_totnvisits_`yd'yd_new  >=0
    g all_avgnvisits_`yd'yd_exc = (all_totnvisits_`yd'yd - nvisits)/npoinm_`yd'yd_exc
    assert all_avgnvisits_`yd'yd_exc >= 0
    assert all_avgnvisits_`yd'yd_exc  == . if nvisits == . | npoinm_`yd'yd_exc == 0
    replace all_avgnvisits_`yd'yd_exc = 0 if npoinm_`yd'yd_exc == 0
}
        
keep placekey date_range_start all_avgnvisits_200yd_exc all_avgnvisits_500yd_exc all_avgnvisits_1000yd_exc
unique placekey date_range_start
tempfile avgtraffic
save `avgtraffic', replace

* Time-varying microarea average traffic levels for brand retailers, independent retailers, and non-retail POIS. 
* From gen_drug_nearby_businesses_radius_brandtype.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear

merge 1:1 placekey date_range_start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", assert(2 3) keepusing(d_brand nvisits)
drop if _merge == 2
assert _merge == 3
drop _merge

foreach yd of numlist 200 500 1000{
    assert allpoi_totnvisits_`yd'yd == brn_totnvisits_`yd'yd + ind_totnvisits_`yd'yd + rem_totnvisits_`yd'yd
    assert allpoi_npoinm_`yd'yd == brn_npoinm_`yd'yd + ind_npoinm_`yd'yd + rem_npoinm_`yd'yd
}

g nvisits_brand = nvisits if d_brand == 1
replace nvisits_brand = 0 if d_brand == 0
assert nvisits_brand != . if nvisits != .

g nvisits_nonbrand = nvisits if d_brand == 0
replace nvisits_nonbrand = 0 if d_brand == 1
assert nvisits_nonbrand != . if nvisits != .

* Calculate microarea traffic excluding traffic to banks. Restrict POIs with non-missing traffic data.
foreach yd of numlist 200 500 1000{
    g brn_npoinm_`yd'yd_exc = brn_npoinm_`yd'yd - d_brand if nvisits != .
    replace brn_npoinm_`yd'yd_exc = 0 if brn_npoinm_`yd'yd_exc < 0
    assert brn_npoinm_`yd'yd_exc >= 0
    g brn_avgnvisits_`yd'yd_exc = (brn_totnvisits_`yd'yd - nvisits_brand)/(brn_npoinm_`yd'yd_exc)
    replace brn_avgnvisits_`yd'yd_exc = 0 if brn_npoinm_`yd'yd_exc == 0
    assert brn_avgnvisits_`yd'yd_exc >= 0

    g ind_npoinm_`yd'yd_exc = ind_npoinm_`yd'yd - ~d_brand if nvisits != .
    replace ind_npoinm_`yd'yd_exc = 0 if ind_npoinm_`yd'yd_exc < 0
    assert ind_npoinm_`yd'yd_exc >= 0
    g ind_avgnvisits_`yd'yd_exc = (ind_totnvisits_`yd'yd - nvisits_nonbrand)/(ind_npoinm_`yd'yd_exc)
    replace ind_avgnvisits_`yd'yd_exc = 0 if ind_npoinm_`yd'yd_exc == 0
    assert ind_avgnvisits_`yd'yd_exc >= 0

    g rem_npoinm_`yd'yd_exc = rem_npoinm_`yd'yd 
    replace rem_npoinm_`yd'yd_exc = 0 if rem_npoinm_`yd'yd_exc < 0
    assert rem_npoinm_`yd'yd_exc >= 0
    g rem_avgnvisits_`yd'yd_exc = (rem_totnvisits_`yd'yd)/(rem_npoinm_`yd'yd_exc)
    replace rem_avgnvisits_`yd'yd_exc = 0 if rem_npoinm_`yd'yd_exc == 0
    assert rem_avgnvisits_`yd'yd_exc >= 0
}

keep placekey date_range_start ///
    brn_avgnvisits_200yd* brn_avgnvisits_500yd* brn_avgnvisits_1000yd* ///
    ind_avgnvisits_200yd* ind_avgnvisits_500yd* ind_avgnvisits_1000yd* ///
    rem_avgnvisits_200yd* rem_avgnvisits_500yd* rem_avgnvisits_1000yd* 

unique placekey date_range_start
tempfile avgtraffic_brandtype
save `avgtraffic_brandtype', replace

* Calculate dummy on microarea: # of pharmacies, # grocery store, # wholesale retail POI, # post office, # finance POI, # finance POI (excluding banks), # Real estate and professional services POI, # medical and health services POI, # Hotel and restaurants POI, # Other services POI, # Religious and NGOS POI, # government POI, # all other POI.
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_nearby_businesses_old_brandtype.dta", clear
keep if date_range_start == "2020-01-01"

* Replace missing variables with 0
foreach var of varlist npoi_*_*yd {
	replace `var' = 0 if `var' == .
}

foreach yd of numlist 200 500 1000{
    assert allpoi_totnvisits_`yd'yd  != . | allpoi_totnvisits_`yd'yd  >0

    * Exclude focal store for pharmacy
    replace npoi_pharmacy_`yd'yd = npoi_pharmacy_`yd'yd - 1

    * Proximity indicator
    g grocery_poi_`yd'yd = npoi_grocery_`yd'yd >0
    g drug_poi_`yd'yd = npoi_pharmacy_`yd'yd >0 /* Exclude self */ 
    g whsale_ret_poi_`yd'yd = npoi_whsale_ret_`yd'yd >0 
    g postoffice_poi_`yd'yd = npoi_postoffice_`yd'yd >0 
    g finance_poi_`yd'yd = npoi_finance_`yd'yd >0 
    g finance_xb_poi_`yd'yd = npoi_finance_xb_`yd'yd >0 
    g re_prosvc_poi_`yd'yd = npoi_re_prosvc_`yd'yd >0 
    g medhealth_poi_`yd'yd = npoi_medhealth_`yd'yd >0 
    g medical_poi_`yd'yd = npoi_medical_`yd'yd >0 
    g hotel_rest_poi_`yd'yd = npoi_hotel_rest_`yd'yd >0 
    g othersvc_poi_`yd'yd = npoi_othersvc_`yd'yd >0 
    g religious_ngo_poi_`yd'yd = npoi_religious_ngo_`yd'yd >0 
    g government_poi_`yd'yd = npoi_government_`yd'yd >0 
    g other_poi_`yd'yd = npoi_other_`yd'yd >0 
}

keep placekey *_poi_*yd npoi_*_*yd


foreach var of varlist *_poi_*yd{
	di "`var'"
    egen test = mean(`var')
    assert test > 0 & test < 1
    drop test
}
unique placekey
tempfile surrounding
save `surrounding', replace

* All pharmacies excluding pharmacies inside grocery stores, pharmacies colocated with other businesses, and shared polygons (unreliable foot traffic data).
* From gen_advan_drugfiltered_panel.py in code/upstairs/final/
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", clear
g d_nonbrand = ~d_brand

preserve
keep if date_range_start == "2020-03-01"
keep placekey nvisits
ren nvisits nvisits_mar20
unique placekey
tempfile nv2019
save `nv2019', replace
restore

merge m:1 placekey using `nv2019', assert(3) nogen

preserve
keep if date_range_start == "2020-04-01"
keep placekey nvisits
ren nvisits nvisits_apr20
unique placekey
tempfile nv2019
save `nv2019', replace
restore

merge m:1 placekey using `nv2019', assert(3) nogen

* Store-level bank proximity variables (for all banks and by bank type)
* From calc_bank_dist_drug.py in code/upstairs/final/
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_bankbranch_dist.dta", assert(1 3) 
drop _merge

foreach var of varlist nbank_branch_* bank_branch_* ncb_branch_* cb_branch_* nnoncb_branch_* noncb_branch_* nnoncra_branch_* noncra_branch_* nyescra_branch_* yescra_branch_* {
    qui replace `var' = 0 if `var' == .
}

* Set minimum distance to 1mil yards for stores without any bank in county (missing data).
qui replace mindist_from_bank_branch_yd = 1000000 if mindist_from_bank_branch_yd == .
qui replace mindist_from_cb_branch_yd = 1000000 if mindist_from_cb_branch_yd == .
qui replace mindist_from_noncb_branch_yd = 1000000 if mindist_from_noncb_branch_yd == .
qui replace mindist_from_noncra_branch_yd = 1000000 if mindist_from_noncra_branch_yd == .

* Merge with store-level proximity indicators by bank size
* From gen_drug_banksize_proximity.py in code/upstairs/final/
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/drugstore_dist_banksize.dta", ///
    assert(3) nogen keepusing(*mega* *cb* open_* nopen_*)

foreach var of varlist *mega*_*yd *mega*_*yd_ring *cb_*yd *cb_*yd_ring ///
                        open_*_200yd open_*_500yd open_*_1000yd ///
                        open_*_200yd_ring open_*_500yd_ring open_*_1000yd_ring ///
                        nopen_*_200yd nopen_*_500yd nopen_*_1000yd ///
                        nopen_*_200yd_ring nopen_*_500yd_ring nopen_*_1000yd_ring {
    assert `var' != .
}

foreach yd of numlist 200 500 1000 {
    assert open_cb_`yd'yd    <= cb_`yd'yd
    assert nopen_cb_`yd'yd   <= ncb_`yd'yd
    assert open_mega_`yd'yd  <= mega_`yd'yd
    assert nopen_mega_`yd'yd <= nmega_`yd'yd
}

gen quarter = ceil(month/3)
g year = substr(date_range_start,1,4)
destring year, replace

* Tract-level small business lending data for 2019 (CRA loan data)
merge m:1 tract using `tract_cra'
assert tract == "00003526004" | tract == "39049001121" if _merge == 1 /* Last tract had CRA lending 2020 but not in 2019. No actual loans in this tract */
drop if _merge == 2

foreach var of varlist NoSBL100k_all19 AmtSBL100k_all19 NoSBL250k_all19 AmtSBL250k_all19 NoSBL_all19 AmtSBL_all19 NoSBL_rev_0_1mil_all19 AmtSBL_rev_0_1mil_all19{
    replace `var' = 0 if _merge == 1
    assert `var' != .
}
drop _merge

g cty = substr(tract, 1, 5)

* Tract-level ranking of amount and number of CRA loans across all tracts. 
merge m:1 tract using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/all_tract_cra19_rank.dta"
assert tract == "00003526004" | tract == "39049001121" if _merge == 1 /* Last tract had CRA lending 2020 but not in 2019. No actual loans in this tract */
drop if _merge == 2

foreach var of varlist H*SBL* T*SBL* Q*SBL* D*SBL* H*medinc* T*medinc* Q*medinc* D*medinc*{
    replace `var' = 0 if _merge == 1
    assert `var' != .
}
drop _merge

* Merge with all deposit rank 
* From gen_allmega_depositgrowth_rank.do in C:\Users\elliotoh\Box\lodes_shared\pharmacy\code\stata\regression\cumchange\main\data generation\stata
merge m:1 placekey year quarter using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/allmega_depgrowth_rank.dta"
drop if _merge == 2
replace _merge = . if (mega_500yd == 0 | d_brand == 1 | (mega_500yd == 1 & nmega_500yd != nbank_branch_500yd ) | year == 2019 | (year == 2020 & month <= 3)) & _merge == 1

foreach var of varlist *depgr_all{
    di "`var'"
    replace `var' = 0 if _merge == .
    assert `var' != .
}
assert _merge != 1
drop _merge

* Microarea data: # POI, # branded retailer POI, # independent retailer POI, # non-retailer POI
merge m:1 placekey using `micro_jan20', keepusing (npoi_xb_200yd npoinm_xb_200yd npoi_xb_500yd npoinm_xb_500yd npoi_xb_1000yd npoinm_xb_1000yd) assert(3) nogen 
merge m:1 placekey using `micro_jan20_brandtype', keepusing(brn_npoi_*yd ind_npoi_*yd rem_npoi_*yd brn_npoinm_*yd ind_npoinm_*yd rem_npoinm_*yd) assert(3) nogen 

g strlen = strlen(tract)
tab strlen
assert strlen == 11
drop strlen

g strlen = strlen(zip)
tab strlen
replace zip = "0" + zip if strlen == 4
drop strlen
g strlen = strlen(zip)
assert strlen == 5
drop strlen

merge m:1 tract using `acs'
unique tract if _merge == 3
unique tract if _merge == 1
unique tract if _merge == 2
keep if _merge == 3
drop _merge

assert placekey != ""

* Time-varying microarea average traffic levels
merge 1:1 placekey date_range_start using `avgtraffic', assert(2 3)
drop if _merge == 2
drop _merge

* Time-varying microarea average traffic levels for branded retailer, independent retailer, and non-retailer POI
merge 1:1 placekey date_range_start using `avgtraffic_brandtype', assert(2 3)
drop if _merge == 2
drop _merge

preserve
keep if date_range_start == "2020-03-01"
keep placekey brn_avgnvisits_*yd ind_avgnvisits_*yd rem_avgnvisits_*yd 
ren brn_avgnvisits_*yd brn_avgnvisits_*yd_mar20
ren ind_avgnvisits_*yd ind_avgnvisits_*yd_mar20
ren rem_avgnvisits_*yd rem_avgnvisits_*yd_mar20
unique placekey
tempfile nv2019
save `nv2019', replace
restore

merge m:1 placekey using `nv2019', assert(3) nogen

g start3 = date(date_range_start, "YMD")
format start3 %td
g end3 = date(date_range_end, "YMD")
format end3 %td
ren *3 *
assert start != . & end != .            

egen id_ad = group(placekey)
egen imonth = group(start)
xtset id_ad imonth 

g ln_visits = ln(nvisits)
g dln_visits_mar20 = ln(nvisits) - ln(nvisits_mar20)

* Tract-level brand presence based on full drug store data in Jan 2020
merge m:1 tract using `brand_count1', assert(2 3)
drop if _merge == 2
drop _merge

* Exclude focal store
replace ndrugtr = (ndrugtr - 1)
replace nbrandtr = (nbrandtr - d_brand)
replace nnbrandtr = (nnbrandtr - ~d_brand)

* Microarea traffic change between April 2019 and April 2020
merge m:1 placekey using `chgapr', assert(2 3)
drop if _merge == 2
drop _merge

* Microarea traffic change between April 2019 and April 2020 (brand retailer, independent retailer, non-retailer POI)
merge m:1 placekey using `chgapr_brandtype', assert(2 3)
drop if _merge == 2
drop _merge

* Microarea business environment dummies: pharmacies, grocery stores and 2-digit NAICS code.
merge m:1 placekey using `surrounding', assert(2 3)
drop if _merge == 2
drop _merge

unique placekey start
// keep if start >= date("2020-01-01","YMD")
drop category_tags closed_on device_type enclosed end geometry_type is_synthetic n_dwell_times_21_60 n_dwell_times_240_ n_dwell_times_5 n_dwell_times_5_20 n_dwell_times_61_240 n_nonus_visitors n_us_visitors norm_visits_by_reg_nc_visitors norm_visits_by_reg_nc_visits norm_visits_by_state_scaling norm_visits_by_total_visitors norm_visits_by_total_visits open_hours opened_on phone_number polygon_class popularity_by_day popularity_by_hour sub_category top_category tracking_closed_since visits_by_day visits_by_day_std websites 

* drop one invalid tracts
drop if regexm(tract, "^00")
g state = substr(tract, 1, 2)

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", replace
