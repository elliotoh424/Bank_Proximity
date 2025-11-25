* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_independents_drug.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
bysort placekey: egen treated_ever = max(treated)
g never_treated = treated_ever == 0
tab never_treated treated

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/bank_radius500_nonbrand_ipw.dta"
assert _merge == 1 if ~d_nbrand 
keep if _merge == 3
drop _merge

g no_overlap = (nbank500 == nmega500) | (nbank500 == nnonmega500) | (nbank500 == ncb500)

// drop if nonmega500 | cb500
g d_win10_mega = (d_pre1_mega | d_pre0_mega )
g d_win11_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega )
g d_win12_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega)
g d_win13_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega | d_post3_mega)

g n_win10 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb)
g n_win11 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb)  
g n_win12 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb)
g n_win13 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb) + (n_post3_mega + n_post3_nonmega + d_post3_cb)

**************
* Post variables
g post = start >= date("2020-04-01","YMD")
g post_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g post_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g post_apr22_nov22 = start >= date("2022-04-01","YMD")

local banksize "mega500 nonmega500 cb500"
foreach bs of local banksize{

g `bs'Xd_win10 = `bs' * d_win10_mega
g `bs'Xd_win11 = `bs' * d_win11_mega
g `bs'Xd_win12 = `bs' * d_win12_mega
g `bs'Xd_win13 = `bs' * d_win13_mega
assert `bs'Xd_win10 != . & `bs'Xd_win11 != . & `bs'Xd_win12 != . & `bs'Xd_win13 != .

g `bs'Xp_apr20_mar21 = `bs' * post_apr20_mar21 
g `bs'Xp_apr21_mar22 = `bs' * post_apr21_mar22
g `bs'Xp_apr22_nov22 = `bs' * post_apr22_nov22
assert `bs'Xp_apr20_mar21 != . & `bs'Xp_apr21_mar22 != . & `bs'Xp_apr22_nov22 != .


* Separate periods
g `bs'Xd_pre1Xp_apr20_mar21 = `bs' * d_pre1_mega * post_apr20_mar21 
g `bs'Xd_pre1Xp_apr21_mar22 = `bs' * d_pre1_mega * post_apr21_mar22
g `bs'Xd_pre1Xp_apr22_nov22 = `bs' * d_pre1_mega * post_apr22_nov22
assert `bs'Xd_pre1Xp_apr20_mar21 != . & `bs'Xd_pre1Xp_apr21_mar22 != . & `bs'Xd_pre1Xp_apr22_nov22 != .

g `bs'Xd_pre0Xp_apr20_mar21 = `bs' * d_pre0_mega * post_apr20_mar21 
g `bs'Xd_pre0Xp_apr21_mar22 = `bs' * d_pre0_mega * post_apr21_mar22
g `bs'Xd_pre0Xp_apr22_nov22 = `bs' * d_pre0_mega * post_apr22_nov22
assert `bs'Xd_pre0Xp_apr20_mar21 != . & `bs'Xd_pre0Xp_apr21_mar22 != . & `bs'Xd_pre0Xp_apr22_nov22 != .

g `bs'Xd_post1Xp_apr20_mar21 = `bs' * d_post1_mega * post_apr20_mar21 
g `bs'Xd_post1Xp_apr21_mar22 = `bs' * d_post1_mega * post_apr21_mar22
g `bs'Xd_post1Xp_apr22_nov22 = `bs' * d_post1_mega * post_apr22_nov22
assert `bs'Xd_post1Xp_apr20_mar21 != . & `bs'Xd_post1Xp_apr21_mar22 != . & `bs'Xd_post1Xp_apr22_nov22 != .

g `bs'Xd_post2Xp_apr20_mar21 = `bs' * d_post2_mega * post_apr20_mar21 
g `bs'Xd_post2Xp_apr21_mar22 = `bs' * d_post2_mega * post_apr21_mar22
g `bs'Xd_post2Xp_apr22_nov22 = `bs' * d_post2_mega * post_apr22_nov22
assert `bs'Xd_post2Xp_apr20_mar21 != . & `bs'Xd_post2Xp_apr21_mar22 != . & `bs'Xd_post2Xp_apr22_nov22 != .

g `bs'Xd_post3Xp_apr20_mar21 = `bs' * d_post3_mega * post_apr20_mar21 
g `bs'Xd_post3Xp_apr21_mar22 = `bs' * d_post3_mega * post_apr21_mar22
g `bs'Xd_post3Xp_apr22_nov22 = `bs' * d_post3_mega * post_apr22_nov22
assert `bs'Xd_post3Xp_apr20_mar21 != . & `bs'Xd_post3Xp_apr21_mar22 != . & `bs'Xd_post3Xp_apr22_nov22 != .

* Combine periods
g `bs'Xd_win10Xp_apr20_mar21 = `bs'Xd_win10 * post_apr20_mar21 
g `bs'Xd_win10Xp_apr21_mar22 = `bs'Xd_win10 * post_apr21_mar22
g `bs'Xd_win10Xp_apr22_nov22 = `bs'Xd_win10 * post_apr22_nov22

g `bs'Xd_win11Xp_apr20_mar21 = `bs'Xd_win11 * post_apr20_mar21 
g `bs'Xd_win11Xp_apr21_mar22 = `bs'Xd_win11 * post_apr21_mar22
g `bs'Xd_win11Xp_apr22_nov22 = `bs'Xd_win11 * post_apr22_nov22

g `bs'Xd_win12Xp_apr20_mar21 = `bs'Xd_win12 * post_apr20_mar21 
g `bs'Xd_win12Xp_apr21_mar22 = `bs'Xd_win12 * post_apr21_mar22
g `bs'Xd_win12Xp_apr22_nov22 = `bs'Xd_win12 * post_apr22_nov22

g `bs'Xd_win13Xp_apr20_mar21 = `bs'Xd_win13 * post_apr20_mar21 
g `bs'Xd_win13Xp_apr21_mar22 = `bs'Xd_win13 * post_apr21_mar22
g `bs'Xd_win13Xp_apr22_nov22 = `bs'Xd_win13 * post_apr22_nov22
}

tab yq d_pre0_mega  if ipw != . & n_win11 <= 1
tab yq d_win11_mega  if ipw != . & n_win11 <= 1
tab yq d_win12_mega  if ipw != . & n_win12 <= 1
tab yq d_win13_mega  if ipw != . & n_win13 <= 1

************************************************************************************************************
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

local yvar "dln_visits_micro_mar20"

local megaonly "mega500"
global output_file "tables/ind_banksize_exam_foottraffic_did_int"

eststo clear

********************************************************************************
* Base: Table 2 Column 1. Megabank500. Include pre0
********************************************************************************

* Baseline
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & n_win11 <= 1, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd' - no overlap"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Only Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "One Mega`yd'"

* Window 11
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win11Xp_apr20_mar21 mega500Xd_win11Xp_apr21_mar22 mega500Xd_win11Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win11Xp_apr20_mar21 mega500Xd_win11Xp_apr21_mar22 mega500Xd_win11Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & n_win11 <= 1, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd' - no overlap"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win11Xp_apr20_mar21 mega500Xd_win11Xp_apr21_mar22 mega500Xd_win11Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Only Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win11Xp_apr20_mar21 mega500Xd_win11Xp_apr21_mar22 mega500Xd_win11Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "One Mega`yd'"


* Window 12
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win12Xp_apr20_mar21 mega500Xd_win12Xp_apr21_mar22 mega500Xd_win12Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win12Xp_apr20_mar21 mega500Xd_win12Xp_apr21_mar22 mega500Xd_win12Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & n_win12 <= 1, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd' - no overlap"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win12Xp_apr20_mar21 mega500Xd_win12Xp_apr21_mar22 mega500Xd_win12Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Only Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win12Xp_apr20_mar21 mega500Xd_win12Xp_apr21_mar22 mega500Xd_win12Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "One Mega`yd'"


* Window 13
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win13Xp_apr20_mar21 mega500Xd_win13Xp_apr21_mar22 mega500Xd_win13Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win13Xp_apr20_mar21 mega500Xd_win13Xp_apr21_mar22 mega500Xd_win13Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & n_win13 <= 1, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Any Mega`yd' - no overlap"


* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win13Xp_apr20_mar21 mega500Xd_win13Xp_apr21_mar22 mega500Xd_win13Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "Only Mega`yd'"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win13Xp_apr20_mar21 mega500Xd_win13Xp_apr21_mar22 mega500Xd_win13Xp_apr22_nov22 ///
nonmega500Xp_apr20_mar21 nonmega500Xp_apr21_mar22 nonmega500Xp_apr22_nov22 ///
cb500Xp_apr20_mar21 cb500Xp_apr21_mar22 cb500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Cty-Month"
qui estadd local ctrl "Y"
qui estadd local sample "One Mega`yd'"



esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:ln(\#Visits)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 cluster ctrl sample r2_a F, fmt(0 0 0 0 0 2 2) labels("Nobs" "FE" "Cluster" "Controls" "Sample" "Adj. R$^{2}$" "F Stat")) ///
 title("CRA Examination on Nonbrand Foot traffic around Megabank`yd'") nomtitle 

