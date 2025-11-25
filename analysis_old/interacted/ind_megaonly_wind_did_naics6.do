* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"
// cd "/home/nber/elliotoh/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_independents_drug.dta", clear
// use "/home/nber/elliotoh/craexam_independents_drug.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
bysort placekey: egen treated_ever = max(treated)
g never_treated = treated_ever == 0
tab never_treated treated

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta"
// merge m:1 placekey using "/home/nber/elliotoh/megaonly_radius500_ipw.dta"
drop if _merge == 2
assert _merge == 1 if nonmega500 >0 & cb500 > 0  
keep if _merge == 3
drop _merge

ren *_per_* *_*
ren *namt_est *nest
ren *namt_n9 *nest9
ren *namt_n19 *nest19
ren *namt_n49 *nest49

// drop if nonmega500 | cb500
g d_win10_mega = (d_pre1_mega | d_pre0_mega )
g d_win11_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega )
g d_win12_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega)
g d_win13_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega | d_post3_mega)

// g n_win10 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb)
// g n_win11 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb)  
// g n_win12 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb)
// g n_win13 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb) + (n_post3_mega + n_post3_nonmega + d_post3_cb)

g mega500Xd_win10 = mega500 * d_win10_mega
g mega500Xd_win11 = mega500 * d_win11_mega
g mega500Xd_win12 = mega500 * d_win12_mega
g mega500Xd_win13 = mega500 * d_win13_mega

// tab yq d_pre0_mega  if ipw != . & n_win11 <= 1
// tab yq d_win11_mega  if ipw != . & n_win11 <= 1
// tab yq d_win12_mega  if ipw != . & n_win12 <= 1
// tab yq d_win13_mega  if ipw != . & n_win13 <= 1


**************
* Post variables
g post = start >= date("2020-04-01","YMD")
g post_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g post_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g post_apr22_nov22 = start >= date("2022-04-01","YMD")

g mega500Xp_apr20_mar21 = mega500 * post_apr20_mar21 
g mega500Xp_apr21_mar22 = mega500 * post_apr21_mar22
g mega500Xp_apr22_nov22 = mega500 * post_apr22_nov22

g mega500Xd_win10Xp_apr20_mar21 = mega500Xd_win10 * post_apr20_mar21 
g mega500Xd_win10Xp_apr21_mar22 = mega500Xd_win10 * post_apr21_mar22
g mega500Xd_win10Xp_apr22_nov22 = mega500Xd_win10 * post_apr22_nov22

g mega500Xd_win11Xp_apr20_mar21 = mega500Xd_win11 * post_apr20_mar21 
g mega500Xd_win11Xp_apr21_mar22 = mega500Xd_win11 * post_apr21_mar22
g mega500Xd_win11Xp_apr22_nov22 = mega500Xd_win11 * post_apr22_nov22

g mega500Xd_win12Xp_apr20_mar21 = mega500Xd_win12 * post_apr20_mar21 
g mega500Xd_win12Xp_apr21_mar22 = mega500Xd_win12 * post_apr21_mar22
g mega500Xd_win12Xp_apr22_nov22 = mega500Xd_win12 * post_apr22_nov22

g mega500Xd_win13Xp_apr20_mar21 = mega500Xd_win13 * post_apr20_mar21 
g mega500Xd_win13Xp_apr21_mar22 = mega500Xd_win13 * post_apr21_mar22
g mega500Xd_win13Xp_apr22_nov22 = mega500Xd_win13 * post_apr22_nov22

************************************************************************************************************
g naics6 = substr(naics_code_micro,1,6)
g naics5 = substr(naics_code_micro,1,5)
g naics3 = substr(naics_code_micro,1,3)
g naics2 = substr(naics_code_micro,1,2)

local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

// local drug_control "ndrug`yd'"
// local microarea_control "lnpoi`yd' all_`yd'yd_chg_exc4"
// local tract_control "lmedage lmedinc pctuniv pctmnty lpdsty lmedhome pct0to10 pctpublic"

* Restrict to industries with establishment (n>9) > 5000
* Too few obs: 811192, 81219, 44819
// local naics6 "713940 721110 722511 713990 722320 812910 812990 812199 445310"
// local naics5 "71394 72111 44529 71399 72232 81291 44611 81119 81299 72251 44531 81232 72241 "
// local naics4 "7211 7139 4452 7225 4453 7224 8123 8129 4511 4412 7113"
local naics3 "713 721 722 445 812"
foreach nc of local naics3{

local yvar "dln_visits_micro_mar20"

local megaonly "mega500"
global output_file "tables/ind_megaonly_exam_foottraffic_did_naics_naics`nc'"

eststo clear

********************************************************************************
* Base: Table 2 Column 1. Megabank500. Include pre0
********************************************************************************

* Baseline
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & naics3 == "`nc'", ///
absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Store"
qui estadd local microarea_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega`yd'"

* Window 10
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win10Xp_apr20_mar21 mega500Xd_win10Xp_apr21_mar22 mega500Xd_win10Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & naics3 == "`nc'", ///
absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Store"
qui estadd local microarea_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega`yd'"

* Window 11
********************************************************************************
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500Xd_win11Xp_apr20_mar21 mega500Xd_win11Xp_apr21_mar22 mega500Xd_win11Xp_apr22_nov22 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & naics3 == "`nc'", ///
absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local cluster "Store"
qui estadd local microarea_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega`yd'"

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:ln(\#Visits)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 cluster microarea_ctrl tract_ctrl sample variable r2_a F, fmt(0 0 0 0 0 0 0 2 2) labels("Nobs" "FE" "Cluster" "Microarea Controls" "Tract Controls" "Sample" "Variable" "Adj. R$^{2}$" "F Stat")) ///
 title("CRA Examination on Nonbrand Foot traffic around Megabank`yd' NAICS `nc'") nomtitle 
}
