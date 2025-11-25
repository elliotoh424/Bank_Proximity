* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_drugstore.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
bysort placekey: egen treated_ever = max(treated)
g never_treated = treated_ever == 0
tab never_treated treated
ren *rev_cty *rc

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta", assert(1 3)
drop if d_brand == 1
assert _merge == 1 if nonmega500 >0 & cb500 > 0  
keep if _merge == 3
drop _merge

// drop if nonmega500 | cb500
g d_win10_mega = (d_pre1_mega | d_pre0_mega )
g d_win11_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega )
g d_win12_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega)
g d_win13_mega = (d_pre1_mega | d_pre0_mega | d_post1_mega | d_post2_mega | d_post3_mega)

g n_win10 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb)
g n_win11 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb)  
g n_win12 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb)
g n_win13 = (n_pre1_mega + n_pre1_nonmega + d_pre1_cb) + (n_pre0_mega + n_pre0_nonmega + d_pre0_cb) + (n_post1_mega + n_post1_nonmega + d_post1_cb) + (n_post2_mega + n_post2_nonmega + d_post2_cb) + (n_post3_mega + n_post3_nonmega + d_post3_cb)

g mega500Xd_win10 = mega500 * d_win10_mega
g mega500Xd_win11 = mega500 * d_win11_mega
g mega500Xd_win12 = mega500 * d_win12_mega
g mega500Xd_win13 = mega500 * d_win13_mega

local hightypes "H T Q"
foreach h of local hightypes{
g mega500X`h'NoSBLrc = mega500 * `h'NoSBLrc

g mega500Xd_win10X`h'NoSBLrc = mega500Xd_win10 * `h'NoSBLrc
g mega500Xd_win11X`h'NoSBLrc = mega500Xd_win11 * `h'NoSBLrc
g mega500Xd_win12X`h'NoSBLrc = mega500Xd_win12 * `h'NoSBLrc
g mega500Xd_win13X`h'NoSBLrc = mega500Xd_win13 * `h'NoSBLrc
}


tab yq d_pre0_mega  if ipw != . & n_win11 <= 1
tab yq d_win11_mega  if ipw != . & n_win11 <= 1
tab yq d_win12_mega  if ipw != . & n_win12 <= 1
tab yq d_win13_mega  if ipw != . & n_win13 <= 1


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
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

local yvar "dln_visits_mar20"

local megaonly "mega500"
global output_file "tables/megaonly_exam_loan_foottraffic_did_int"

eststo clear

********************************************************************************
* Base: Table 2 Column 1. Megabank500. Include pre0
********************************************************************************

local vars `yvar' dln_ind`yd'
foreach var of local vars {

  * =============================================================================
  * MEGA ONLY SAMPLES - 2021
  * =============================================================================
  
  * Baseline - 2021
  eststo: reghdfe `var' ///
  mega500 ///
  dln_brn500 ///
  [aw = ipw] if d_nbrand & post_apr21_mar22, ///
  absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
  qui estadd local fe1 "Cty-Month FE"
  qui estadd local cluster "Store"
  qui estadd local microarea_ctrl "Y"
  qui estadd local tract_ctrl "Y"
  qui estadd local variable "`var'"
  qui estadd local sample "Only Mega`yd' 2021"
  
  local hightypes "T Q"
  foreach h of local hightypes {
    * Window 10 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win10 ///
    mega500Xd_win10X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    local pct = cond("`h'"=="T", "33%", "25%")
    qui estadd local sample "Only Mega`yd' 2021 - Top `pct'"
    
    * Window 11 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win11 ///
    mega500Xd_win11X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "Only Mega`yd' 2021 - Top `pct'"
    
    * Window 12 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win12 ///
    mega500Xd_win12X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "Only Mega`yd' 2021 - Top `pct'"
  }
  
  * =============================================================================
  * MEGA ONLY SAMPLES - 2022
  * =============================================================================
  
  * Baseline - 2022
  eststo: reghdfe `var' ///
  mega500 ///
  dln_brn500 ///
  [aw = ipw] if d_nbrand & post_apr22_nov22, ///
  absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
  qui estadd local fe1 "Cty-Month FE"
  qui estadd local cluster "Store"
  qui estadd local microarea_ctrl "Y"
  qui estadd local tract_ctrl "Y"
  qui estadd local variable "`var'"
  qui estadd local sample "Only Mega`yd' 2022"
  
  local hightypes "T Q"
  foreach h of local hightypes {
    * Window 10 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win10 ///
    mega500Xd_win10X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    local pct = cond("`h'"=="T", "33%", "25%")
    qui estadd local sample "Only Mega`yd' 2022 - Top `pct'"
    
    * Window 11 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win11 ///
    mega500Xd_win11X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "Only Mega`yd' 2022 - Top `pct'"
    
    * Window 12 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win12 ///
    mega500Xd_win12X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "Only Mega`yd' 2022 - Top `pct'"
  }
  
  * =============================================================================
  * ONE MEGA SAMPLES - 2021
  * =============================================================================
  
  * Baseline - 2021
  eststo: reghdfe `var' ///
  mega500 ///
  dln_brn500 ///
  [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr21_mar22, ///
  absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
  qui estadd local fe1 "Cty-Month FE"
  qui estadd local cluster "Store"
  qui estadd local microarea_ctrl "Y"
  qui estadd local tract_ctrl "Y"
  qui estadd local variable "`var'"
  qui estadd local sample "One Mega`yd' 2021"
  
  local hightypes "T Q"
  foreach h of local hightypes {
    * Window 10 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win10 ///
    mega500Xd_win10X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    local pct = cond("`h'"=="T", "33%", "25%")
    qui estadd local sample "One Mega`yd' 2021 - Top `pct'"
    
    * Window 11 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win11 ///
    mega500Xd_win11X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "One Mega`yd' 2021 - Top `pct'"
    
    * Window 12 - 2021
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win12 ///
    mega500Xd_win12X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr21_mar22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "One Mega`yd' 2021 - Top `pct'"
  }
  
  * =============================================================================
  * ONE MEGA SAMPLES - 2022
  * =============================================================================
  
  * Baseline - 2022
  eststo: reghdfe `var' ///
  mega500 ///
  dln_brn500 ///
  [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr22_nov22, ///
  absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
  qui estadd local fe1 "Cty-Month FE"
  qui estadd local cluster "Store"
  qui estadd local microarea_ctrl "Y"
  qui estadd local tract_ctrl "Y"
  qui estadd local variable "`var'"
  qui estadd local sample "One Mega`yd' 2022"
  
  local hightypes "T Q"
  foreach h of local hightypes {
    * Window 10 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win10 ///
    mega500Xd_win10X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    local pct = cond("`h'"=="T", "33%", "25%")
    qui estadd local sample "One Mega`yd' 2022 - Top `pct'"
    
    * Window 11 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win11 ///
    mega500Xd_win11X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "One Mega`yd' 2022 - Top `pct'"
    
    * Window 12 - 2022
    eststo: reghdfe `var' ///
    mega500 ///
    mega500X`h'NoSBLrc ///
    mega500Xd_win12 ///
    mega500Xd_win12X`h'NoSBLrc ///
    dln_brn500 ///
    [aw = ipw] if d_nbrand & nmega500 <= 1 & post_apr22_nov22, ///
    absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
    qui estadd local fe1 "Cty-Month FE"
    qui estadd local cluster "Store"
    qui estadd local microarea_ctrl "Y"
    qui estadd local tract_ctrl "Y"
    qui estadd local variable "`var'"
    qui estadd local sample "One Mega`yd' 2022 - Top `pct'"
  }
}

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:ln(\#Visits)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 cluster microarea_ctrl tract_ctrl sample variable r2_a F, fmt(0 0 0 0 0 0 0 2 2) labels("Nobs" "FE" "Cluster" "Microarea Controls" "Tract Controls" "Sample" "Variable" "Adj. R$^{2}$" "F Stat")) ///
 title("CRA Examination on Nonbrand Foot traffic around Megabank`yd'") nomtitle 

