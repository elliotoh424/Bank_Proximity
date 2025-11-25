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

merge m:1 placekey_micro using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/ind_megaonly_radius500_ipw.dta"
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

g naics3 = substr(naics4, 1,3)
g naics2 = substr(naics4, 1,2)

g d44 = naics2 == "44"
g d45 = naics2 == "45"
g d71 = naics2 == "71"
g d72 = naics2 == "72"
g d81 = naics2 == "81"


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

foreach var of varlist d44 d45 d71 d72 d81{
g mega500X`var' = mega500 * `var'

g mega500X`var'Xd_win10 = mega500 * `var' * d_win10_mega
g mega500X`var'Xd_win11 = mega500 * `var' * d_win11_mega
g mega500X`var'Xd_win12 = mega500 * `var' * d_win12_mega
g mega500X`var'Xd_win13 = mega500 * `var' * d_win13_mega
}
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
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

local yvar "dln_visits_micro_mar20"

local megaonly "mega500"
global output_file "tables/ind_megaonly_exam_foottraffic_did_nbhdwt"

eststo clear

********************************************************************************
* Base: Table 2 Column 1. Megabank500. Include pre0
********************************************************************************

* Baseline
********************************************************************************
*2020
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr20_mar21, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2020"


*2021
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr21_mar22, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2021"

*2022
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr22_nov22, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2022"

* Exam window
*2020
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 mega500Xwin10 ///
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr20_mar21, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2020"

* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 mega500Xwin10 ///
mega500Xd44 mega500Xd45 mega500Xd71 mega500Xd72 mega500Xd81 /// 
mega500Xd44Xwin10 mega500Xd45Xwin10 mega500Xd71Xwin10 mega500Xd72Xwin10 mega500Xd81Xwin10 /// 
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr20_mar21, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2020"


*2021
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 mega500Xwin10 ///
mega500Xd44 mega500Xd45 mega500Xd71 mega500Xd72 mega500Xd81 /// 
mega500Xd44Xwin10 mega500Xd45Xwin10 mega500Xd71Xwin10 mega500Xd72Xwin10 mega500Xd81Xwin10 /// 
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr21_mar22, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2021"

*2022
* Microarea brand traffic_it 
eststo: reghdfe `yvar' ///
mega500 mega500Xwin10 ///
mega500Xd44 mega500Xd45 mega500Xd71 mega500Xd72 mega500Xd81 /// 
mega500Xd44Xwin10 mega500Xd45Xwin10 mega500Xd71Xwin10 mega500Xd72Xwin10 mega500Xd81Xwin10 /// 
dln_brn500 ///
[aw = ipw] if d_nbrand & post_apr22_nov22, ///
absorb(cty_ym_id drug_control' microarea_control' tract_control') vce(cluster id_ad) 
qui estadd local fe1 "Cty-Month FE" 
qui estadd local cluster "Store" 
qui estadd local microarea_ctrl "Y" 
qui estadd local tract_ctrl "Y" 
qui estadd local variable "yvar'"
qui estadd local sample "Only Megayd' 2022"

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:ln(\#Visits)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 cluster microarea_ctrl tract_ctrl sample variable r2_a F, fmt(0 0 0 0 0 0 0 2 2) labels("Nobs" "FE" "Cluster" "Microarea Controls" "Tract Controls" "Sample" "Variable" "Adj. R$^{2}$" "F Stat")) ///
 title("CRA Examination on Nonbrand Foot traffic around Megabank`yd'") nomtitle 

