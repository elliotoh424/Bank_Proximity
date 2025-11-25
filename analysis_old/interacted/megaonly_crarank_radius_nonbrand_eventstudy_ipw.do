* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

* All stores located in tract with CRA loans and SBL loans.
assert AmtSBL_all19 != .
assert AmtSBL_rev_0_1mil_all19 != .

foreach var of varlist *SBL* {
	replace `var' = 0 if mega500 == 0
}

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta", assert(1 3)
assert _merge == 1 if nonmega500 >0 & cb500 > 0
drop _merge


**************
* Post variables
g post = start >= date("2020-04-01","YMD")
g post_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g post_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g post_apr22_nov22 = start >= date("2022-04-01","YMD")
g indXpost = d_nbrand * post
g indXpost_apr20_mar21 = d_nbrand * post_apr20_mar21
g indXpost_apr21_mar22 = d_nbrand * post_apr21_mar22
g indXpost_apr22_nov22 = d_nbrand * post_apr22_nov22

************************************************************************************************************
// foreach yd of numlist 200 500 1000 {
// foreach yd of numlist 200 500 {
// foreach yd of numlist 500 {


local banksize "mega nonmega cb"
foreach bs of local banksize{
g `bs'500Xpost = `bs'500 * post
g `bs'500Xp_apr20_mar21 = `bs'500 * post_apr20_mar21
g `bs'500Xp_apr21_mar22 = `bs'500 * post_apr21_mar22
g `bs'500Xp_apr22_nov22 = `bs'500 * post_apr22_nov22

assert `bs'500Xpost  != . & `bs'500Xp_apr20_mar21  != . & `bs'500Xp_apr21_mar22 != . & `bs'500Xp_apr22_nov22 != .
}

ren *Amt* *At*
ren *_any *_a
ren *_only *_s
ren *_one *_o

local yd 500

local branch_type s o
local cra_suffix ///
HAtSBL TAtSBL QAtSBL ///
HAtSBLr TAtSBLr QAtSBLr ///
HNoSBL TNoSBL QNoSBL ///
HNoSBLr TNoSBLr QNoSBLr 

foreach suffix of local branch_type {
foreach bs of local cra_suffix{	
di "`bs'_`suffix'"
* Bank500X AtSBL (national)
g mega`yd'X`bs'_`suffix' = mega`yd' * `bs'_`suffix'
replace mega`yd'X`bs'_`suffix' = 0 if mega`yd' == 0
// assert mega`yd'X`bs'_`suffix' != . if d_nbrand

* Bank500X AtSBL (cty)
g mega`yd'X`bs'c_`suffix' = mega`yd' * `bs'c_`suffix'
replace mega`yd'X`bs'c_`suffix' = 0 if mega`yd' == 0
// assert mega`yd'X`bs'c_`suffix' != . if d_nbrand

}
}

foreach suffix of local branch_type {
foreach bs of local cra_suffix {
// di "`bs'"

* Bank500X AtSBL (national) X period
g mega`yd'X`bs'_`suffix'Xpost = mega`yd'X`bs'_`suffix' * post
g mega`yd'X`bs'_`suffix'Xp_apr20_mar21 = mega`yd'X`bs'_`suffix' * post_apr20_mar21
g mega`yd'X`bs'_`suffix'Xp_apr21_mar22 = mega`yd'X`bs'_`suffix' * post_apr21_mar22
g mega`yd'X`bs'_`suffix'Xp_apr22_nov22 = mega`yd'X`bs'_`suffix' * post_apr22_nov22
assert mega`yd'X`bs'_`suffix'Xpost == 0 & mega`yd'X`bs'_`suffix'Xp_apr20_mar21 == 0 & mega`yd'X`bs'_`suffix'Xp_apr21_mar22 == 0 & mega`yd'X`bs'_`suffix'Xp_apr22_nov22 == 0 if mega500 == 0

* Bank500X AtSBL (cty) X period
g mega`yd'X`bs'c_`suffix'Xpost = mega`yd'X`bs'c_`suffix' * post
g mega`yd'X`bs'c_`suffix'Xp_apr20_mar21 = mega`yd'X`bs'c_`suffix' * post_apr20_mar21
g mega`yd'X`bs'c_`suffix'Xp_apr21_mar22 = mega`yd'X`bs'c_`suffix' * post_apr21_mar22
g mega`yd'X`bs'c_`suffix'Xp_apr22_nov22 = mega`yd'X`bs'c_`suffix' * post_apr22_nov22
assert mega`yd'X`bs'c_`suffix'Xpost == 0 & mega`yd'X`bs'c_`suffix'Xp_apr20_mar21 == 0 & mega`yd'X`bs'c_`suffix'Xp_apr21_mar22 == 0 & mega`yd'X`bs'c_`suffix'Xp_apr22_nov22 == 0 if mega500 == 0

}
}

// HAtSBL100k_nb_b500tr_cty TAtSBL100k_nb_b500tr_cty QAtSBL100k_nb_b500tr_cty DAtSBL100k_nb_b500tr_cty 
// HAtSBL250k_nb_b500tr_cty TAtSBL250k_nb_b500tr_cty QAtSBL250k_nb_b500tr_cty DAtSBL250k_nb_b500tr_cty 
// HNoSBL100k_nb_b500tr_cty TNoSBL100k_nb_b500tr_cty QNoSBL100k_nb_b500tr_cty DNoSBL100k_nb_b500tr_cty 
// HNoSBL250k_nb_b500tr_cty TNoSBL250k_nb_b500tr_cty QNoSBL250k_nb_b500tr_cty DNoSBL250k_nb_b500tr_cty 

// HAtSBL100k_nb_b500tr TAtSBL100k_nb_b500tr QAtSBL100k_nb_b500tr DAtSBL100k_nb_b500tr 
// HAtSBL250k_nb_b500tr TAtSBL250k_nb_b500tr QAtSBL250k_nb_b500tr DAtSBL250k_nb_b500tr 
// HNoSBL100k_nb_b500tr TNoSBL100k_nb_b500tr QNoSBL100k_nb_b500tr DNoSBL100k_nb_b500tr 
// HNoSBL250k_nb_b500tr TNoSBL250k_nb_b500tr QNoSBL250k_nb_b500tr DNoSBL250k_nb_b500tr 

local suffix s
foreach yvar of varlist dln_visits_mar20 dln_ind500{
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

global output_file "tables/megaonly_nb_eventstudy_table_radius`yd'_ipw_trcra_`suffix'_int_`var'"


eststo clear
****************************************
* FULL SAMPLE REGRESSIONS
****************************************
* Only Megabank
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank"

* Only Megabank
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand & nmega500 <= 1, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "One Megabank"


************************************************************************************************
* BankX High AtSBL (national). 
* Enough variation in tract-level CRA loans within county. 
* Within county, there is a fair number of tracts and stores that rank in high SBL origination tracts and have bank branches.
* 393 tracts (top 10%), 1038 tracts (top 25%), 1398 tracts (top 33%) out of 8184 tracts with independent pharmacies and 4278 tracts with independent pharmacies and bank branches. 
* 536 stores (top 10%), 1282 stores (top 25%), 1707 stores (Top 33%) out of 9596 independent pharmacies and 5036 independent pharmacies with bank branches.
* Ranking nationally, we have fewer stores in high SBL origination tracts.
* When we rank nationally, we still have around 50% of all tracts that rank in high SBL origination tracts
* 107 tracts (top 10%), 486 tracts (top 25%), 779 tracts (top 33%) out of 8184 tracts with independent pharmacies and 4278 tracts with independent pharmacies and bank branches.
* 154 stores (top 10%), 641 stores (top 25%), 993 stores (top 33%) out of 9596 independent pharmacies and 5036 independnet pharmacies with bank branches.
************************************************************************************************


****************************************************************
* CASE 1: ONLY MEGABANK
****************************************************************
* Only Megabank - Top 50%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XHAtSBLr_`suffix'Xp_apr20_mar21 mega500XHAtSBLr_`suffix'Xp_apr21_mar22 mega500XHAtSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 50%"

* Only Megabank - Top 33%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XTAtSBLr_`suffix'Xp_apr20_mar21 mega500XTAtSBLr_`suffix'Xp_apr21_mar22 mega500XTAtSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 33%"

* Only Megabank - Top 25%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XQAtSBLr_`suffix'Xp_apr20_mar21 mega500XQAtSBLr_`suffix'Xp_apr21_mar22 mega500XQAtSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 25%"



************************************************************************************************
* BankX High AtSBL (within cty). 
************************************************************************************************

****************************************************************
* CASE 1: Only MEGABANK
****************************************************************
* Only Megabank - Top 50%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XHAtSBLrc_`suffix'Xp_apr20_mar21 mega500XHAtSBLrc_`suffix'Xp_apr21_mar22 mega500XHAtSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 50%"

* Only Megabank - Top 33%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XTAtSBLrc_`suffix'Xp_apr20_mar21 mega500XTAtSBLrc_`suffix'Xp_apr21_mar22 mega500XTAtSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 33%"

* Only Megabank - Top 25%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XQAtSBLrc_`suffix'Xp_apr20_mar21 mega500XQAtSBLrc_`suffix'Xp_apr21_mar22 mega500XQAtSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 25%"

************************************************************************************************
* BankX High NoSBL (national)
************************************************************************************************

****************************************************************
* CASE 1: Only MEGABANK
****************************************************************
* Only Megabank - Top 50%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XHNoSBLr_`suffix'Xp_apr20_mar21 mega500XHNoSBLr_`suffix'Xp_apr21_mar22 mega500XHNoSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 50%"

* Only Megabank - Top 33%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XTNoSBLr_`suffix'Xp_apr20_mar21 mega500XTNoSBLr_`suffix'Xp_apr21_mar22 mega500XTNoSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 33%"

* Only Megabank - Top 25%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XQNoSBLr_`suffix'Xp_apr20_mar21 mega500XQNoSBLr_`suffix'Xp_apr21_mar22 mega500XQNoSBLr_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 25%"

************************************************************************************************
* BankX High NoSBL (within cty)
************************************************************************************************

****************************************************************
* CASE 1: Only MEGABANK
****************************************************************
* Only Megabank - Top 50%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XHNoSBLrc_`suffix'Xp_apr20_mar21 mega500XHNoSBLrc_`suffix'Xp_apr21_mar22 mega500XHNoSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 50%"

* Only Megabank - Top 33%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XTNoSBLrc_`suffix'Xp_apr20_mar21 mega500XTNoSBLrc_`suffix'Xp_apr21_mar22 mega500XTNoSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 33%"

* Only Megabank - Top 25%
eststo: reghdfe `yvar' ///
mega500Xp_apr20_mar21 mega500Xp_apr21_mar22 mega500Xp_apr22_nov22 ///
mega500XQNoSBLrc_`suffix'Xp_apr20_mar21 mega500XQNoSBLrc_`suffix'Xp_apr21_mar22 mega500XQNoSBLrc_`suffix'Xp_apr22_nov22 ///
dln_brn500 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Only Megabank - Top 25%"

* Output table
esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist variable micro_type sample dv r2_a F, fmt(0 0 0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "CRA Variable" "Microarea type" "Sample" "DV" "Adj. R$^{2}$" "F Stat")) ///
title("Effect of Bank Size Proximity on Nonbrands by Bank CRA") nomtitle

}
