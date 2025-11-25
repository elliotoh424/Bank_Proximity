* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta", assert(1 3)
drop if d_brand == 1
assert _merge == 1 if nonmega500 >0 & cb500 > 0  
keep if _merge == 3
drop _merge

**************
* Post variables
g post = start >= date("2020-04-01","YMD")
g post_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g post_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g post_apr22_nov22 = start >= date("2022-04-01","YMD")

local banksize mega nonmega cb
foreach bs of local banksize{
g `bs'500Xpost = `bs'500 * post
g `bs'500Xp_apr20_mar21 = `bs'500 * post_apr20_mar21
g `bs'500Xp_apr21_mar22 = `bs'500 * post_apr21_mar22
g `bs'500Xp_apr22_nov22 = `bs'500 * post_apr22_nov22
}

* Generate interactions for different percentile thresholds with cb and noncb
local percentiles "H T Q D"
foreach v of local percentiles {


* Maximum
g mega500Xd_`v'deponly = mega500 * `v'depgr_only
replace mega500Xd_`v'deponly = 0 if mega500 == 0
assert mega500Xd_`v'deponly != .
g mega500Xd_`v'deponlyXp_apr20_mar21 = mega500Xd_`v'deponly * post_apr20_mar21
g mega500Xd_`v'deponlyXp_apr21_mar22 = mega500Xd_`v'deponly * post_apr21_mar22
g mega500Xd_`v'deponlyXp_apr22_nov22 = mega500Xd_`v'deponly * post_apr22_nov22
assert mega500Xd_`v'deponlyXp_apr20_mar21 != . & mega500Xd_`v'deponlyXp_apr21_mar22 != . & mega500Xd_`v'deponlyXp_apr22_nov22 != .

g mega500Xd_`v'depone = mega500 * `v'depgr_one
replace mega500Xd_`v'depone = 0 if mega500 == 0
assert mega500Xd_`v'depone != .
g mega500Xd_`v'deponeXp_apr20_mar21 = mega500Xd_`v'depone * post_apr20_mar21
g mega500Xd_`v'deponeXp_apr21_mar22 = mega500Xd_`v'depone * post_apr21_mar22
g mega500Xd_`v'deponeXp_apr22_nov22 = mega500Xd_`v'depone * post_apr22_nov22
assert mega500Xd_`v'deponeXp_apr20_mar21 != . & mega500Xd_`v'deponeXp_apr21_mar22 != . & mega500Xd_`v'deponeXp_apr22_nov22 != .

* Mean
g mega500Xp_`v'deponly = mega500 * mean_`v'depgr_only
replace mega500Xp_`v'deponly = 0 if mega500 == 0
assert mega500Xp_`v'deponly != .
g mega500Xp_`v'deponlyXp_apr20_mar21 = mega500Xp_`v'deponly * post_apr20_mar21
g mega500Xp_`v'deponlyXp_apr21_mar22 = mega500Xp_`v'deponly * post_apr21_mar22
g mega500Xp_`v'deponlyXp_apr22_nov22 = mega500Xp_`v'deponly * post_apr22_nov22
assert mega500Xp_`v'deponlyXp_apr20_mar21 != . & mega500Xp_`v'deponlyXp_apr21_mar22 != . & mega500Xp_`v'deponlyXp_apr22_nov22 != .
}


************************************************************************************************************
// foreach yd of numlist 200 500 1000 {
// foreach yd of numlist 200 500 {
// foreach yd of numlist 500 {

foreach yvar of varlist dln_visits_mar20 dln_ind500{

local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

global output_file "tables/megabank_depgrowth_nb_eventstudy_table_radius`yd'_ipw_int_`yvar'"


*****************************************************************************************************
*County FE
* Full sample cumulative brand gap
eststo clear

********************************************************************************
* First set: All nonbrand observations (d_nbrand)
********************************************************************************
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

**************************************************************************************
* Mega only
**************************************************************************************

* Indicator
* 50% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xd_HdeponlyXp_apr20_mar21 mega`yd'Xd_HdeponlyXp_apr21_mar22 mega`yd'Xd_HdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

* 33% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xd_TdeponlyXp_apr20_mar21 mega`yd'Xd_TdeponlyXp_apr21_mar22 mega`yd'Xd_TdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

* 25% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xd_QdeponlyXp_apr20_mar21 mega`yd'Xd_QdeponlyXp_apr21_mar22 mega`yd'Xd_QdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

* Percentage
* 50% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xp_HdeponlyXp_apr20_mar21 mega`yd'Xp_HdeponlyXp_apr21_mar22 mega`yd'Xp_HdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

* 33% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xp_TdeponlyXp_apr20_mar21 mega`yd'Xp_TdeponlyXp_apr21_mar22 mega`yd'Xp_TdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"

* 25% deposit growth
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
mega`yd'Xp_QdeponlyXp_apr20_mar21 mega`yd'Xp_QdeponlyXp_apr21_mar22 mega`yd'Xp_QdeponlyXp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Mega only"


//
// // coefplot , keep(mega0_`yd'Xp_apr22_nov22 nononmega0_`yd'Xp_apr22_nov22 cb0_`yd'Xp_apr22_nov22 ///
// // ) yline(0) vertical ytitle(dln(\#Visits Rel. to Mar. 2020)) title("Bank0_`yd'XPostPeriod") ylab(-0.05(0.05)0.1) ///
// // coeflabels(mega0_`yd'Xp_apr22_nov22 = `""mega0_`yd'X" "D(4/22-11/22)""' nononmega0_`yd'Xp_apr22_nov22 = `""nononmega0_`yd'X" "D(4/22-11/22)""' cb0_`yd'Xp_apr22_nov22 = `""CB0_`yd'X" "D(4/22-11/22)""') saving("large", replace)
// // graph combine "bank.gph" "large.gph"
// // graph export "figures/largebank`yd'Xpost_coefficients.png", replace
//

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of Mega Bank Proximity on Nonbrands by Deposit Growth") nomtitle 
}
