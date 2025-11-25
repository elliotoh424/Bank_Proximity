* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"
// cd "/home/nber/elliotoh/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_independents_drug.dta", clear
// use "/home/nber/elliotoh/adv_independents_drug.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

merge m:1 placekey_micro using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/ind_megaonly_radius500_ipw.dta"
// merge m:1 placekey using "/home/nber/elliotoh/megaonly_radius500_ipw.dta"
assert _merge == 1 if ~d_nbrand
keep if _merge == 3
drop _merge

g other500 = bank500 
replace other500 = 0 if mega500

ren *_per_* *_*
ren *namt_est *nest
ren *namt_n9 *nest9
ren *namt_n19 *nest19
ren *namt_n49 *nest49


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

local yd 500

local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

g bank`yd'Xpost = bank`yd' * post
g bank`yd'Xp_apr20_mar21 = bank`yd' * post_apr20_mar21
g bank`yd'Xp_apr21_mar22 = bank`yd' * post_apr21_mar22
g bank`yd'Xp_apr22_nov22 = bank`yd' * post_apr22_nov22

local banksize "mega nonmega cb other"
foreach bs of local banksize{
g `bs'`yd'Xpost = `bs'`yd' * post
g `bs'`yd'Xp_apr20_mar21 = `bs'`yd' * post_apr20_mar21
g `bs'`yd'Xp_apr21_mar22 = `bs'`yd' * post_apr21_mar22
g `bs'`yd'Xp_apr22_nov22 = `bs'`yd' * post_apr22_nov22

foreach var of varlist Qnest Qnest9 Qnest19 Qnest49 Tnest Tnest9 Tnest19 Tnest49{
g `bs'`yd'X`var'Xpost = `bs'`yd' * `var' * post
g `bs'`yd'X`var'Xp_apr20_mar21 = `bs'`yd' * `var' * post_apr20_mar21
g `bs'`yd'X`var'Xp_apr21_mar22 = `bs'`yd' * `var' * post_apr21_mar22
g `bs'`yd'X`var'Xp_apr22_nov22 = `bs'`yd' * `var' * post_apr22_nov22
}
}


local yvar "dln_visits_micro_mar20"

global output_file "tables/ind_megaonly_nb_eventstudy_table_radius`yd'_ipw_int"
eststo clear

* Basic specification with county-month FE
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Adding microarea and tract controls
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

**************************
* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XTnest9Xp_apr20_mar21 mega`yd'XTnest9Xp_apr21_mar22 mega`yd'XTnest9Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XQnest9Xp_apr20_mar21 mega`yd'XQnest9Xp_apr21_mar22 mega`yd'XQnest9Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XTnest19Xp_apr20_mar21 mega`yd'XTnest19Xp_apr21_mar22 mega`yd'XTnest19Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XQnest19Xp_apr20_mar21 mega`yd'XQnest19Xp_apr21_mar22 mega`yd'XQnest19Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XTnest49Xp_apr20_mar21 mega`yd'XTnest49Xp_apr21_mar22 mega`yd'XTnest49Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Brand microarea specification
eststo: reghdfe `yvar' ///
mega`yd'XQnest49Xp_apr20_mar21 mega`yd'XQnest49Xp_apr21_mar22 mega`yd'XQnest49Xp_apr22_nov22 ///
mega`yd'Xp_apr20_mar21 mega`yd'Xp_apr21_mar22 mega`yd'Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

//
// // coefplot , keep(mega`yd'Xp_apr22_nov22 nononmega`yd'Xp_apr22_nov22 cb`yd'Xp_apr22_nov22 ///
// // ) yline(0) vertical ytitle(dln(\#Visits Rel. to Mar. 2020)) title("Bank`yd'XPostPeriod") ylab(-0.05(0.05)0.1) ///
// // coeflabels(mega`yd'Xp_apr22_nov22 = `""mega`yd'X" "D(4/22-11/22)""' nononmega`yd'Xp_apr22_nov22 = `""nononmega`yd'X" "D(4/22-11/22)""' cb`yd'Xp_apr22_nov22 = `""CB`yd'X" "D(4/22-11/22)""') saving("large", replace)
// // graph combine "bank.gph" "large.gph"
// // graph export "figures/largebank`yd'Xpost_coefficients.png", replace
//


esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of Bank Proximity on Nonbrands by Bank Size, Dynamic DID (Relative to Mar 2020)") nomtitle 
// }
