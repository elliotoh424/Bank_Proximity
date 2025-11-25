* ============================================================================
* EVENT STUDY REGRESSION ANALYSIS: BANK PROXIMITY EFFECTS ON INDEPENDENT PHARMACIES
* ============================================================================
*
* DESCRIPTION: This do-file performs dynamic difference-in-difference regressions to estimate the causal effect of bank proximity on independent pharmacy foot traffic during the COVID-19 pandemic. The analysis uses inverse probability weighting (IPW) to address selection bias.
*
* INPUT:
* - acs_drug_matched_month_radiusdef_forreg.dta (Regression-ready dataset)
* - bank_radius500_nonbrand_ipw.dta (IPW weights for independent pharmacies)
*
*
* REGRESSION SPECIFICATIONS:
*
* DEPENDENT VARIABLE 1: Pharmacy Foot Traffic (dln_visits_mar20)
* ----------------------------------------------------------------------------
* MODEL 1: Baseline Specification
* dln(visits_it) = β1 bank500_i × Pd1_t + β2 bank500_i × Pd2_t + β3 bank500_i × Pd3_t 
*                 + County-Time FE + ε_it
*
* MODEL 2: Include Time-Varying Controls
* Add γ(X_i × Time Dummies)
* X_i × Time Dummies = Time Dummies X (ndrug500, lnpoi500, all_500yd_chg_exc4, lmedage, lmedinc, pctuniv, pctmnty, lpdsty, lmedhome, pct0to10, pctpublic)
*
* MODEL 3: Include Time-Varying Controls + Brand Retailer Traffic Control
* Add γ(X_i × Time Dummies) and dln_brn500_it
* X_i × Time Dummies = Time Dummies X (ndrug500, lnpoi500, all_500yd_chg_exc4, lmedage, lmedinc, pctuniv, pctmnty, lpdsty, lmedhome, pct0to10, pctpublic)
* dln_brn500_it: Change in microarea traffic to brand retailers

* MODEL 4: Include Time-Varying Controls + Brand Retailer Traffic Control
* Add γ(X_i × Time Dummies) and dln_ind500_it
* X_i × Time Dummies = Time Dummies X (ndrug500, lnpoi500, all_500yd_chg_exc4, lmedage, lmedinc, pctuniv, pctmnty, lpdsty, lmedhome, pct0to10, pctpublic)
* dln_ind500_it: Change in microarea traffic to independent retailers
*

* DEPENDENT VARIABLE 2: Microarea Independent Retailer Traffic (dln_ind500)
* ----------------------------------------------------------------------------
* MODEL 5: Baseline Specification
* dln(ind_traffic_it) = β1 bank500_i × Pd1_t + β2 bank500_i × Pd2_t + β3 bank500_i × Pd3_t 
*                      + County-Time FE + ε_it
*
* MODEL 6: Include Time-Varying Controls
* Add γ(X_i × Time Dummies)
* X_i × Time Dummies = Time Dummies X (ndrug500, lnpoi500, all_500yd_chg_exc4, lmedage, lmedinc, pctuniv, pctmnty, lpdsty, lmedhome, pct0to10, pctpublic)
*
* MODEL 7: Include Time-Varying Controls + Brand Retailer Traffic Control
* Add γ(X_i × Time Dummies) and dln_brn500_it
* X_i × Time Dummies = Time Dummies X (ndrug500, lnpoi500, all_500yd_chg_exc4, lmedage, lmedinc, pctuniv, pctmnty, lpdsty, lmedhome, pct0to10, pctpublic)
* dln_brn500_it: Change in microarea traffic to brand retailers
*
* ============================================================================

cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

* Merge IPW weights
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/bank_radius500_nonbrand_ipw.dta"
assert _merge == 1 if ~d_nbrand
keep if _merge == 3
drop _merge


**************
* Time variables
g post = start >= date("2020-04-01","YMD")
g pd1 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g pd2 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g pd3 = start >= date("2022-04-01","YMD")
g indXpost = d_nbrand * post
g indXpd1 = d_nbrand * pd1
g indXpd2 = d_nbrand * pd2
g indXpd3 = d_nbrand * pd3


************************************************************************************************************
foreach yd of numlist 500 {

local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

g bank`yd'Xpost = bank`yd' * post
g bank`yd'Xpd1 = bank`yd' * pd1
g bank`yd'Xpd2 = bank`yd' * pd2
g bank`yd'Xpd3 = bank`yd' * pd3

local yvar "dln_visits_mar20"

global output_file "tables/bank_nb_eventstudy_table_radius`yd'_ipw_int"

eststo clear

*****************************************************************************************************
* Regression for drug store traffic
*****************************************************************************************************

* No Controls
eststo: reghdfe `yvar' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Controls * Month dummies
eststo: reghdfe `yvar' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Controls * Month dummies + Change in microarea foot traffic to brand retailers
eststo: reghdfe `yvar' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Controls * Month dummies + Change in microarea foot traffic to independent retailers
eststo: reghdfe `yvar' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 dln_ind`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "NonBrand Microarea"

*****************************************************************************************************
* Regression for average traffic to independent retailers in microarea
*****************************************************************************************************

* No Controls
eststo: reghdfe dln_ind`yd' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"

* Controls * Month dummies
eststo: reghdfe dln_ind`yd' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"

* Controls * Month dummies + Change in microarea foot traffic to brand retailers
eststo: reghdfe dln_ind`yd' ///
bank`yd'Xpd1 bank`yd'Xpd2 bank`yd'Xpd3 dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"


esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of Bank Proximity on Nonbrands IPW, Dynamic DID (Relative to Mar 2020)") nomtitle 
}
