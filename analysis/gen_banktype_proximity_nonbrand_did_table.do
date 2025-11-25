* ============================================================================
* EVENT STUDY REGRESSION ANALYSIS: BANK PROXIMITY EFFECTS ON INDEPENDENT PHARMACIES
* ============================================================================
*
* DESCRIPTION: This do-file performs dynamic difference-in-difference regressions to estimate the causal effect of bank proximity (for community and non-community banks)on independent pharmacy foot traffic during the COVID-19 pandemic. The analysis uses inverse probability weighting (IPW) to address selection bias.
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
* dln(visits_it) = β1 cb500_i × Pd1_t + β2 cb500_i × Pd2_t + β3 cb500_i × Pd3_t 
*                 + γ1 noncb500_i × Pd1_t + γ2 noncb500_i × Pd2_t + γ3 noncb500_i × Pd3_t
*                 + County-Time FE + ε_it
* cb500: Community bank within 500 yards dummy
* noncb500: Non-Community bank within 500 yards dummy
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
* dln(ind_traffic_it) = β1 cb500_i × Pd1_t + β2 cb500_i × Pd2_t + β3 cb500_i × Pd3_t 
*                 + γ1 noncb500_i × Pd1_t + γ2 noncb500_i × Pd2_t + γ3 noncb500_i × Pd3_t
*                 + County-Time FE + ε_it
* cb500: Community bank within 500 yards dummy
* noncb500: Non-Community bank within 500 yards dummy
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

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_radius500_nonbrand_ipw.dta"
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
// local nbank_control "nbank`yd'"

g cb`yd'Xpost = cb`yd' * post
g noncb`yd'Xpost = noncb`yd' * post
g cb`yd'Xpd1 = cb`yd' * pd1
g noncb`yd'Xpd1 = noncb`yd' * pd1
g cb`yd'Xpd2 = cb`yd' * pd2
g noncb`yd'Xpd2 = noncb`yd' * pd2
g cb`yd'Xpd3 = cb`yd' * pd3
g noncb`yd'Xpd3 = noncb`yd' * pd3

local yvar "dln_visits_mar20"

global output_file "tables/banktype_nb_eventstudy_table_radius`yd'_ipw_int"

eststo clear

*****************************************************************************************************
* Regression for drug store traffic
*****************************************************************************************************

* No Controls
eststo: reghdfe `yvar' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Controls * Month dummies
eststo: reghdfe `yvar' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"

* Controls * Month dummies + Change in microarea foot traffic to brand retailers
eststo: reghdfe `yvar' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Plot Coefficient estimates
coefplot , keep( ///
noncb`yd'Xpd1 ///
noncb`yd'Xpd2 ///
noncb`yd'Xpd3 ) vertical yline(0) ytitle(dln(\#Visits Rel. to Mar. 2020)) title("Proximity to Non-Community Bank") /// 
coeflabels( ///
noncb`yd'Xpd1 = `""noncb`yd'X" "Recovery1""' ///
noncb`yd'Xpd2 = `""noncb`yd'X" "Recovery2""' ///
noncb`yd'Xpd3  = `""noncb`yd'X" "Recovery3""' ) saving("noncb", replace)

coefplot , keep( ///
cb`yd'Xpd1 ///
cb`yd'Xpd2 ///
cb`yd'Xpd3) vertical yline(0) ytitle(dln(\#Visits Rel. to Mar. 2020)) title("Proximity to Community Bank") /// 
coeflabels( ///
cb`yd'Xpd1 = `""cb`yd'X" "Recovery1""' ///
cb`yd'Xpd2 = `""cb`yd'X" "Recovery2""' ///
cb`yd'Xpd3  = `""cb`yd'X" "Recovery3""') saving("cb", replace)

graph combine "noncb.gph" "cb.gph"
graph export "figures/banktype`yd'Xpost_coefficients.png", replace

*****************************************************************************************************
* Regression for average traffic to independent retailers in microarea
*****************************************************************************************************

* No Controls
eststo: reghdfe dln_ind`yd' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"
qui estadd local micro_type "NonBrand Microarea"

* Controls * Month dummies
eststo: reghdfe dln_ind`yd' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"
qui estadd local micro_type "NonBrand Microarea"

* Controls * Month dummies + Change in microarea foot traffic to brand retailers
eststo: reghdfe dln_ind`yd' ///
cb`yd'Xpd1 noncb`yd'Xpd1 ///
cb`yd'Xpd2 noncb`yd'Xpd2 ///
cb`yd'Xpd3 noncb`yd'Xpd3 dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "dln_ind`yd'"
qui estadd local micro_type "NonBrand Microarea"

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of Bank Proximity on Nonbrands by Bank Type, Dynamic DID (Relative to Mar 2020)") nomtitle 
 }
