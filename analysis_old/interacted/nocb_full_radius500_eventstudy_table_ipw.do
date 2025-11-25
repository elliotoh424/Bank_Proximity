* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
keep if ~cb500

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/nocb_radius500_ipw.dta"
assert _merge == 1 if ~d_nbrand 
keep if _merge == 3
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
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

g noncb`yd'Xpost = noncb`yd' * post
g noncb`yd'Xp_apr20_mar21 = noncb`yd' * post_apr20_mar21
g noncb`yd'Xp_apr21_mar22 = noncb`yd' * post_apr21_mar22
g noncb`yd'Xp_apr22_nov22 = noncb`yd' * post_apr22_nov22

local yvar "dln_visits_mar20"

global output_file "tables/nocb_full_eventstudy_table_radius`yd'_ipw_int"

*****************************************************************************************************
*County FE
eststo clear

* Nonbrand post in periods - Original DV
eststo: reghdfe `yvar' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 [aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "`yvar'"

eststo: reghdfe `yvar' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 [aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "`yvar'"

eststo: reghdfe `yvar' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 dln_brn500 [aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "`yvar'"
qui estadd local micro_type "Brand Microarea"

* Nonbrand post in periods - Using dln_ind`yd' as DV
eststo: reghdfe dln_ind`yd' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 [aw= ipw] if d_nbrand, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 [aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' noncb`yd'Xp_apr20_mar21 noncb`yd'Xp_apr21_mar22 noncb`yd'Xp_apr22_nov22 dln_brn500 [aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local noncb_dist "`yd' Yards"
qui estadd local sample "Nonbrands"
qui estadd local dv "dln_ind`yd'"
qui estadd local micro_type "Brand Microarea"

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of NonCB Proximity on NonBrands, Full Sample Event Study (Relative to Mar 2020)") nomtitle 

