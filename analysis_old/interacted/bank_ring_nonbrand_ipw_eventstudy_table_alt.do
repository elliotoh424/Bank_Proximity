* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_radius500_nonbrand_ipw.dta"
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

* Redefine bank rings for 200 and 500 yards.
drop bank500ring
g bank200ring = bank200
g bank500ring = (bank300exc | bank400exc | bank500exc)

************************************************************************************************************
local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

* Generate interaction variables for both distance ranges
g bank0_200Xpost = bank200ring * post
g bank200_500Xpost = bank500ring * post
g bank500_1000Xpost = bank1000ring * post
g bank0_200Xp_apr20_mar21 = bank200ring * post_apr20_mar21
g bank200_500Xp_apr20_mar21 = bank500ring * post_apr20_mar21
g bank500_1000Xp_apr20_mar21 = bank1000ring * post_apr20_mar21
g bank0_200Xp_apr21_mar22 = bank200ring * post_apr21_mar22
g bank200_500Xp_apr21_mar22 = bank500ring * post_apr21_mar22
g bank500_1000Xp_apr21_mar22 = bank1000ring * post_apr21_mar22
g bank0_200Xp_apr22_nov22 = bank200ring * post_apr22_nov22
g bank200_500Xp_apr22_nov22 = bank500ring * post_apr22_nov22
g bank500_1000Xp_apr22_nov22 = bank1000ring * post_apr22_nov22


local yvar "dln_visits_mar20"

global output_file "tables/bank_nb_eventstudy_table_radius_altrings_ipw_int"

*****************************************************************************************************
*County FE
* Full sample cumulative brand gap
eststo clear
* Brand microarea
eststo: reghdfe `yvar' ///
bank0_200Xp_apr20_mar21 bank0_200Xp_apr21_mar22 bank0_200Xp_apr22_nov22 ///
bank200_500Xp_apr20_mar21 bank200_500Xp_apr21_mar22 bank200_500Xp_apr22_nov22 ///
bank500_1000Xp_apr20_mar21 bank500_1000Xp_apr21_mar22 bank500_1000Xp_apr22_nov22 ///
dln_brn`yd' ///
[aw= ipw] if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "0-500 500-1000 Yards"
qui estadd local sample "`yvar'"
qui estadd local micro_type "Brand Microarea"

coefplot , keep(bank0_200Xp_apr22_nov22 bank200_500Xp_apr22_nov22 bank500_1000Xp_apr22_nov22 ///
) yline(0) vertical ytitle(dln(\#Visits Rel. to Mar. 2020)) title("BankXRecovery3 by Distance") ylab(-0.05(0.05)0.1) ///
coeflabels(bank0_200Xp_apr22_nov22 = `"0-200 yards"' bank200_500Xp_apr22_nov22 = `"200-500 yards"' bank500_1000Xp_apr22_nov22 = `"500-1000 yards"') saving("bank", replace)
graph export "figure/bankringXrecovery3_alt.png", replace



//
// esttab using "$output_file.csv", replace ///
// collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
//  stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type r2_a F, fmt(0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea type" "Adj. R$^{2}$" "F Stat")) ///
//  title("Effect of Bank Proximity on Nonbrands IPW, Dynamic DID (Relative to Mar 2020)") nomtitle 
//
