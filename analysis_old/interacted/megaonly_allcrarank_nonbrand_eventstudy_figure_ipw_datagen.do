* Generate Advan-ACS matched tract-level data for all Advan stores
// cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

local vars QAmtSBLru TAmtSBLru HAmtSBLru 
foreach var of local vars {

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
// use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
use "/homes/nber/elliotoh/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

* All stores located in tract with CRA loans and SBL loans.
assert AmtSBL_all19 != .
assert AmtSBL_rev_0_1mil_all19 != .

foreach var1 of varlist *SBL* {
	replace `var1' = 0 if mega500 == 0
}

// merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta", assert(1 3)
merge m:1 placekey using "/homes/nber/elliotoh/megaonly_radius500_ipw.dta", assert(1 3)
assert _merge == 1 if nonmega500 >0 & cb500 > 0
drop _merge


************************************************************************************************************
// foreach num of numlist 5{

local num 5
local yd `num'00
local baseperiod 15 /* March 2020 */
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
local nbank_control "i.month_id#c.nbank`yd'"

g mega`yd'`var' = mega`yd' * `var'

xi i.month_id|mega`yd' i.month_id|mega`yd'`var', noomit
drop *`baseperiod'


local yvar "dln_visits_mar20"

global output_file "tables/megaonly_allcrarank_nonbrand_eventstudy_figure_radius`yd'_`var'_ipw"

*****************************************************************************************************
* Full sample cumulative brand gap

eststo clear

eststo: reghdfe `yvar' _ImonXmega_*  _ImonXmegaa* [aw= ipw], absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap1
parmest, saving(`gap1') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXmega_*  _ImonXmegaa* [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap2
parmest, saving(`gap2') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXmega_*  _ImonXmegaa* dln_brn`yd' [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap3
parmest, saving(`gap3') format(parm estimate min95 max95)


esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Since Apr 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(a2) ) se(par(( )) fmt(a1))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl sample r2_a F, fmt(0 0 0 0 0 0 3 1) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Sample" "Adj. R$^{2}$" "F Stat")) ///
 title("Event Study on Bank Proximity") nomtitle 

 * Full sample brand-nonbrand gap
preserve
use `gap1', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) (estimate_1 min95_1 max95_1)
qui: merge 1:1 parm using `gap2', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_2 min95_2 max95_2)
qui: merge 1:1 parm using `gap3', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) (estimate_3 min95_3 max95_3)
save "tables/megaonly_allcrarank_`var'_ipw_int.dta", replace


}
		


