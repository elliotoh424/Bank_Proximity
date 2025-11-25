* ============================================================================
* EVENT STUDY ANALYSIS: BANK PROXIMITY EFFECTS ON BRAND-INDEPENDENT PHARMACY GAP
* ============================================================================
*
* DESCRIPTION: This do-file performs event study regressions to estimate the  differential effect of bank proximity on the foot traffic gap between independent pharmacies and brand pharmacies during COVID-19.
* The analysis uses triple-difference specifications and generates coefficient plots for visualization.
*
* INPUT:
* - acs_drug_matched_month_radiusdef_forreg.dta
* - bank_radius500_gap_ipw.dta (IPW weights for brand-indepedent gap analysis)
*
* KEY SPECIFICATION:
* Y_it = Σ_t[β_t × Bank_i × Independent_i × Month_t] + FE + Controls + ε_it
* ============================================================================

cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

* Merge IPW weights
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/bank_radius500_gap_ipw.dta"
assert _merge == 3
keep if _merge == 3
drop _merge

* ============================================================================
* MAIN EVENT STUDY LOOP - TRIPLE DIFFERENCE SPECIFICATIONS
* ============================================================================

local yd 500
local baseperiod 15 /* March 2020 */
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

* Create triple interaction term
g bank`yd'nb = bank`yd' * d_nbrand

* Generate saturated interactions (bank × independent × month)
xi i.month_id|d_nbrand i.month_id|bank`yd' i.month_id|bank`yd'nb, noomit
drop *`baseperiod'

local yvar "dln_visits_mar20"

global output_file "tables/bank_gap_eventstudy_figure_radius`yd'_ipw_int"


eststo clear

* ============================================================================
* COUNTY-MONTH FIXED EFFECTS SPECIFICATIONS
* ============================================================================
* Model 1: Basic triple difference
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap1
parmest, saving(`gap1') format(parm estimate min95 max95)

* Model 2: With microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap2
parmest, saving(`gap2') format(parm estimate min95 max95)

* Model 3: With brand retailer traffic control
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* dln_brn`yd' [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap3
parmest, saving(`gap3') format(parm estimate min95 max95)


* ============================================================================
* ZIP-MONTH FIXED EFFECTS SPECIFICATIONS (Robustness Check)
* ============================================================================

* Model 4: Basic triple difference with zip FE
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap4
parmest, saving(`gap4') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap5
parmest, saving(`gap5') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* dln_brn`yd' [aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap6
parmest, saving(`gap6') format(parm estimate min95 max95)

* ============================================================================
* COMBINE AND SAVE ESTIMATION RESULTS
* ============================================================================

* Combine county FE results
preserve
use `gap1', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) (estimate_1 min95_1 max95_1)
qui: merge 1:1 parm using `gap2', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_2 min95_2 max95_2)
qui: merge 1:1 parm using `gap3', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) (estimate_3 min95_3 max95_3)
save "tables/full_gap_`yd'yd_cty_eventstudy_ipw_int.dta", replace

* Combine zip FE results
use `gap4', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) (estimate_4 min95_4 max95_4)
qui: merge 1:1 parm using `gap5', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_5 min95_5 max95_5)
qui: merge 1:1 parm using `gap6', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) (estimate_6 min95_6 max95_6)
save "tables/full_gap_`yd'yd_zip_eventstudy_ipw_int.dta", replace


restore
}

* ============================================================================
* CREATE EVENT STUDY PLOTS
* ============================================================================

* Prepare data for plotting
local yd 500
local baseperiod 15 /* March 2020 */

* Extract month_id from parameter names
use "tables/full_gap_`yd'yd_cty_eventstudy_ipw_int.dta", clear
gen month_id = real(substr(parm, -2,.))
replace month_id = real(substr(parm, -1,.)) if month_id == .
replace month_id = . if parm == "dln_brn500"
assert ~regexm("_Imon", parm) if month_id == .
drop if month_id == .

* Rename variables for clarity
foreach num of numlist 1 2 3 {
ren *_`num' *_nb`num'
}

* Keep only bank-indepedent interaction terms
keep if strpos(parm, "_ImonXbanka")>0

* Add baseline period (month 15 = March 2020)
set obs `=_N+1'
replace month_id = `baseperiod' if missing(month_id)
foreach v of varlist estimate_* {
    replace `v' = 0 if month_id == `baseperiod'  // Normalize baseline to zero
}
sort month_id

* Add baseline period (month 15 = March 2020)
set obs `=_N+1'
replace month_id = `baseperiod' if missing(month_id)
foreach v of varlist estimate_* {
	replace `v' = 0 if month_id == `baseperiod'
}
sort month_id

* ============================================================================
* GENERATE COEFFICIENT PLOTS
* ============================================================================

* Plot 1: No controls
twoway 	///
(connected estimate_nb1 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb1 max95_nb1 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (No Controls)" 2 "95% CI")) yline(0) ///
title("Bank Proximity on Brand-Independent Gap") ///
xlab( ///
16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/bank_gap_noctrl_radius`yd'_cty_ipw_int.png", replace

* Plot 2: Microarea and tract controls
twoway 	///
(connected estimate_nb2 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb2 max95_nb2 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (Microarea, Tract Controls)" 2 "95% CI")) yline(0) ///
title("Bank Proximity on Brand-Independent Gap") ///
xlab( ///
16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/bank_gap_ctrl1_radius`yd'_cty_ipw_int.png", replace

* Plot 3: All controls including brand traffic
twoway 	///
(connected estimate_nb3 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb3 max95_nb3 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (All Controls)" 2 "95% CI" )) yline(0) ///
title("Bank Proximity on Brand-Independent Gap") ///
xlab( ///
16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/bank_gap_ctrl2_radius`yd'_cty_ipw_int.png", replace
