* ============================================================================
* VISUALIZATION: INDEPENDENT PHARMACY FOOT TRAJECTORIES BY BANK PROXIMITY
* ============================================================================
*
* DESCRIPTION: This do-file generates comparative line plots showing the evolution of foot traffic for independent pharmacies based on proximity to banking institutions. The analysis uses inverse probability weighting (IPW) to account for selection bias.
*
* INPUT:
* - regression_pharmacy_sample.dta (Main regression sample)
* - bank_radius500_nonbrand_ipw.dta (IPW weights for independent pharmacies)
*
* OUTPUT:
* - figure/bank500_gap_nonbrands_ipw.png (Comparative trajectory plot)
*
* ============================================================================

cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/regression_pharmacy_sample.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores */

g other500 = bank500 
replace other500 = 0 if mega500

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_radius500_nonbrand_ipw.dta"
assert _merge == 1 if ~d_nbrand
keep if _merge == 3
drop _merge

* Plot average foot traffic for independent pharmacies near banks vs. not near banks 
foreach yd of numlist 500 {

preserve
g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id bank`yd')

tw ( ///
connected dln_visits_mar20_nbrand month_id if bank`yd' == 1, ///
lcolor(navy) msymbol(circle) lpattern(solid) ) ///
( ///
connected dln_visits_mar20_nbrand month_id if bank`yd' == 0, ///
lcolor(maroon) msymbol(circle) lpattern(solid) connect(line) ), ///
legend(row(2) order(1 "Bank within `yd' Yards" 2 "No Bank within `yd' Yards")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15 27 39, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Nearby Bank vs. No Nearby Bank)
graph export "figure/bank`yd'_gap_nonbrands_ipw.png", replace
restore
}
