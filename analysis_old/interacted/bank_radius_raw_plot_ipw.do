* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

g other500 = bank500 
replace other500 = 0 if mega500

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/bank_radius500_nonbrand_ipw.dta"
assert _merge == 1 if ~d_nbrand
keep if _merge == 3
drop _merge

foreach yd of numlist 500 {

* Bank vs. No Bank for brands and nonbrands
preserve

g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1
g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id bank`yd')

tw (connected dln_visits_mar20_nbrand month_id if bank`yd' == 1, lcolor(navy) msymbol(circle) lpattern(solid) ) (connected dln_visits_mar20_nbrand month_id if bank`yd' == 0, lcolor(maroon) msymbol(circle) lpattern(solid) connect(line) ), legend(row(2) order(1 "Bank within `yd' Yards" 2 "No Bank within `yd' Yards")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15 27 39, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Nearby Bank vs. No Nearby Bank)
graph export "figure/bank`yd'_gap_nonbrands_ipw.png", replace
restore

* Bank vs. No Bank by bank size for brands and nonbrands
preserve

g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

g dln_visits_mar20_mega = dln_visits_mar20
replace dln_visits_mar20_mega = . if d_nbrand == 0
replace dln_visits_mar20_mega = . if mega500 ==0
// replace dln_visits_mar20_mega = . if nonmega500 | cb500

g dln_visits_mar20_nonmega = dln_visits_mar20
replace dln_visits_mar20_nonmega = . if d_nbrand == 0
replace dln_visits_mar20_nonmega = . if nonmega500 ==0
// replace dln_visits_mar20_nonmega = . if mega500 | cb500

g dln_visits_mar20_cb = dln_visits_mar20
replace dln_visits_mar20_cb = . if d_nbrand == 0
replace dln_visits_mar20_cb = . if cb500 ==0
// replace dln_visits_mar20_cb = . if mega500 | nonmega500

collapse (mean) dln_visits_mar20_mega dln_visits_mar20_nonmega dln_visits_mar20_cb [aw = ipw], by(start month_id )

tw (connected dln_visits_mar20_mega month_id, msymbol(circle) lpattern(solid) ) ///
(connected dln_visits_mar20_nonmega month_id, msymbol(circle) lpattern(solid) ) ///
(connected dln_visits_mar20_cb month_id, msymbol(circle) lpattern(solid) ) ///
, legend(row(2) order(1 "MegaBank within `yd' Yards" 2 "Non-MegaBank within `yd' Yards" 3 "CB within `yd' Yards")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Nearby Bank by Bank Size)
graph export "figure/banksize`yd'_gap_nonbrands_ipw.png", replace
restore

}
