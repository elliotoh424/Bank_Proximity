* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
drop if nonmega500 | cb500

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/megaonly_radius500_ipw.dta"
assert _merge == 1 if ~d_nbrand
keep if _merge == 3
drop _merge

local yd 500
* Megabank only vs. No Bank for brands and nonbrands
preserve

g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1
g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id mega`yd')

tw (connected dln_visits_mar20_nbrand month_id if mega`yd' == 1, lcolor(navy) msymbol(circle) lpattern(solid) ) (connected dln_visits_mar20_nbrand month_id if mega`yd' == 0, lcolor(maroon) msymbol(circle) lpattern(solid) connect(line) ), legend(row(2) order(1 "Mega Bank within `yd' Yards" 2 "No Bank within `yd' Yards")) ///
ylab(-0.6(0.2)0.4) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents by Proximity to Mega Bank Only)
graph export "figure/mega`yd'_gap_nonbrands_ipw.png", replace
restore


* High No. SBL 2019 Megabank only vs. No Bank for brands and nonbrands (top 33%)
preserve
keep if mega`yd' == 1

g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1
g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id TAmtSBLru)

tw (connected dln_visits_mar20_nbrand month_id if TAmtSBLru == 1, lcolor(navy) msymbol(circle) mcolor(navy) lpattern(longdash) ) (connected dln_visits_mar20_nbrand month_id if TAmtSBLru == 0, lcolor(midblue) msymbol(diamond) mcolor(midblue) lpattern(longdash) ), legend(row(2) order(1 "Top 33% Tracts in Small Business Loan Origination" 2 "Bottom 67% Tracts in Small Business Loan Origination")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Mega Bank Branch Within `yd' Yards)
graph export "figure/mega`yd'_Tamtsblp_nonbrands_ipw.png", replace
restore


* High No. SBL 2019 Megabank only vs. No Bank for brands and nonbrands (top 25%)
preserve
keep if mega`yd' == 1

g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1
g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id QAmtSBLru)

tw (connected dln_visits_mar20_nbrand month_id if QAmtSBLru == 1, lcolor(navy) msymbol(circle) mcolor(navy) lpattern(longdash) ) (connected dln_visits_mar20_nbrand month_id if QAmtSBLru == 0, lcolor(midblue) msymbol(diamond) mcolor(midblue) lpattern(longdash) ), legend(row(2) order(1 "Top 25% Tracts in Small Business Loan Origination" 2 "Bottom 75% Tracts in Small Business Loan Origination")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Mega Bank Branch Within `yd' Yards)
graph export "figure/mega`yd'_Qamtsblp_nonbrands_ipw.png", replace
restore



* High No. SBL 2019 Megabank only vs. No Bank for brands and nonbrands (top 25%)
preserve
keep if mega`yd' == 1

g dln_visits_mar20_brand = dln_visits_mar20
replace dln_visits_mar20_brand = . if d_nbrand == 1
g dln_visits_mar20_nbrand = dln_visits_mar20
replace dln_visits_mar20_nbrand = . if d_nbrand == 0

collapse (mean) dln_visits_mar20_brand dln_visits_mar20_nbrand dln_visits_mar20 [aw = ipw], by(start month_id QNoSBLrc_only)

tw (connected dln_visits_mar20_nbrand month_id if QNoSBLrc_only == 1, lcolor(navy) msymbol(circle) mcolor(navy) lpattern(longdash) ) (connected dln_visits_mar20_nbrand month_id if QNoSBLrc_only == 0, lcolor(midblue) msymbol(diamond) mcolor(midblue) lpattern(longdash) ), legend(row(2) order(1 "Top 25% Tracts in No. Small Business Loans" 2 "Bottom 75% Tracts in No. Small Business Loans")) ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xline(15, lstyle(dot)) xtitle(Date) ytitle(Cumulative Change in Foot Traffic) title(Independents with Mega Bank Branch Within `yd' Yards)
graph export "figure/mega`yd'_nosblp75_nonbrands_ipw.png", replace
restore

