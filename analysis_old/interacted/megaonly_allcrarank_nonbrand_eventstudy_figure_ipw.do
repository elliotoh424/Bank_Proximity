
* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

local vars TAmtSBLru QAmtSBLru
foreach var of local vars{
local yd 500
local baseperiod 15 /* March 2020 */

* Nonbrand
use "tables/megaonly_allcrarank_`var'_ipw_int.dta", clear
gen month_id = real(substr(parm, -2,.))
replace month_id = real(substr(parm, -1,.)) if month_id == .
replace month_id = . if parm == "dln_brn500"
assert ~regexm("_Imon", parm) if month_id == .
drop if month_id == .
keep if strpos(parm, "_ImonXmegaa")>0
drop parm
tempfile gap
isid month_id
save `gap', replace


use `gap', clear

set obs `=_N+1'
replace month_id = `baseperiod' if missing(month_id)
foreach v of varlist estimate_* {
	replace `v' = 0 if month_id == `baseperiod'
}
sort month_id


*estimate 1: gap no control
*estimate 2: gap microarea control
*estimate 3: gap tract control

* Bank gap only
twoway 	///
(connected estimate_1 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_1 max95_1 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Top 33% SBL-Bot 66% SBL Gap (No Controls)" 2 "95% CI")) yline(0) ///
title("HSBLXMega Bank Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/megaonly_`var'rank_noctrl_radius`yd'_cty_ipw_int.png", replace

twoway 	///
(connected estimate_2 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_2 max95_2 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Top 33% SBL-Bot 66% SBL Gap (Microarea, Tract Controls)" 2 "95% CI")) yline(0) ///
title("HSBLXMega Bank Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/megaonly_`var'rank_ctrl1_radius`yd'_cty_ipw_int.png", replace

twoway 	///
(connected estimate_3 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_3 max95_3 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Top 33% SBL-Bot 66% SBL Gap (All Controls)" 2 "95% CI" )) yline(0) ///
title("HSBLXMega Bank Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) 
graph export "figures/megaonly_`var'rank_ctrl2_radius`yd'_cty_ipw_int.png", replace

}
