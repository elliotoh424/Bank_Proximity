
* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"
foreach macro in mega nonmega cb {
    if "`macro'" == "mega" {
        local cond "mega_"
        local banksize "Mega Bank"
    }
    else if "`macro'" == "nonmega" {
        local cond "nonm_"
        local banksize "Non-Mega Bank"
    }
    else if "`macro'" == "cb" {
        local cond "cb50_"
        local banksize "Community Bank"
    }
}

local yd 500
local baseperiod 15 /* March 2020 */

* Nonbrand
use "tables/full_banksize_nonbrand_`yd'yd_cty_eventstudy_ipw.dta", clear
gen month_id = real(substr(parm, -2,.))
replace month_id = real(substr(parm, -1,.)) if month_id == .
replace month_id = . if parm == "dln_brn500"
assert parm == "_cons" | parm == "dln_brn500" if month_id == .
drop if month_id == .
foreach num of numlist 1 2 3 {
ren *_`num' *_nb`num'
}
keep if strpos(parm, "`cond'")>0
di "`cond'"
drop parm
tempfile nonbrand
isid month_id
save `nonbrand', replace



use `nonbrand', clear

set obs `=_N+1'
replace month_id = `baseperiod' if missing(month_id)
foreach v of varlist estimate_* {
	replace `v' = 0 if month_id == `baseperiod'
}
sort month_id


*estimate 1: nonbrand no control
*estimate 2: nonbrand microarea control
*estimate 3: nonbrand tract control

* Bank nonbrand only
twoway 	///
(connected estimate_nb1 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb1 max95_nb1 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (No Controls)" 2 "95% CI")) yline(0) ///
title("`banksize' Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ///
ylab( -0.15(0.05)0.15)
graph export "figures/`macro'_nonbrand_noctrl_radius`yd'_cty_ipw.png", replace

twoway 	///
(connected estimate_nb2 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb2 max95_nb2 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (Microarea, Tract Controls)" 2 "95% CI")) yline(0) ///
title("`banksize' Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ///
ylab( -0.15(0.05)0.15)
graph export "figures/`macro'_nonbrand_ctrl1_radius`yd'_cty_ipw.png", replace

twoway 	///
(connected estimate_nb3 month_id, msize(small) lcolor(midblue) mfc(white))  ///
(rcap min95_nb3 max95_nb3 month_id if month_id~=`baseperiod', msize(small) lcolor(midblue) mfc(white)) ///
, xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order( 1 "Bank-No Bank Gap (All Controls)" 2 "95% CI" )) yline(0) ///
title("`banksize' Proximity on Independent Pharmacies") ///
xlab( ///
4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ///
ylab( -0.15(0.05)0.15)
graph export "figures/`macro'_nonbrand_ctrl2_radius`yd'_cty_ipw.png", replace
}

