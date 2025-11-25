* Obtain nonbrand stores close to bank.
local years "00 05 10 13"
foreach year of local years{

use placekey d_brand tract mega_500yd nonmega_500yd cb_500yd using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef.dta", clear
g cty = substr(tract, 1,5)
duplicates drop
duplicates list placekey

merge m:1 cty using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba`year'19_cty.dta"
drop if _merge == 2
foreach var of varlist LoanAmt`year'19 NoLoan`year'19 est`year'19 NoLoan_est`year'19 LoanAmt_est`year'19 {
	replace `var' = 0 if _merge == 1
}
// keep if d_brand == 0 & mega500 == 1 & nonmega500 == 0 & cb500 == 0
// unique cty if d_brand == 0
// unique cty 

keep if d_brand == 0
drop if nonmega_500yd == 1 | cb_500yd == 1
tab mega_500yd d_brand
keep cty LoanAmt`year'19 NoLoan`year'19 est`year'19 NoLoan_est`year'19 LoanAmt_est`year'19
duplicates drop
unique cty
ren LoanAmt* AmtSBA*
ren NoLoan* NoSBA*
// ren *_est* *est*

* Assuming you have a dataset with multiple variables and a county identifier
foreach var of varlist AmtSBA`year'19 NoSBA`year'19 NoSBA_est`year'19 AmtSBA_est`year'19{
su `var', d
_pctile `var', p(67)
di r(r1)
}

* Create indicators for top percentiles unconditionally across all variables
foreach var of varlist AmtSBA`year'19 NoSBA`year'19 NoSBA_est`year'19 AmtSBA_est`year'19{

    * Get unconditional percentiles
    summarize `var', detail
    local p50 `r(p50)'
    local p75 `r(p75)'
	
    * Create unconditional indicators for different thresholds
    generate H`var'_mn = (`var' >= `r(p50)')
	replace H`var'_mn  = . if `var' == .
    generate Q`var'_mn = (`var' >= `r(p75)')
	replace Q`var'_mn  = . if `var' == .
    generate D`var'_mn = (`var' >= `r(p90)')
	replace D`var'_mn  = . if `var' == .

    * For top 33%, we need to calculate the 67th percentile explicitly
    quietly _pctile `var', p(67)
    local p67 = r(r1)
    generate T`var'_mn = (`var' >= `p67')
	replace T`var'_mn  = . if `var' == .
	
    * Label the variables
    label variable H`var'_mn "Top 50% of `var' unconditionally"
    label variable T`var'_mn "Top 33% of `var' unconditionally"
    label variable Q`var'_mn "Top 25% of `var' unconditionally"
    label variable D`var'_mn "Top 10% of `var' unconditionally"
	di "`var' `p50' `p67' `p75'"
}


keep cty *AmtSBA* *NoSBA*
drop AmtSBA`year'19 NoSBA`year'19 NoSBA_est`year'19 AmtSBA_est`year'19
unique cty

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/pharmacy_sba_intensity`year'19_mega.dta", replace
}
