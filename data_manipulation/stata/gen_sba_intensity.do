* Obtain nonbrand stores close to bank.
local years "00 05 10 13"
foreach year of local years{

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba`year'19_ind_cty.dta", clear
// keep if d_brand == 0 & mega500 == 1 & nonmega500 == 0 & cb500 == 0
// unique cty if d_brand == 0
// unique cty 
keep cty LoanAmt`year'19 NoLoan`year'19 iest`year'19 NoLoan_iest`year'19 LoanAmt_iest`year'19
duplicates drop
unique cty
ren LoanAmt* AmtSBA*
ren NoLoan* NoSBA*
// ren *_est* *est*
g fips = cty

merge 1:1 fips using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba`year'19_all_cty.dta"
replace cty = fips if _merge == 2
assert fips != ""
drop _merge
ren smest_* sest_*

g AmtSBA_sest`year'19 = AmtSBA`year'19 / sest_`year'19
g NoSBA_sest`year'19 = NoSBA`year'19 / sest_`year'19

* Assuming you have a dataset with multiple variables and a county identifier
foreach var of varlist AmtSBA`year'19 AmtSBA_iest`year'19 AmtSBA_sest`year'19 NoSBA`year'19 NoSBA_iest`year' NoSBA_sest`year'19 {
_pctile `var', p(50)
di r(r1)
_pctile `var', p(67)
di r(r1)
}

* Create indicators for top percentiles unconditionally across all variables
foreach var of varlist AmtSBA`year'19 AmtSBA_iest`year'19 AmtSBA_sest`year'19 NoSBA`year'19 NoSBA_iest`year'19 NoSBA_sest`year'19 {

    * Get unconditional percentiles
    summarize `var', detail
    local p50 `r(p50)'
    local p75 `r(p75)'
	
    * Create unconditional indicators for different thresholds
    generate H`var'_nat = (`var' >= `r(p50)')
	replace H`var'_nat  = . if `var' == .
    generate Q`var'_nat = (`var' >= `r(p75)')
	replace Q`var'_nat  = . if `var' == .
    generate D`var'_nat = (`var' >= `r(p90)')
	replace D`var'_nat  = . if `var' == .

    * For top 33%, we need to calculate the 67th percentile explicitly
    quietly _pctile `var', p(67)
    local p67 = r(r1)
    generate T`var'_nat = (`var' >= `p67')
	replace T`var'_nat  = . if `var' == .
	
    * Label the variables
    label variable H`var'_nat "Top 50% of `var' unconditionally"
    label variable T`var'_nat "Top 33% of `var' unconditionally"
    label variable Q`var'_nat "Top 25% of `var' unconditionally"
    label variable D`var'_nat "Top 10% of `var' unconditionally"
	di "`var' `p50' `p67' `p75'"
}


keep cty *AmtSBA* *NoSBA* *iest*
ren *_nat *
unique cty

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/pharmacy_sba_intensity`year'19.dta", replace
}
