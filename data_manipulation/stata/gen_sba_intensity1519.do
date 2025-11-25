* Obtain nonbrand stores close to bank.
use placekey d_brand tract using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/adv_drug_stores_final.dta", clear
g cty = substr(tract, 1,5)
duplicates drop
duplicates list placekey

merge m:1 cty using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/sba1519_cty.dta"
drop if _merge == 2
foreach var of varlist LoanAmt1519 NoLoan1519 est1519 NoLoan_est1519 LoanAmt_est1519 {
	replace `var' = 0 if _merge == 1
}
// keep if d_brand == 0 & mega500 == 1 & nonmega500 == 0 & cb500 == 0
// unique cty if d_brand == 0
// unique cty 
keep cty LoanAmt1519 NoLoan1519 est1519 NoLoan_est1519 LoanAmt_est1519
duplicates drop
unique cty
ren LoanAmt* AmtSBA*
ren NoLoan* NoSBA*
// ren *_est* *est*

* Assuming you have a dataset with multiple variables and a county identifier
foreach var of varlist AmtSBA1519 NoSBA1519 NoSBA_est1519 AmtSBA_est1519{
su `var', d
_pctile `var', p(67)
di r(r1)
}

* Create indicators for top percentiles unconditionally across all variables
foreach var of varlist AmtSBA1519 NoSBA1519 NoSBA_est1519 AmtSBA_est1519{

    * Get unconditional percentiles
    summarize `var', detail
    
    * Create unconditional indicators for different thresholds
    generate H`var'_nat = (`var' > `r(p50)')
	replace H`var'_nat  = . if `var' == .
    generate Q`var'_nat = (`var' > `r(p75)')
	replace Q`var'_nat  = . if `var' == .
    generate D`var'_nat = (`var' > `r(p90)')
	replace D`var'_nat  = . if `var' == .

    * For top 33%, we need to calculate the 67th percentile explicitly
    quietly _pctile `var', p(67)
    local p67 = r(r1)
    generate T`var'_nat = (`var' > `p67')
	replace T`var'_nat  = . if `var' == .
	
    * Label the variables
    label variable H`var'_nat "Top 50% of `var' unconditionally"
    label variable T`var'_nat "Top 33% of `var' unconditionally"
    label variable Q`var'_nat "Top 25% of `var' unconditionally"
    label variable D`var'_nat "Top 10% of `var' unconditionally"
}


keep cty *AmtSBA* *NoSBA*
ren *_nat *
unique cty

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/pharmacy_sba_intensity1519.dta", replace

