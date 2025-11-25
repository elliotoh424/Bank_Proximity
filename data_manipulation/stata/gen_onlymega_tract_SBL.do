* Obtain nonbrand stores close to bank.
use placekey d_brand tract using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_stores_final.dta", clear
duplicates drop
duplicates list placekey

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_dist_banksize.dta", assert(1 3) keepusing(*mega_500yd *cb_500yd)
ren *_500yd *500
foreach var of varlist *500{
	replace `var' = 0 if _merge == 1
	assert `var' != .
}
drop _merge

joinby tract using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract19.dta", unmatched(both)
drop if _merge == 2
foreach var of varlist NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBL_rev_0_1mil AmtSBL_rev_0_1mil{
	replace `var' = 0 if _merge == 1
	assert `var' != .
}

keep placekey fips tract d_brand mega500 nmega500 nonmega500 cb500 NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBL_rev_0_1mil AmtSBL_rev_0_1mil
ren *_rev_0_1mil *rev

// g AmtSBL100k_br = AmtSBL100k/trnbranch
// g AmtSBL250k_br = AmtSBL250k/trnbranch
// g AmtSBL_br = AmtSBL/trnbranch
// g AmtSBLrev_br = AmtSBLrev/trnbranch
//
// g AmtSBL100k_dep = AmtSBL100k/trdeposit
// g AmtSBL250k_dep = AmtSBL250k/trdeposit
// g AmtSBL_dep = AmtSBL/trdeposit
// g AmtSBLrev_dep = AmtSBLrev/trdeposit

g geoid = tract
merge m:1 geoid using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/final/acs_5yr_2019_final.dta", keepusing(medinc)
drop if _merge == 2
drop _merge geoid

* No missing variables
foreach var of varlist d_brand mega500 nmega500 nonmega500 cb500 NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBLrev AmtSBLrev {
	di "`var'"
	count if `var' == .
}

* Restrict to nonbrand stores near mega banks only
keep if d_brand == 0 & mega500 == 1 & nonmega500 == 0 & cb500 == 0
drop placekey d_brand mega500 nonmega500 cb500 nmega500
duplicates drop
unique tract

su NoSBL* AmtSBL* , d /* All tracts with independent pharmacy and nearby bank has CRA loans. */
// ren NoSBL* NoSBL*_nb_b500
// ren AmtSBL* AmtSBL*_nb_b500


* Assuming you have a dataset with multiple variables and a county identifier

* Create indicators for top percentiles unconditionally across all variables
foreach var of varlist AmtSBL100k AmtSBL250k AmtSBL AmtSBLrev NoSBL100k NoSBL250k NoSBL NoSBLrev medinc {

    * Get unconditional percentiles
    quietly summarize `var', detail
    
	assert `var' != .
    * Create unconditional indicators for different thresholds
    generate H`var'_nat = (`var' > `r(p50)')
    generate Q`var'_nat = (`var' > `r(p75)')
    generate D`var'_nat = (`var' > `r(p90)')
    
    * For top 33%, we need to calculate the 67th percentile explicitly
    quietly _pctile `var', p(67)
    local p67 = r(r1)
    generate T`var'_nat = (`var' > `p67')
	
    * Label the variables
    label variable H`var'_nat "Top 50% of `var' unconditionally"
    label variable T`var'_nat "Top 33% of `var' unconditionally"
    label variable Q`var'_nat "Top 25% of `var' unconditionally"
    label variable D`var'_nat "Top 10% of `var' unconditionally"
}

* Create indicators for top percentiles within each county
foreach var of varlist AmtSBL100k AmtSBL250k AmtSBL AmtSBLrev NoSBL100k NoSBL250k NoSBL NoSBLrev {
    * Create within-county percentile variables
    bysort fips: egen p50_county_`var' = pctile(`var'), p(50)
    bysort fips: egen p67_county_`var' = pctile(`var'), p(67)
    bysort fips: egen p75_county_`var' = pctile(`var'), p(75)
    bysort fips: egen p90_county_`var' = pctile(`var'), p(90)
    
    * Create county-specific indicators
    generate H`var'_cty = (`var' > p50_county_`var')
    generate T`var'_cty = (`var' > p67_county_`var')
    generate Q`var'_cty = (`var' > p75_county_`var')
    generate D`var'_cty = (`var' > p90_county_`var')
    
    * Label the variables
    label variable H`var'_cty "Top 50% of `var' within county"
    label variable T`var'_cty "Top 33% of `var' within county"
    label variable Q`var'_cty "Top 25% of `var' within county"
    label variable D`var'_cty "Top 10% of `var' within county"
    
    * Clean up temporary percentile variables
    drop p50_county_`var' p67_county_`var' p75_county_`var' p90_county_`var'
}
drop No* Amt*

keep tract *NoSBL* *AmtSBL*
ren *_nat *
ren *_cty *c
ren *rev* *r*
ren * *_only
ren tract_only tract
unique tract

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/nonbrand_onlymega500_tract_cra19.dta", replace

