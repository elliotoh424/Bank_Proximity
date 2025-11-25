use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
keep if d_brand == 0 & mega500 == 1
keep tract 
duplicates drop
tempfile ind_tract
save `ind_tract', replace

use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract19.dta", clear
keep tract fips *SBL*

foreach var of varlist *SBL*{
	assert `var' != .
}

ren *_rev_0_1mil *r
merge 1:1 tract using `ind_tract'
drop if _merge ==2
g sample = 1 if _merge == 3
replace sample = 0 if sample == .
drop _merge

g geoid = tract
merge m:1 geoid using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/final/acs_5yr_2019_final.dta", keepusing(medinc)
drop if _merge == 2
drop _merge geoid

g ln_NoSBLr = log(NoSBLr)
tw (hist ln_NoSBLr, color(blue%30) ) (hist ln_NoSBLr if sample, color(red%30) ), ///
legend(label(1 "All Tracts") label(2 "Tracts with Megabank-Independent Pharmacy")) xtitle("log(No of Small Business Loans in 2019)") title("Small Business Loan Distribution ")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/NoSBLr_hist.png", replace

g ln_AmtSBLr = log(AmtSBLr)
tw (hist ln_AmtSBLr, color(blue%30) ) (hist ln_AmtSBLr if sample, color(red%30) ), ///
legend(label(1 "All Tracts") label(2 "Tracts with Megabank-Independent Pharmacy")) xtitle("log(Amt of Small Business Loans in 2019)") title("Small Business Loan Distribution ")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/AmtSBLr_hist.png", replace

g ln_medinc = log(medinc)
tw (hist ln_medinc, color(blue%30) ) (hist ln_medinc if sample, color(red%30) ), ///
legend(label(1 "All Tracts") label(2 "Tracts with Megabank-Independent Pharmacy")) xtitle("log(Median Income in 2019)") title("Median Income Distribution")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/medinc_hist.png", replace

su AmtSBLr NoSBLr, d
su AmtSBLr NoSBLr if sample, d

g neg_medinc = -medinc
* Create indicators for top percentiles unconditionally across all variables
foreach var of varlist NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBLr AmtSBLr medinc neg_medinc{

	di "`var'"
	count if `var' == .

    * Get unconditional percentiles
    quietly summarize `var', detail
    
    * Create unconditional indicators for different thresholds
    generate H`var'_nat_uncond = (`var' > `r(p50)')
    generate Q`var'_nat_uncond = (`var' > `r(p75)')
    generate D`var'_nat_uncond = (`var' > `r(p90)')
    
    * For top 33%, we need to calculate the 67th percentile explicitly
    quietly _pctile `var', p(67)
    local p67 = r(r1)
    generate T`var'_nat_uncond = (`var' > `p67')
	
    * Label the variables
    label variable H`var'_nat_uncond "Top 50% of `var' unconditionally"
    label variable T`var'_nat_uncond "Top 33% of `var' unconditionally"
    label variable Q`var'_nat_uncond "Top 25% of `var' unconditionally"
    label variable D`var'_nat_uncond "Top 10% of `var' unconditionally"
}

* Create indicators for top percentiles within each county
foreach var of varlist NoSBL100k AmtSBL100k NoSBL250k AmtSBL250k NoSBL AmtSBL NoSBLr AmtSBLr medinc neg_medinc{
    * Create within-county percentile variables
    bysort fips: egen p50_county_`var' = pctile(`var'), p(50)
    bysort fips: egen p67_county_`var' = pctile(`var'), p(67)
    bysort fips: egen p75_county_`var' = pctile(`var'), p(75)
    bysort fips: egen p90_county_`var' = pctile(`var'), p(90)
    
    * Create county-specific indicators
    generate H`var'_cty_uncond = (`var' > p50_county_`var')
    generate T`var'_cty_uncond = (`var' > p67_county_`var')
    generate Q`var'_cty_uncond = (`var' > p75_county_`var')
    generate D`var'_cty_uncond = (`var' > p90_county_`var')
    
    * Label the variables
    label variable H`var'_cty_uncond "Top 50% of `var' within county"
    label variable T`var'_cty_uncond "Top 33% of `var' within county"
    label variable Q`var'_cty_uncond "Top 25% of `var' within county"
    label variable D`var'_cty_uncond "Top 10% of `var' within county"
    
    * Clean up temporary percentile variables
    drop p50_county_`var' p67_county_`var' p75_county_`var' p90_county_`var'
}
ren Hneg_* Hn*
ren Tneg_* Tn*
ren Qneg_* Qn*
drop No* Amt* medinc neg_medinc

keep tract *NoSBL* *AmtSBL* *medinc*
ren *_nat_uncond *u
ren *_cty_uncond *cu
unique tract

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/all_tract_cra19_rank.dta", replace

