* Mega bank only tracts
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged", clear
keep if year == 2019
keep if ind_pharmacy == 1
unique tract year

* Sort data by year and AmtSBLr
ren AmtSBLr AmtSBLr19
sort year AmtSBLr19

foreach var of varlist ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedage lmedinc pct_univ pct_mnty lpdsty lmedhome pct_0to10 pct_public{
	drop if `var' == .
}

// pca ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedage lmedinc pct_univ lpdsty lmedhome pct_0to10 pct_public
// predict pc1 pc2 pc3, score
// cor pc1 pc2 pc3 ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedage lmedinc pct_univ lpdsty lmedhome pct_0to10 pct_public
// save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged_pca.dta", replace

* Calculate percentile thresholds within each year
by year: egen p67_threshold = pctile(AmtSBLr19), p(67)
by year: egen p75_threshold = pctile(AmtSBLr19), p(75)

* Create dummy variables based on thresholds
gen top33_dummy = (AmtSBLr19 >= p67_threshold) if !missing(AmtSBLr19)
gen top25_dummy = (AmtSBLr19 >= p75_threshold) if !missing(AmtSBLr19)

* Clean up temporary variables
drop p67_threshold p75_threshold

// keep if mega == 1
egen cty_id = group(cty)
// egen tract_id = group(tract)

binscatter AmtSBLr19 ndrug_tract, name(g1, replace)
binscatter AmtSBLr19 lnpoi_tract, name(g2, replace) ytitle("Small Business Loan Orig.") ylab(0(1)4) xlab(2(1)7)
binscatter AmtSBLr19 all_tract_chg_exc4, name(g3, replace)
binscatter AmtSBLr19 lmedage, name(g4, replace)
binscatter AmtSBLr19 lmedinc, name(g5, replace) ytitle("Small Business Loan Orig.") ylab(0(0.5)2.5) xlab(10(0.5)12.5)
binscatter AmtSBLr19 pct_univ, name(g6, replace)
binscatter AmtSBLr19 pct_mnty, name(g7, replace) ytitle("Small Business Loan Orig.") ylab(0(0.5)1.5) xlab(0(0.2)1)
binscatter AmtSBLr19 lpdsty, name(g8, replace) ytitle("Small Business Loan Orig.")  ylab(0(0.5)2) xlab(0(2)12)
binscatter AmtSBLr19 lmedhome, name(g9, replace)
binscatter AmtSBLr19 pct_0to10, name(g10, replace)
binscatter AmtSBLr19 pct_public, name(g11, replace)
// binscatter AmtSBLr19 nmega, name(g12, replace)
binscatter AmtSBLr19 pct_black, name(g12, replace)

graph combine g2 g5 g7 g8 , cols(2) imargin(zero) title("All Tracts with CRA loans (Ind Pharmacy)")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figure/all19_indphar_binscatter_raw_select.png", replace


graph combine g1 g2 g3 g4 g5 g6 g7 g8 g9 g10 g11 g12, cols(4) imargin(zero) title("All Tracts with CRA loans (Ind Pharmacy)")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figure/all19_indphar_binscatter_raw_full.png", replace
