* Mega bank only tracts
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged", clear
keep if year == 2019
keep if pct_mega == 1
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

graph combine g2 g5 g7 g8 , cols(2) imargin(zero) title("Tracts with Mega Banks Only")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figure/megaonly19_nobank_binscatter_select.png", replace

graph combine g1 g2 g3 g4 g5 g6 g7 g8 g9 g10 g11 g12, cols(4) imargin(zero) title("Tracts with Mega Banks Only")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figure/megaonly19_nobank_binscatter_raw_full.png", replace

//
// * Define the list of variables to loop through
// // local varlist "ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedage lmedinc pct_univ pct_mnty lpdsty lmedhome pct_0to10 pct_public"
// local varlist "ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedinc pct_univ pct_mnty lpdsty lmedhome pct_0to10 pct_public"
//
//
// binsregselect AmtSBLr19 pct_mnty ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedinc pct_univ lpdsty lmedhome pct_0to10 pct_public
//
// binsreg AmtSBLr19 pct_mnty ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedinc pct_univ lpdsty lmedhome pct_0to10 pct_public, ///
//         xtitle("`var'") ytitle("AmtSBLr") 
// graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/pct_mnty_ctrl_binsreg.png", replace
//
// binsreg AmtSBLr19 pct_mnty i.cty_id, ///
//         xtitle("`var'") ytitle("AmtSBLr") 
//		
// 		binspos(es)  ///
// 		ylab(0(5)25)
//
//
// mpg weight foreign
//
// * Loop through each variable and create individual graphs
// foreach var of local varlist {
//     local controls ""
//     foreach control_var of local varlist {
//         if "`control_var'" != "`var'" {
//             local controls "`controls' `control_var'"
//         }
//     }
//    
//     display "Processing `var'..."
//    
// 	if "`var'" == "lnpoi_tract"{
//     binsreg AmtSBLr19 `var' `controls', nbins(10) ///
//         xtitle("`var'") ytitle("AmtSBLr") binspos(es)  ///
// 		ylab(0(5)25) ///
//         savedata(binsreg_`var') replace ///
//         name(plot_`var', replace)
// 	}
//	
// 	else{
//     binsreg AmtSBLr19 `var' `controls', nbins(10) ///
//         xtitle("`var'") ytitle("AmtSBLr") binspos(es)  ///
// 		ylab(0(1)3) ///
//         savedata(binsreg_`var') replace ///
//         name(plot_`var', replace)
// 	}
//	
//	
// 		}
//
// * Combine all graphs into one figure
// graph combine plot_lnpoi_tract plot_all_tract_chg_exc4 ///
//               plot_lmedinc plot_pct_univ ///
//               plot_pct_mnty plot_lpdsty plot_lmedhome ///
//               plot_pct_0to10 plot_pct_public, ///
//     rows(4) cols(3)
//
// * Save the combined graph
// graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures_new/combined_binsreg_subplots.png", replace width(1200) height(900)
//
//
// //
// // * UW with cty-month FE
// // binscatter AmtSBLr19 ndrug_tract, absorb(cty_id) name(g1, replace)
// // binscatter AmtSBLr19 lnpoi_tract, absorb(cty_id) name(g2, replace)
// // binscatter AmtSBLr19 all_tract_chg_exc4, absorb(cty_id) name(g3, replace)
// // binscatter AmtSBLr19 lmedage, absorb(cty_id) name(g4, replace)
// // binscatter AmtSBLr19 lmedinc, absorb(cty_id) name(g5, replace)
// // binscatter AmtSBLr19 pct_univ, absorb(cty_id) name(g6, replace)
// // binscatter AmtSBLr19 pct_mnty, absorb(cty_id) name(g7, replace)
// // binscatter AmtSBLr19 lpdsty, absorb(cty_id) name(g8, replace)
// // binscatter AmtSBLr19 lmedhome, absorb(cty_id) name(g9, replace)
// // binscatter AmtSBLr19 pct_0to10, absorb(cty_id) name(g10, replace)
// // binscatter AmtSBLr19 pct_public, absorb(cty_id) name(g11, replace)
// // // binscatter AmtSBLr19 nmega, absorb(cty_id) name(g12, replace)
// // binscatter AmtSBLr19 pct_black, absorb(cty_id) name(g12, replace)
// //
// // graph combine g1 g2 g3 g4 g5 g6 g7 g8 g9 g10 g11 g12, cols(5) imargin(zero) 
// // graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/megaonly19_binscatter_residualized_uw.png", replace
//
// global output_file "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/tables/megaonly_nobank_AmtSBLr19_tract_char_regression"
//
// eststo clear
// foreach yvar of varlist AmtSBLr19 top33_dummy top25_dummy{
// *****************************************************************************************************
// * Full sample cumulative brand gap
// local yd 500
// local drug_control "ndrug_tract"
// local microarea_control "lnpoi_tract all_tract_chg_exc4"
// local microarea_control_alt "all_tract_chg_exc4"
// local tract_control "lmedage lmedinc pct_univ pct_mnty lpdsty lmedhome pct_0to10 pct_public"
// local tract_control_alt "lmedage lmedinc pct_univ pct_black lpdsty lmedhome pct_0to10 pct_public"
// // local nbank_control "nbank`yd'"
//
// eststo: reghdfe `yvar' `drug_control' `microarea_control' `tract_control', noabsorb vce(cluster tract_id)
// qui estadd local fe1 ""
// qui estadd local fe2 "UW"
// qui estadd local sample "Mega only vs no bank"
// qui estadd local variable "`yvar'"
//
// eststo: reghdfe `yvar' `drug_control' `microarea_control_alt' `tract_control', noabsorb vce(cluster tract_id)
// qui estadd local fe1 ""
// qui estadd local fe2 "UW"
// qui estadd local sample "Mega only vs no bank"
// qui estadd local variable "`yvar'"
//
// // eststo: reghdfe `yvar' nmega `drug_control' `microarea_control' `tract_control', noabsorb vce(cluster tract_id)
// // qui estadd local fe1 ""
// // qui estadd local fe2 "UW"
// // qui estadd local sample "Mega only vs no bank"
// // qui estadd local variable "`yvar'"
//
// eststo: reghdfe `yvar' `drug_control' `microarea_control' `tract_control', absorb(cty_id) vce(cluster tract_id)
// qui estadd local fe1 "Cty FE"
// qui estadd local fe2 "UW"
// qui estadd local sample "Mega only vs no bank"
// qui estadd local variable "`yvar'"
//
// eststo: reghdfe `yvar' `drug_control' `microarea_control' `tract_control_alt', noabsorb vce(cluster tract_id)
// qui estadd local fe1 ""
// qui estadd local fe2 "UW"
// qui estadd local sample "Mega only vs no bank"
// qui estadd local variable "`yvar'"
//
// eststo: reghdfe `yvar' `drug_control' `microarea_control' `tract_control' if mega, noabsorb vce(cluster tract_id)
// qui estadd local fe1 ""
// qui estadd local fe2 "UW"
// qui estadd local sample "Mega only"
// qui estadd local variable "`yvar'"
//
// // eststo: reghdfe `yvar' nmega `drug_control' `microarea_control' `tract_control', absorb(cty_id) vce(cluster tract_id)
// // qui estadd local fe1 "Cty FE"
// // qui estadd local fe2 "UW"
// // qui estadd local sample "Mega only vs no bank"
// // qui estadd local variable "`yvar'"
//
// }
//
// esttab using "$output_file.csv", replace ///
// collabels("",lhs("DV:AmtSBLr19")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
//  stats(N fe1 fe2 sample variable r2_a F, fmt(0 0 0 0 0 2 2) labels("Nobs" "FE" "Weights" "Sample" "Variable" "Adj. R$^{2}$" "F Stat")) ///
//  title("Loan Origination and Characteristics") nomtitle 
//
//
