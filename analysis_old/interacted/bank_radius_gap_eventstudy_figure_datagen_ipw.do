* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */

merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/bank_radius500_gap_ipw.dta"
assert _merge == 3
keep if _merge == 3
drop _merge
g comb_wt = ipw * mnvisits_wt

// keep if start >= date("2020-01-01","YMD")

************************************************************************************************************
foreach num of numlist 5{
local yd `num'00
local baseperiod 15 /* March 2020 */
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

g bank`yd'nb = bank`yd' * d_nbrand

xi i.month_id|d_nbrand i.month_id|bank`yd' i.month_id|bank`yd'nb, noomit
drop *`baseperiod'

local yvar "dln_visits_mar20"

global output_file "tables/bank_gap_eventstudy_figure_radius`yd'_ipw_int"


*****************************************************************************************************
* Full sample cumulative brand gap
eststo clear

eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap1
parmest, saving(`gap1') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap2
parmest, saving(`gap2') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* dln_brn`yd' [aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap3
parmest, saving(`gap3') format(parm estimate min95 max95)


*****************************************************************************************************
* No bank cumulative brand gap
eststo: reghdfe `yvar' _ImonXd_nb_* [aw= ipw] if bank`yd' == 0, absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap4
parmest, saving(`gap4') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXd_nb_* [aw= ipw] if bank`yd' == 0, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap5
parmest, saving(`gap5') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXd_nb_* dln_brn`yd' [aw= ipw] if bank`yd' == 0, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap6
parmest, saving(`gap6') format(parm estimate min95 max95)

****************************
*** Zip FE
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap7
parmest, saving(`gap7') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* [aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap8
parmest, saving(`gap8') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXbanka* _ImonXd_nb_* _ImonXbank_* dln_brn`yd' [aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
tempfile gap9
parmest, saving(`gap9') format(parm estimate min95 max95)


*****************************************************************************************************
* No bank cumulative brand gap
eststo: reghdfe `yvar' _ImonXd_nb_* [aw= ipw] if bank`yd' == 0, absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap10
parmest, saving(`gap10') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXd_nb_* [aw= ipw] if bank`yd' == 0, absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap11
parmest, saving(`gap11') format(parm estimate min95 max95)

* Microarea and tract controls
eststo: reghdfe `yvar' _ImonXd_nb_* dln_brn`yd' [aw= ipw] if bank`yd' == 0, absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "No Bank in `yd' Yards"
tempfile gap12
parmest, saving(`gap12') format(parm estimate min95 max95)

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Since Apr 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(a2) ) se(par(( )) fmt(a1))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl sample r2_a F, fmt(0 0 0 0 0 0 3 1) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Sample" "Adj. R$^{2}$" "F Stat")) ///
 title("Event Study on Bank Proximity") nomtitle 


* Full sample brand-nonbrand gap
preserve
use `gap1', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) (estimate_1 min95_1 max95_1)
qui: merge 1:1 parm using `gap2', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_2 min95_2 max95_2)
qui: merge 1:1 parm using `gap3', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) (estimate_3 min95_3 max95_3)
save "tables/full_gap_`yd'yd_cty_eventstudy_ipw_int.dta", replace

* No bank sample brand-nonbrand gap
use `gap4', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) ( estimate_4 min95_4 max95_4)
qui: merge 1:1 parm using `gap5', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_5 min95_5 max95_5)
qui: merge 1:1 parm using `gap6', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_6 min95_6 max95_6)
save "tables/nobank_`yd'yd_cty_gap_eventstudy_ipw_int.dta", replace

**** Zip FE
use `gap7', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) (estimate_7 min95_7 max95_7)
qui: merge 1:1 parm using `gap8', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_8 min95_8 max95_8)
qui: merge 1:1 parm using `gap9', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) (estimate_9 min95_9 max95_9)
save "tables/full_gap_`yd'yd_zip_eventstudy_ipw_int.dta", replace

* No bank sample brand-nonbrand gap
use `gap10', clear
qui: keep parm estimate min95 max95
qui: rename (estimate min95 max95) ( estimate_10 min95_10 max95_10)
qui: merge 1:1 parm using `gap11', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_11 min95_11 max95_11)
qui: merge 1:1 parm using `gap12', keepusing(estimate min95 max95) nogen
qui: rename (estimate min95 max95) ( estimate_12 min95_12 max95_12)
save "tables/nobank_`yd'yd_zip_gap_eventstudy_ipw_int.dta", replace

restore
}

// keep if strpos(parm, "_ImonXd_nb_")>0
// gen month_id = real(substr(parm, -2,.))
// replace month_id = real(substr(parm, -1,.)) if month_id == .
// assert month_id != .
// drop parm
// set obs `=_N+1'
// replace month_id = `baseperiod' if missing(month_id)
// foreach v of varlist estimate_* {
// 	replace `v' = 0 if month_id == `baseperiod'
// }
// sort month_id
//
//
// twoway 	///
// (connected estimate_gap1 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_gap1 max95_gap1 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "No Controls" 2 "95% CI")) yline(0) ///
// title("Monthly Brand-Independent Gap") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on NonBrandXMonth)
// graph export "figures/gap_noctrl_radius`yd'.png", replace
//
// twoway 	///
// (connected estimate_gap2 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_gap2 max95_gap2 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "Microarea, Tract Controls" 2 "95% CI")) yline(0)  ///
// title("Monthly Brand-Independent Gap") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on NonBrandXMonth)
// graph export "figures/gap_ctrl_radius`yd'.png", replace
//
// * Bank on Brand-nonbrand gap
// use `gap1', clear
// qui: keep parm estimate min95 max95
// qui: rename (estimate min95 max95) (estimate_gap1 min95_gap1 max95_gap1)
// qui: merge 1:1 parm using `gap2', keepusing(estimate min95 max95) nogen
// qui: rename (estimate min95 max95) ( estimate_gap2 min95_gap2 max95_gap2)
//
// keep if strpos(parm, "_ImonXbanka")>0
// gen month_id = real(substr(parm, -2,.))
// replace month_id = real(substr(parm, -1,.)) if month_id == .
// assert month_id != .
// drop parm
// set obs `=_N+1'
// replace month_id = `baseperiod' if missing(month_id)
// foreach v of varlist estimate_* {
// 	replace `v' = 0 if month_id == `baseperiod'
// }
// sort month_id
//
//
// twoway 	///
// (connected estimate_gap1 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_gap1 max95_gap1 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "No Controls" 2 "95% CI")) yline(0)  ///
// title("Bank Proximity on Brand-Independent Gap") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on Bank`yd'XNonBrandXMonth) ylab(-0.1(0.05)0.15)
// graph export "figures/bank_gap_noctrl_radius`yd'.png", replace
//
// twoway 	///
// (connected estimate_gap2 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_gap2 max95_gap2 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "Microarea, Tract Controls" 2 "95% CI")) yline(0)  ///
// title("Bank Proximity on Brand-Independent Gap") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on Bank`yd'XNonBrandXMonth) ylab(-0.1(0.05)0.15)
// graph export "figures/bank_gap_ctrl_radius`yd'.png", replace
//
// * Bank on Nonbrands
// use `gap3', clear
// qui: keep parm estimate min95 max95
// qui: rename (estimate min95 max95) (estimate_bank3 min95_bank3 max95_bank3)
// qui: merge 1:1 parm using `gap4', keepusing(estimate min95 max95) nogen
// qui: rename (estimate min95 max95) ( estimate_bank4 min95_bank4 max95_bank4)
// save "tables/nb_bank_`yd'yd_eventstudy.dta", replace
//
// keep if strpos(parm, "_ImonXbank_")>0
// gen month_id = real(substr(parm, -2,.))
// replace month_id = real(substr(parm, -1,.)) if month_id == .
// assert month_id != .
// drop parm
// set obs `=_N+1'
// replace month_id = `baseperiod' if missing(month_id)
// foreach v of varlist estimate_* {
// 	replace `v' = 0 if month_id == `baseperiod'
// }
// sort month_id
//
// twoway 	///
// (connected estimate_bank3 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_bank3 max95_bank3 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "No Controls" 2 "95% CI")) yline(0)  ///
// title("Bank Proximity on Nonbrand Foot Traffic") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on BankXNonBrandXMonth) ylab(-0.1(0.05)0.15)
// graph export "figures/bank_nonbrand_noctrl_radius`yd'.png", replace
//
// twoway 	///
// (connected estimate_bank4 month_id , msize(small) lcolor(midblue) mfc(white))  ///
// (rcap min95_bank4 max95_bank4 month_id if month_id~=`baseperiod' , lc(midblue) lw(medthin)) ///
// , xline(15.2, lstyle(dot)) scheme(aspen) legend(row(2) order(1 "Microarea, Tract Controls" 2 "95% CI")) yline(0)  ///
// title("Bank Proximity on Nonbrand Foot Traffic") ///
// xlab( ///
// 4 "4/19" 8 "8/19" 12 "12/19" 16 "4/20" 20 "8/20" 24 "12/20" 28 "4/21" 32 "8/21" 36 "12/21" 40 "4/22" 44 "8/22" 48 "12/22") xtitle(Date) ytitle(Coefficient on BankXNonBrandXMonth) ylab(-0.1(0.05)0.15)
// graph export "figures/bank_nonbrand_ctrl_radius`yd'.png", replace






* Color and style settings from Choi, Kuziemko, Washington, Wright AER (2024)
// return local es_sct1_sty =	"msize(small) mlc(`c1') mfc(white)"
// return local es_sct2_sty =	"msize(small) m(diamond) mlw(medthin) mlc(`c2') mfc(white)"
// return local es_sct3_sty =	"msize(small) m(+) mlc(`c3') mlw(medthin) mfc(white) "
// return local es_sct4_sty =	"msize(medsmall) mlc(`c4') mlw(medthin) mfc(white)"
// return local es_sct5_sty =  "msize(medsmall) mlw(medthin) mlc(`c5') mfc(white)"
// /* Confidence intervals */
// return local es_ci1_sty =	"lc(`c1') lw(medthin)"
//
//
// local c3 "black"
// local c2 "orange" 
// local c1 "midblue" 
// local c4 "green"
// local c5 "maroon" 

		


