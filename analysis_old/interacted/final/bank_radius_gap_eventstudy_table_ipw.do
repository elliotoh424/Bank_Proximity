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

**************
* Post variables
g post = start >= date("2020-04-01","YMD")
g post_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")
g post_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")
g post_apr22_nov22 = start >= date("2022-04-01","YMD")
g indXpost = d_nbrand * post
g indXp_apr20_mar21 = d_nbrand * post_apr20_mar21
g indXp_apr21_mar22 = d_nbrand * post_apr21_mar22
g indXp_apr22_nov22 = d_nbrand * post_apr22_nov22


************************************************************************************************************
// foreach yd of numlist 200 500 {
foreach yd of numlist 500 {

local yd 500
local drug_control "i.month_id#c.ndrug`yd'"
local microarea_control "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"
// local nbank_control "nbank`yd'"

local bothtype1 "pctbrandzip_jan2020 >0 & pctbrandzip_jan2020 < 1 & pctbrandzip_jan2020 != ."
local bothtype2 "pctbrandtr_jan2020 >0 & pctbrandtr_jan2020 < 1 & pctbrandtr_jan2020 != ."

g bank`yd'Xind = bank`yd' * d_nbrand

g bank`yd'Xpost = bank`yd' * post
g bank`yd'Xp_apr20_mar21 = bank`yd' * post_apr20_mar21
g bank`yd'Xp_apr21_mar22 = bank`yd' * post_apr21_mar22
g bank`yd'Xp_apr22_nov22 = bank`yd' * post_apr22_nov22

g bank`yd'XindXpost = bank`yd'Xind * post
g bank`yd'XindXp_apr20_mar21 = bank`yd'Xind * post_apr20_mar21
g bank`yd'XindXp_apr21_mar22 = bank`yd'Xind * post_apr21_mar22
g bank`yd'XindXp_apr22_nov22= bank`yd'Xind * post_apr22_nov22

local yvar "dln_visits_mar20"

global output_file "tables/bank_gap_eventstudy_table_radius`yd'_ipw_int"

*****************************************************************************************************
*County FE
* Full sample cumulative brand gap
eststo clear

* Nonbrand-brand gap post in periods
* Cty-month FE
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local dv "`yvar'"


eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local dv "`yvar'"


* Brand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "Brand Microarea"
qui estadd local dv "`yvar'"


* Nonbrand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_ind`yd' ///
[aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "`yvar'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(cty_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw], absorb(cty_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

* Zip-month FE
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local dv "`yvar'"


eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local dv "`yvar'"


* Brand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "Brand Microarea"
qui estadd local dv "`yvar'"


* Nonbrand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_ind`yd' ///
[aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "`yvar'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw], absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "All Stores"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

* Zip-month FE & Overlapping Zips
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local dv "`yvar'"


eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local dv "`yvar'"


* Brand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local micro_type "Brand Microarea"
qui estadd local dv "`yvar'"

* Nonbrand microarea
eststo: reghdfe `yvar' ///
bank`yd'XindXp_apr20_mar21 bank`yd'XindXp_apr21_mar22 bank`yd'XindXp_apr22_nov22 ///
indXp_apr20_mar21 indXp_apr21_mar22 indXp_apr22_nov22 ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_ind`yd' ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "`yvar'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "N"
qui estadd local tract_ctrl "N"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

eststo: reghdfe dln_ind`yd' ///
bank`yd'Xp_apr20_mar21 bank`yd'Xp_apr21_mar22 bank`yd'Xp_apr22_nov22 dln_brn`yd' ///
[aw= ipw] if `bothtype1', absorb(zip_ym_id `drug_control' `microarea_control' `tract_control' ) vce(cluster id_ad)
qui estadd local fe1 "Zip-Month FE"
qui estadd local fe2 ""
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local bank_dist "`yd' Yards"
qui estadd local sample "Overlapping Zips"
qui estadd local micro_type "NonBrand Microarea"
qui estadd local dv "dln_ind`yd'"

esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:dln(\#Visits Rel. to Mar. 2020)")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 micro_ctrl tract_ctrl bank_dist sample micro_type dv r2_a F, fmt(0 0 0 0 0 0 0 0 0 3 3) labels("Nobs" "FE" " " "Microarea Controls" "Tract Controls" "Distance from Bank" "Sample" "Microarea Type" "DV" "Adj. R$^{2}$" "F Stat")) ///
 title("Effect of Bank Proximity on Brand-Nonbrand Gap, Dynamic DID (Relative to Mar 2020)") nomtitle 
}
