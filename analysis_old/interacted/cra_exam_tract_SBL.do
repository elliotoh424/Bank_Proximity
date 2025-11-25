* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract_merged", clear

keep if pct_mega == 0 | pct_mega == 1
// local nbank_control "nbanktract"

**************
* Post variables
g p_2020 = year == 2020
g p_2021 = year == 2021
g p_2022 = year == 2022

g megaXp_2020 = mega * p_2020
g megaXp_2021 = mega * p_2021
g megaXp_2022 = mega * p_2022

g pct_examXp_2020 = pct_exam * p_2020
g pct_examXp_2021 = pct_exam * p_2021
g pct_examXp_2022 = pct_exam * p_2022

g megaXpct_examXp_2020 = mega * pct_exam * p_2020
g megaXpct_examXp_2021 = mega * pct_exam * p_2021
g megaXpct_examXp_2022 = mega * pct_exam * p_2022

g ln_AmtSBLr = ln(AmtSBLr)
winsor2 AmtSBLr, cuts(1 99) replace
winsor2 ln_AmtSBLr, cuts(1 99) replace
************************************************************************************************************
global output_file "tables/megaonly_exam_tract_SBL"
local ctrl "ndrug_tract lnpoi_tract all_tract_chg_exc4 lmedage lmedinc pct_univ pct_mnty lpdsty lmedhome pct_0to10 pct_public"

su `ctrl', d
eststo clear

foreach yvar of varlist AmtSBLr ln_AmtSBLr{


*******************************************************************************
* Base: Table 2 Column 1. Megabank500. Include pre0
********************************************************************************


********************************************************************************
* ONLY MEGA
********************************************************************************

* Baseline
eststo: reghdfe `yvar' ///
    pct_examXp_2020 pct_examXp_2021 pct_examXp_2022 if mega, ///
    absorb(tract_id cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "tract FE"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega"

* Controls
eststo: reghdfe `yvar' ///
    pct_examXp_2020 pct_examXp_2021 pct_examXp_2022 if mega, ///
    absorb(`ctrl' cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "Controls"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega"

********************************************************************************
* ONE MEGA OR LESS
********************************************************************************

* Baseline
eststo: reghdfe `yvar' ///
    pct_examXp_2020 pct_examXp_2021 pct_examXp_2022 if nmega ==1, ///
    absorb(tract_id cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "tract FE"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "One Mega"

* Controls
eststo: reghdfe `yvar' ///
    pct_examXp_2020 pct_examXp_2021 pct_examXp_2022 if nmega ==1, ///
    absorb(`ctrl' cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "Controls"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "One Mega"


* Baseline
eststo: reghdfe `yvar' ///
    megaXp_2020 megaXp_2021 megaXp_2022 ///
    megaXpct_examXp_2020 megaXpct_examXp_2021 megaXpct_examXp_2022, ///
    absorb(tract_id cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "tract FE"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega vs no bank"

* Controls
eststo: reghdfe `yvar' ///
    megaXp_2020 megaXp_2021 megaXp_2022 ///
    megaXpct_examXp_2020 megaXpct_examXp_2021 megaXpct_examXp_2022, ///
    absorb(`ctrl' cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "Controls"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "Only Mega vs no bank"

* Baseline
eststo: reghdfe `yvar' ///
    megaXp_2020 megaXp_2021 megaXp_2022 ///
    megaXpct_examXp_2020 megaXpct_examXp_2021 megaXpct_examXp_2022 if nmega <= 1, ///
    absorb(tract_id cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "tract FE"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "One Mega vs no bank"

* Controls
eststo: reghdfe `yvar' ///
    megaXp_2020 megaXp_2021 megaXp_2022 ///
    megaXpct_examXp_2020 megaXpct_examXp_2021 megaXpct_examXp_2022 if nmega <= 1, ///
    absorb(`ctrl' cty_yr_id) vce(cluster cty_yr_id)
qui estadd local fe1 "Cty-year FE"
qui estadd local fe2 "Controls"
qui estadd local cluster "Cty-year"
qui estadd local variable "`yvar'"
qui estadd local sample "One Mega vs no bank"
}
esttab using "$output_file.csv", replace ///
collabels("",lhs("DV:AmtSBL")) star(* 0.10 ** 0.05 *** 0.01) cells(b(star fmt(3) ) se(par(( )) fmt(3))) ///
 stats(N fe1 fe2 cluster sample variable r2_a F, fmt(0 0 0 0 0 0 2 2) labels("Nobs" "FE" " " "Cluster" "Sample" "Variable" "Adj. R$^{2}$" "F Stat")) ///
 title("CRA Examination on Tract SBL") nomtitle 

