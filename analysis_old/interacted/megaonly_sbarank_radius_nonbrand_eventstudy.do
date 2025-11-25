* ----------------------------------------------------------------------
* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Load base file and filter
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
drop if npoi500 <= 5
drop if placekey == "226-222@8dy-qsc-hqz" | placekey == "223-222@5s8-cj6-jd9" /* Two outlier stores with low propensity scores */
drop if nonmega500 | cb500

* Sanity: all SBA-related variables should be non-missing (if they exist)
// foreach var of varlist *SBA* {
//     assert `var' != .
// }

* Merge IPW / bank proximity file
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/megaonly_radius500_ipw.dta"
assert _merge == 1 if ~d_nbrand 
keep if _merge == 3
drop _merge

su NoSBA_iest1019 if d_nbrand == 1 & tag == 1, d

centile NoSBA_iest1019 if d_nbrand==1 & tag==1, centile(67)

egen tag = tag(cty)
hist AmtSBA_iest1019 if d_nbrand == 1 & AmtSBA_iest1019 <= 16666.67 & tag == 1, frac ///
    xline(2706.477, lcolor(red) lpattern(dash)) ///
    xline(5163.636, lcolor(blue) lpattern(dash)) ///
	title("Total AmtSBA between 2010-2019/Ind Pharmacy at Cty-level" /// 
	"Truncated at 90th percentile") ylab(0(0.1)0.5)
graph export "figure/intermediate/ambsba_hist.png", replace 

//
// twoway ///
//     (binsreg NoSBA_iest1019 iest1019 if tag==1 & iest1019 <= 23 & mega500_cty==1, nbins(50) mfcolor(blue)) ///
//     (binsreg NoSBA_iest1019 iest1019 if tag==1 & iest1019 <= 23 & bank500_cty==1, nbins(50) mfcolor(red)), ///
//     legend(label(1 "Mega500") label(2 "Bank500")) ///
//     title("Overlay: Mega500_cty vs Bank500_cty")



* 27% have zeros.
hist NoSBA_iest1019 if d_nbrand == 1 & NoSBA_iest1019 <= 0.05 & tag == 1, frac ///
    xline(.0173424, lcolor(red) lpattern(dash)) ///
    xline(.024, lcolor(blue) lpattern(dash)) ///
	title("Total NoSBA between 2010-2019/Ind Pharmacy at Cty-level" /// 
	"Truncated at 90th percentile") ylab(0(0.1)0.5)
graph export "figure/intermediate/nosba_hist.png", replace 

su iest1019, d

br cty city region iest1019 


tab cty if iest1019 > 500 & iest1019 != .

megaXPeriodXh

g x1 = mega500Xpost3
g x2 =  mega500Xpost3 * QNoSBA_iest1019

tab mega500Xpost3 x2 

cor mega500 TNoSBA_iest1019 if d_nbrand == 1
cor mega500 QAmtSBA_iest1019 if d_nbrand == 1

su AmtSBA_iest1019 if tag == 1 & AmtSBA_iest1019 >0, d

binscatter x1 x2 if d_nbrand == 1 & tag == 1

binscatter NoSBA_iest1019 iest1019 if d_nbrand == 1 & NoSBA_iest1019 <= 0.05 & tag == 1 & iest1019 < 100 & iest1019  != .

, frac ///
    xline(.0173424, lcolor(red) lpattern(dash)) ///
    xline(.024, lcolor(blue) lpattern(dash)) ///
	title("Total NoSBA between 2010-2019/Ind Pharmacy at Cty-level" /// 
	"Truncated at 90th percentile") ylab(0(0.1)0.5)


	
* ------------------------
* Post-period indicators
* ------------------------
gen post = start >= date("2020-04-01","YMD")
gen p_apr20_mar21 = start >= date("2020-04-01","YMD") & start < date("2021-04-01","YMD")   // post1
gen p_apr21_mar22 = start >= date("2021-04-01","YMD") & start < date("2022-04-01","YMD")   // post2
gen p_apr22_nov22 = start >= date("2022-04-01","YMD")                                      // post3

* ------------------------
* Base interactions: mega500 × each post period
* ------------------------
capture drop mega500Xpost1 mega500Xpost2 mega500Xpost3
gen mega500Xpost1 = mega500 * p_apr20_mar21
gen mega500Xpost2 = mega500 * p_apr21_mar22
gen mega500Xpost3 = mega500 * p_apr22_nov22
assert mega500Xpost1 != . & mega500Xpost2 != . & mega500Xpost3 != .

ren *Amt* *At* 
ren *0019* *00* 
ren *0519* *05* 
ren *1019* *10* 
ren *1319* *13* 
ren *_iest* *ie*
// ren *_sest* *se*

* ------------------------
* Controls and settings
* ------------------------
local yd 500
local yvar dln_visits_mar20
local drug_control       "i.month_id#c.ndrug`yd'"
local microarea_control  "i.month_id#c.lnpoi`yd' i.month_id#c.all_`yd'yd_chg_exc4"
local tract_control      "i.month_id#c.lmedage i.month_id#c.lmedinc i.month_id#c.pctuniv i.month_id#c.pctmnty i.month_id#c.lpdsty i.month_id#c.lmedhome i.month_id#c.pct0to10 i.month_id#c.pctpublic"

global output_file "tables/megaonly_nb_eventstudy_sbaint_radius`yd'_ipw"

* ------------------------
* Triple interactions: mega500 × post{1,2,3} × (H/T/Q)(AtSBA|NoSBA|AtSBAe|NoSBAe){10,15}
* ------------------------
eststo clear

eststo: reghdfe `yvar' ///
mega500Xpost1 mega500Xpost2 mega500Xpost3 ///
dln_brn500 [aw= ipw] ///
if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)
qui estadd local fe1 "Cty-Month FE"
qui estadd local micro_ctrl "Y"
qui estadd local tract_ctrl "Y"
qui estadd local sample "Baseline"
qui estadd local dv "dln(#Visits Rel. to Mar. 2020)"


local yr_vars "00 05 10 13"
foreach yr of local yr_vars {
foreach cut in T Q {
foreach base in AtSBA AtSBAie NoSBA NoSBAie{

            * The SBA dummy should exist: e.g., HAtSBA10, TNoSBA15, QAtSBAe10, etc.
            local sbadum `cut'`base'`yr'
            confirm variable `sbadum'

            * Build triple interaction terms (drop/recreate to be safe)
            capture drop mega500Xpost1X`sbadum' mega500Xpost2X`sbadum' mega500Xpost3X`sbadum'
            gen mega500Xpost1X`sbadum' = mega500Xpost1 * `sbadum'
            gen mega500Xpost2X`sbadum' = mega500Xpost2 * `sbadum'
            gen mega500Xpost3X`sbadum' = mega500Xpost3 * `sbadum'

            * Run regression:
            * Y = b1*mega500Xpost1 + b2*mega500Xpost2 + b3*mega500Xpost3
            *   + b4*mega500Xpost1X`sbadum' + b5*mega500Xpost2X`sbadum' + b6*mega500Xpost3X`sbadum' + controls
            eststo `cut'_`base'_`yr': reghdfe `yvar' ///
                mega500Xpost1 mega500Xpost2 mega500Xpost3 ///
                mega500Xpost1X`sbadum' mega500Xpost2X`sbadum' mega500Xpost3X`sbadum' ///
                dln_brn500 [aw= ipw] ///
                if d_nbrand, absorb(cty_ym_id `drug_control' `microarea_control' `tract_control') vce(cluster id_ad)

            qui estadd local fe1 "Cty-Month FE"
            qui estadd local micro_ctrl "Y"
            qui estadd local tract_ctrl "Y"
            qui estadd local sample "`cut' `base' `yr'"

            * # of observations in top X%
            quietly count if `sbadum' == 1 & e(sample)
            estadd scalar n_top_pct = r(N)
            
            * # of observations in top X% and near megabank
            quietly count if `sbadum' == 1 & e(sample) & mega500 == 1
            estadd scalar n_top_pct_mega = r(N)
            
            * # of unique counties in top X%
            quietly tab cty if `sbadum' == 1 & e(sample)
            estadd scalar n_counties_top = r(r)
            
            * # of unique counties in top X% and near megabank
            quietly tab cty if `sbadum' == 1 & e(sample) & mega500 == 1
            estadd scalar n_counties_top_mega = r(r)
			
//             * # of establishments
//             quietly summarize allest`yr' if e(sample) & `sbadum' == 1 , meanonly
//             estadd scalar avg_allest = r(mean)

//             * # of establishments near megabank
// 			quietly summarize allest`yr' if e(sample) & mega500 == 1 & `sbadum' == 1 , meanonly
// 			estadd scalar avg_allest_mega = r(mean)

            * # of pharmacies
			quietly summarize iest`yr' if e(sample) & `sbadum' == 1 , meanonly
			estadd scalar avg_est = r(mean)

            * # of pharmacies near megabank
			quietly summarize iest`yr' if e(sample) & mega500 == 1 & `sbadum' == 1 , meanonly
			estadd scalar avg_est_mega = r(mean)

			
			}
    }
}

* ------------------------
* Output table
* ------------------------
* Then update your esttab stats line to include these new statistics:
esttab using "$output_file.csv", replace ///
    collabels("", lhs("DV:dln(#Visits Rel. to Mar. 2020)")) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    cells(b(star fmt(3)) se(par(( )) fmt(3))) ///
    stats(N n_top_pct n_top_pct_mega n_counties_top n_counties_top_mega ///
          avg_est avg_est_mega ///
          fe1 micro_ctrl tract_ctrl sample r2_a F, ///
          fmt(0 0 0 0 0 2 2 0 0 0 0 3 3) ///
          labels("Nobs" "N Top Percentile" "N Top Percentile & Mega" ///
                 "Counties Top Percentile" "Counties Top Percentile & Mega" ///
                 "Average # of ind pharmacies" "Average # of ind pharmacies (Mega=1)" ///
                 "FE" "Microarea Controls" "Tract Controls" "Sample" "Adj. R^2" "F Stat")) ///
    title("Effect of Megabank Proximity × Post × SBA Intensity (Triple Interactions)")

	