* Generate Advan-ACS matched tract-level data for all Advan stores
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/"

* Regression for high vs. low Net Outflow tracts and scaled foot traffic
// copy "D:/pharmacy/advan/acs_drug_matched_month.dta" "C:/Users/elliotoh/Box/analysis/acs_drug_matched_month.dta"
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef.dta", clear
unique placekey start
ren poi_cbg cbg
ren d_nonbrand d_nbrand

destring wkt_area_sq_meters, replace
ren wkt_area_sq_meters store_size

// g cty = substr(tract, 1, 5)
egen month_id = group(start)
egen cty_id = group(cty)
egen cty_ym_id = group(cty start)
egen zip_id = group(zip)
egen zip_ym_id = group(zip start)
egen tract_id = group(tract)
egen tract_ym_id = group(tract start)

xtset id_ad start

* Tried standardizing D-O average difference as a % of d_tract. Large outliers (>100%) causes issues.
ren tract_aland tractsize
ren pdensity* pdsty*
ren pct_* pct* 

foreach yd of numlist 200 500 1000{
g nout`yd' = ngrocery_pharmacy_`yd'yd + ncolocated_pharmacy_`yd'yd
}

local controlvars "medage medinc pdsty pctuniv pctmnty wfh tractsize tot_pop"
local ctrl_dummy ""
foreach var of local controlvars{
	g l`var' = log(`var')
}
*

* Check that cb from bank_dist and cb from banksize are the same.
foreach yd of numlist 200 500 1000{
assert cb_`yd'yd == cb_branch_`yd'yd
}
drop cb_200yd* cb_500yd* cb_1000yd* ncb_200yd* ncb_500yd* ncb_1000yd*


* Modify name of bank branch variables.
ren *bank_branch_*yd *bank_*yd
ren *bank_branch_tract *bank_tract
ren *cb_branch_*yd *cb_*yd
ren *cb_branch_tract *cb_tract
ren *noncra_branch_*yd *noncra_*yd
ren *noncra_branch_tract *noncra_tract
ren *yescra_branch_tract *yescra_tract

// * Check that bank proximity is defined as radius
// foreach num of numlist 1(1)10{
// 	local num1 = `num' + 1
// 	assert bank_`num'00yd <= bank_`num1'00yd
// 	assert nbank_`num'00yd <= nbank_`num1'00yd
// 	assert cb_`num'00yd <= cb_`num1'00yd
// 	assert ncb_`num'00yd <= ncb_`num1'00yd
// 	assert noncb_`num'00yd <= noncb_`num1'00yd
// 	assert nnoncb_`num'00yd <= nnoncb_`num1'00yd	
// }




* Check that microarea is inclusive
assert npoinm_xb_200yd <= npoinm_xb_500yd
assert npoinm_xb_500yd <= npoinm_xb_1000yd

ren *bank_branch_*yd_exc *bank*exc
ren *cb_branch_*yd_exc *cb*exc
local banksize "mega"
foreach bs of local banksize{
ren *`bs'_*yd_ring *`bs'*exc
}

* 0- 500 yards and 500-1000 yards
local vars "bank noncb cb"
foreach v of local vars{
	egen `v'500ring = rowmax(`v'100exc `v'200exc `v'300exc `v'400exc `v'500exc)
	g n`v'500ring = n`v'100exc + n`v'200exc + n`v'300exc + n`v'400exc + n`v'500exc

	egen `v'1000ring = rowmax(`v'600exc `v'700exc `v'800exc `v'900exc `v'1000exc)
	g n`v'1000ring = n`v'600exc + n`v'700exc + n`v'800exc + n`v'900exc + n`v'1000exc

	}

ren *bank_*yd *bank*
ren *cb_*yd *cb*
local banksize "mega"
foreach bs of local banksize{
ren *`bs'_*yd *`bs'*    
}
ren *noncra_*yd *ncra*
ren npoi_xb_*yd npoi*
ren brn_npoi_*yd brn_npoi*
ren ind_npoi_*yd ind_npoi*
ren rem_npoi_*yd rem_npoi*
ren npoi_pharmacy_*yd ndrug*
// ren allpoi_avgnvisits_*yd_exc nvisit*
ren dist_from_cbd dist_cbd
ren median_homevalue medhome

ren *t50 *H
ren *t50_* *H_*
ren *t3 *T
ren *t33_* *T_*
ren *t25 *Q
ren *t25_* *Q_*

* Define dummy for both bank types
g both200 = (cb200) & (noncb200)
g both500 = (cb500) & (noncb500)
g both1000 = (cb1000) & (noncb1000)

* Multiple banks by size
g mult200 = (mega200 & nonmega200) | (mega200 & cb200) | (nonmega200 & cb200)
g mult500 = (mega500 & nonmega500) | (mega500 & cb500) | (nonmega500 & cb500)
g mult1000 = (mega1000 & nonmega1000) | (mega1000 & cb1000) | (nonmega1000 & cb1000)

foreach var of varlist npoi* dist_cbd medhome{
g l`var' = ln(`var')        
}

foreach yd of numlist 200 500 1000{
g dln_all`yd' = ln(all_avgnvisits_`yd'yd_exc) - ln(all_avgnvisits_`yd'yd_exc_mar20)
g dln_brn`yd' = ln(brn_avgnvisits_`yd'yd) - ln(brn_avgnvisits_`yd'yd_mar20)
g dln_ind`yd' = ln(ind_avgnvisits_`yd'yd) - ln(ind_avgnvisits_`yd'yd_mar20)
g dln_rem`yd' = ln(rem_avgnvisits_`yd'yd) - ln(rem_avgnvisits_`yd'yd_mar20)
}

g dln_drugbrntr = ln(drugbrn_avgnvisits_tr_exc) - ln(drugbrn_avgnvisits_tr_exc_mar20)
// g lmi_bandwidth = (pctmfi19 - 80)

// su lmi_bandwidth if chg_Nlmi & d_nbrand, d /* 83% of stores in non-lmi to lmi tract remain within 25, 47% within 10, and 26% within 5.*/
// su lmi_bandwidth if chg_Nlmi & d_nbrand, d /* 90% of stores in lmi to non-lmi tract remain within 25, 60% within 10, and 35% within 5.*/
// count if chg_Nlmi & d_nbrand 
// count if chg_Nlmi & d_nbrand  & abs(lmi_bandwidth) < 5
//
// count if chg_lmi & d_nbrand 
// count if chg_lmi & d_nbrand  & abs(lmi_bandwidth) < 25

* Cumulative change in stable and risky independents in microarea 
local types risky stable
local positions u i g
local percentiles H T Q

foreach type in `types' {
    foreach pos in `positions' {
        foreach pct in `percentiles' {
            g dln_`type'_`pos'`pct' = ln(avgnvisits_`type'_`pos'`pct') - ln(avgnvisits_`type'_`pos'`pct'_mar20)
        }
    }
}
//
// local banktype mega nonmega cb
// foreach var of local banktype{
// * Indicators
// ren max_match_pre0_`var' d_pre0_`var'
// ren max_match_pre1_`var' d_pre1_`var'
// ren max_match_pre2_`var' d_pre2_`var'
// ren max_match_pre3_`var' d_pre3_`var'
//
// ren max_match_post1_`var' d_post1_`var'
// ren max_match_post2_`var' d_post2_`var'
// ren max_match_post3_`var' d_post3_`var'
//
// * % bank
// ren mean_match_pre0_`var' p_pre0_`var'
// ren mean_match_pre1_`var' p_pre1_`var'
// ren mean_match_pre2_`var' p_pre2_`var'
// ren mean_match_pre3_`var' p_pre3_`var'
//
// ren mean_match_post1_`var' p_post1_`var'
// ren mean_match_post2_`var' p_post2_`var'
// ren mean_match_post3_`var' p_post3_`var'
//
// * # bank
// ren sum_match_pre0_`var' n_pre0_`var'
// ren sum_match_pre1_`var' n_pre1_`var'
// ren sum_match_pre2_`var' n_pre2_`var'
// ren sum_match_pre3_`var' n_pre3_`var'
//
// ren sum_match_post1_`var' n_post1_`var'
// ren sum_match_post2_`var' n_post2_`var'
// ren sum_match_post3_`var' n_post3_`var'
//
// }
//
// local banktype mega nonmega cb
// foreach var of local banktype{
// * Indicators
// ren mnmax_match_pre0_`var' md_pre0_`var'
// ren mnmax_match_pre1_`var' md_pre1_`var'
// ren mnmax_match_pre2_`var' md_pre2_`var'
// ren mnmax_match_pre3_`var' md_pre3_`var'
//
// ren mnmax_match_post1_`var' md_post1_`var'
// ren mnmax_match_post2_`var' md_post2_`var'
// ren mnmax_match_post3_`var' md_post3_`var'
//
// * % bank
// ren mnmean_match_pre0_`var' mp_pre0_`var'
// ren mnmean_match_pre1_`var' mp_pre1_`var'
// ren mnmean_match_pre2_`var' mp_pre2_`var'
// ren mnmean_match_pre3_`var' mp_pre3_`var'
//
// ren mnmean_match_post1_`var' mp_post1_`var'
// ren mnmean_match_post2_`var' mp_post2_`var'
// ren mnmean_match_post3_`var' mp_post3_`var'
//
// * # bank
// ren mnsum_match_pre0_`var' mn_pre0_`var'
// ren mnsum_match_pre1_`var' mn_pre1_`var'
// ren mnsum_match_pre2_`var' mn_pre2_`var'
// ren mnsum_match_pre3_`var' mn_pre3_`var'
//
// ren mnsum_match_post1_`var' mn_post1_`var'
// ren mnsum_match_post2_`var' mn_post2_`var'
// ren mnsum_match_post3_`var' mn_post3_`var'
//
// }
drop yq 

g yq = qofd(start)
format yq %tq 
// ren match_post_q11 yex_a11
// ren match_post_q12 yex_a12

* Observations we shouldn't use in bank examination regression.
// g problem_obs1 = store_ma_branch | store_new_branch | store_unmatched_branch
// g problem_obs2 = store_ma_branch | store_overlap_bank | store_new_branch | store_unmatched_branch | store_close_deal_exam

*store_ma_branch: near both acq and tgt (drop)
*store_noexam: near noexam bank with acq. 846 stores(actual no exam. Check that match is all 0)
*store_overlap_bank: near banks with overlapping windows (drop)
*store_new_branch: near bank in drug microarea due to acquisition. Previously not in drug microarea (drop)
*store_unmatched_branch: near banks with no examination with no acquisition. UnCertain but small. 54 stores (drop.) 
*store_close_deal_exam: stores near banks where exam takes place within 2 quarters of deal completion. 1143 stores.  Not enough time to prepare (drop?)

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/advan/acs_drug_matched_month_radiusdef_forreg.dta", replace
