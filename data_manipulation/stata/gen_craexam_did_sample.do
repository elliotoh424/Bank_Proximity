// do "C:/Users/elliotoh/Box/lodes_shared/elliot/code/stata/regression/cumchange/main/data_generation/stata/gen_cra_examination_combined.do" /* generate bank-examination event window data */
// do "C:/Users/elliotoh/Box/lodes_shared/elliot/code/stata/regression/cumchange/main/data_generation/stata/gen_bank_examination_year_month_pairs.do" /* Based on drug-sod 2019, create bank-ym pairs between Jan 2018 - Dec 2023*/

* Merge Drug-SOD2019 with Bank M&A data between July 2019 and March 2025 (current). Create extra rows for placekey-acquiror banks.
* 26 stores nearby bank with overlapping CRA examination windows. Only 1 store nearby only these banks
use placekey ID_RSSD namefull address city stalp dist_store_branch d_brand using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_banksize.dta", clear 
ren ID_RSSD rssdid
keep if dist_store_branch <= 500 & dist_store_branch != .
keep placekey rssdid namefull address city stalp d_brand
duplicates drop
unique placekey rssdid
drop if rssdid == 244149 | rssdid == 261146 | rssdid == 443353 | rssdid == 529341 | rssdid == 593771 | rssdid == 663955 | rssdid == 797140 | rssdid == 911973 | rssdid == 1492817 | rssdid == 2666400 | rssdid == 2838207
duplicates list placekey rssdid

* Obtain bank size classification based on closest bank branch
merge m:1 placekey using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_dist_banksize.dta", assert(2 3)
drop if _merge == 2
drop _merge
ren *mega_*yd *mega*
ren *cb_*yd *cb*


* Merge with exam schedule
* _merge == 1 means bank has no examination between 2018Q1 and 2023Q4 or not among banks matched in drug-SOD2019.
* _merge == 2 means bank-year-quarter had no examination.
* We will only use stores near non-target banks. Branches could have been consolidated for target branches.
joinby rssdid using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/rssdid_event_windows.dta", unmatched(both)
tab _merge
assert _merge == 1 | _merge == 3
keep if _merge == 3
unique placekey /* 11927 stores with examinations*/
unique rssdid /* 2753 banks with examinations near drug stores*/
g yq = yq(year, quarter)
format yq %tq
g exam_yq = yq(orig_year, orig_quarter)
format exam_yq %tq

g treated = 1
sort placekey rssdid exam_yq event_time
unique placekey rssdid yq /* Unique at store-bank-yq level */

* Bank examination. Event_time is missing for unmatched store-bank pairs. Unmatched store-bank (either pre or post acquisition) did not receive CRA examinations between 2018-2023. 
* These observation will simply missing year-quarter values and will be assigned zero for the match_pre/post variables. Ultimately, when we match with the pharmacy-month data, these observations will be unmatched and we will fill in the value zero for these stores.
* No event time --> 0 match value.
g match_pre0 = (event_time == 0)

g match_pre1 = (event_time == -1)
g match_pre2 = (event_time == -2)
g match_pre3 = (event_time == -3)

g match_post1 = (event_time == 1)
g match_post2 = (event_time == 2)
g match_post3 = (event_time == 3)

* No event time --> 0 match value.
local banksize mega nonmega cb 
foreach var of local banksize{
	g match_pre0_`var' = (match_pre0 & `var'500) 

	g match_pre1_`var' = (match_pre1 & `var'500)
	g match_pre2_`var' = (match_pre2 & `var'500)
	g match_pre3_`var' = (match_pre3 & `var'500)

	g match_post1_`var' = (match_post1 & `var'500)
	g match_post2_`var' = (match_post2 & `var'500)
	g match_post3_`var' = (match_post3 & `var'500)
	}

collapse (max) d_pre3_mega = match_pre3_mega d_pre2_mega = match_pre2_mega d_pre1_mega = match_pre1_mega d_pre0_mega = match_pre0_mega d_post1_mega = match_post1_mega d_post2_mega = match_post2_mega d_post3_mega = match_post3_mega ///
d_pre3_nonmega = match_pre3_nonmega d_pre2_nonmega = match_pre2_nonmega d_pre1_nonmega = match_pre1_nonmega d_pre0_nonmega = match_pre0_nonmega d_post1_nonmega = match_post1_nonmega d_post2_nonmega = match_post2_nonmega d_post3_nonmega = match_post3_nonmega ///
d_pre3_cb = match_pre3_cb d_pre2_cb = match_pre2_cb d_pre1_cb = match_pre1_cb d_pre0_cb = match_pre0_cb d_post1_cb = match_post1_cb d_post2_cb = match_post2_cb d_post3_cb = match_post3_cb ///
(mean) p_pre3_mega = match_pre3_mega p_pre2_mega = match_pre2_mega p_pre1_mega = match_pre1_mega p_pre0_mega = match_pre0_mega p_post1_mega = match_post1_mega p_post2_mega = match_post2_mega p_post3_mega = match_post3_mega ///
p_pre3_nonmega = match_pre3_nonmega p_pre2_nonmega = match_pre2_nonmega p_pre1_nonmega = match_pre1_nonmega p_pre0_nonmega = match_pre0_nonmega p_post1_nonmega = match_post1_nonmega p_post2_nonmega = match_post2_nonmega p_post3_nonmega = match_post3_nonmega ///
p_pre3_cb = match_pre3_cb p_pre2_cb = match_pre2_cb p_pre1_cb = match_pre1_cb p_pre0_cb = match_pre0_cb p_post1_cb = match_post1_cb p_post2_cb = match_post2_cb p_post3_cb = match_post3_cb ///
(sum) n_pre3_mega = match_pre3_mega n_pre2_mega = match_pre2_mega n_pre1_mega = match_pre1_mega n_pre0_mega = match_pre0_mega n_post1_mega = match_post1_mega n_post2_mega = match_post2_mega n_post3_mega = match_post3_mega ///
n_pre3_nonmega = match_pre3_nonmega n_pre2_nonmega = match_pre2_nonmega n_pre1_nonmega = match_pre1_nonmega n_pre0_nonmega = match_pre0_nonmega n_post1_nonmega = match_post1_nonmega n_post2_nonmega = match_post2_nonmega n_post3_nonmega = match_post3_nonmega ///
n_pre3_cb = match_pre3_cb n_pre2_cb = match_pre2_cb n_pre1_cb = match_pre1_cb n_pre0_cb = match_pre0_cb n_post1_cb = match_post1_cb n_post2_cb = match_post2_cb n_post3_cb = match_post3_cb, by(placekey d_brand yq treated)
unique placekey yq	
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/examined_store_yq.dta", replace

preserve
keep yq
duplicates drop
tempfile treated_yq
save `treated_yq', replace
restore

* create control sample
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
merge m:1 placekey yq using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/examined_store_yq.dta"
keep if _merge == 1 /* store-yq not in examination window */
drop _merge
* 
merge m:1 yq using `treated_yq', assert(2 3)
drop if _merge == 2
keep placekey d_brand yq 
*
local treated_vars treated ///
d_pre3_mega d_pre2_mega d_pre1_mega d_pre0_mega d_post1_mega d_post2_mega d_post3_mega /// 
d_pre3_nonmega d_pre2_nonmega d_pre1_nonmega d_pre0_nonmega d_post1_nonmega d_post2_nonmega d_post3_nonmega /// 
d_pre3_cb d_pre2_cb d_pre1_cb d_pre0_cb d_post1_cb d_post2_cb d_post3_cb /// 
p_pre3_mega p_pre2_mega p_pre1_mega p_pre0_mega p_post1_mega p_post2_mega p_post3_mega /// 
p_pre3_nonmega p_pre2_nonmega p_pre1_nonmega p_pre0_nonmega p_post1_nonmega p_post2_nonmega p_post3_nonmega /// 
p_pre3_cb p_pre2_cb p_pre1_cb p_pre0_cb p_post1_cb p_post2_cb p_post3_cb /// 
n_pre3_mega n_pre2_mega n_pre1_mega n_pre0_mega n_post1_mega n_post2_mega n_post3_mega /// 
n_pre3_nonmega n_pre2_nonmega n_pre1_nonmega n_pre0_nonmega n_post1_nonmega n_post2_nonmega n_post3_nonmega /// 
n_pre3_cb n_pre2_cb n_pre1_cb n_pre0_cb n_post1_cb n_post2_cb n_post3_cb 

foreach var of local treated_vars{
	g `var' = 0
}

duplicates drop
unique placekey yq
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/unexamined_store_yq.dta", replace

* Combined treated and control observations 
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/examined_store_yq.dta", clear
append using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/unexamined_store_yq.dta"

unique placekey yq
merge 1:m placekey d_brand yq using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta"
tab yq if _merge == 1
keep if _merge == 3
drop _merge
unique placekey start
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_drugstore.dta", replace
