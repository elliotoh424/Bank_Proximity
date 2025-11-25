* Independent store naics4 
use naics_code_micro using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_nonbrand_stores.dta", clear
g naics4 = substr(naics_code_micro, 1,4)
g strlen = strlen(naics4)
keep if strlen == 4
// g naics6 = naics_code_micro
keep naics4
duplicates drop
tempfile naics
save `naics', replace

* CBP naics4 
import delimited using "C:/Users/elliotoh/Box/Chenyang-Elliot/CBP/cbp19co.txt", clear
replace naics  = subinstr(naics , "/", "", .)
replace naics  = subinstr(naics , ".", "", .)
replace naics  = subinstr(naics , "-", "", .)
g strlen = strlen(naics)
keep if strlen == 4
g naics4 = naics
foreach var of varlist n5 n5_9 n10_19 n20_49{
	replace `var' = "0" if `var' == "N"
	destring `var', replace
}
g n9 = n5 + n5_9
g n19 = n9 + n10_19
g n49 = n19 + n20_49

collapse (sum) est n5 n9 n19 n49, by(naics4)
tempfile nest 
save `nest', replace
 
* SBA 7a loans
import delimited "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/foia-7a-fy2010-fy2019-asof-241231.csv", clear bindquotes(strict)

g bank = 1 if bankfdicnumber != "None"
replace bank = 0 if bank == .
g cu = 1 if bankncuanumber != "None"  & bank == 0
replace cu = 0 if cu == .
g other = 1 if bankfdicnumber == "None" & bankncuanumber == "None"
replace other = 0 if other == .
tostring naicscode, replace
g naics4 = substr(naicscode, 1,4)
g naics5 = substr(naicscode, 1,5)
g naics6 = substr(naicscode, 1,6)

keep borrname borrstreet borrcity borrstate borrzip bank cu other grossapproval approvaldate approvalfiscalyear naics4 naics5 naics6 naicscode naicsdescription franchisecode franchisename businessage jobssupported 

g totalloans = _N

merge m:1 naics4 using `nest'
keep if _merge == 3
drop _merge

// * Didn't see much different for 2019 vs. 2017-2019
// tab naics4 if approvalfiscalyear == 2019, sort
// tab naics4 if approvalfiscalyear >= 2017, sort
//
// * Average 12 jobs, 25p 2 jobs, 50p 6 jobs, 75p 14 jobs, 90p 28 jobs, 95p 42 jobs, 99p 97 jobs
// * 7225: average 20 jobs, 25p 5 jobs, 50p 12 jobs
//
// su jobssupported if approvalfiscalyear == 2019, d
// su jobssupported if approvalfiscalyear >= 2017, d
//
// *7225, 8111, 7139, 8121, 7211, 8129
// su jobssupported if approvalfiscalyear == 2019 & naics4 == "7225", d
// su jobssupported if approvalfiscalyear >= 2017, d

* Total approval amount, # of loans, # of loans per total establishment, # of loans per establishment by size threshold
keep if approvalfiscalyear == 2019
collapse (sum) total_amt = grossapproval (count) namt = grossapproval, by(naics4 est n9 n19 n49 totalloans)
g namt_per_est = namt/ est
g namt_per_n9 = namt/n9
g namt_per_n19 = namt/n19
g namt_per_n49 = namt/n49

keep naics4 total_amt namt namt_per_est namt_per_n9 namt_per_n19 namt_per_n49 n9 n19 n49 totalloans
unique naics4

// su namt_per_n9 namt_per_n19 namt_per_n49 if naics6 == "4461"
* namt_per_n9 (drug/min top25%/min top33%/min top50%): .011472    /.0166222/.01293/.0111878
* namt_per_n19 (drug/min top25%/min top33%/min top50%): .0093751  /.0144413/.0109027/.0097492
* namt_per_n19 (drug/min top25%/min top33%/min top50%): .0074153  /.0112539/.010342/.008739

merge m:1 naics4 using `naics'
keep if _merge == 3
drop _merge

g pct_nloans =  namt/totalloans

gsort -namt_per_n9 
br if n9 > 5000 
* naics6: 713940 721110 811192 722511 713990 722320 812910 812990 812199 445310
* naics5: 71394 72111 44529 71399 72232 81291 44611 81119 81299 72251 81219 44531 81232 72241 44819
* naics4: 7211 7139 4452 7225 4453 7224 8123 8129 4511 4412

gsort -namt_per_n19
br if n19 > 5000 
* naics6: 713940 721110 713990 811192 722320 812990 812910 812199 445310 722511
* naics5: 71394 44529 72111 71399 72232 81299 81291 44531 81119 81219 45391 81232 44611
* naics4: 7211 7139 4452 7225 4453 7224 8123 8129 7113 4511


* Designate top 25%, top 33%, and top 50%
foreach var of varlist total_amt namt namt_per_est namt_per_n9 namt_per_n19 namt_per_n49{
	di "Top 25%"
	xtile `var'_qt = `var', nq(4)
	g Q`var' = `var'_qt == 4
	su `var' if Q`var'
	su `var' if ~Q`var'

	di "Top 33%"
	xtile `var'_tt = `var', nq(3)
	g T`var' = `var'_tt == 3
	su `var' if T`var'
	su `var' if ~T`var'

	di "Top 50%"
	xtile `var'_ht = `var', nq(2)
	g H`var' = `var'_ht == 2
	su `var' if H`var'
	su `var' if ~H`var'	
	drop *_qt *_tt *_ht
	}

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/independents_sba.dta", replace


