* All bank examinations 2019-2022
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/bank_examination_mindist.dta", clear
g exam_date = dofc(Exam_Date)
format exam_date %td
g exam_year = year(exam_date)
gen exam = 1
keep if exam_year >= 2019 & exam_year <= 2022
bysort ID_RSSD exam_year: keep if _n == 1       // drop any duplicate events in the same year
keep ID_RSSD exam_year Cert Bank_Name City State CRA_Rating Exam_Criteria Agency 
tempfile bank_exam
save `bank_exam', replace

* SOD 2019
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sod19.dta", clear
qui levelsof ID_RSSD, local(bank)
local years 2019 2020 2021 2022

clear all
* Calculate total number of observations
local num_rssdids : word count `bank'
local num_years : word count `years'

local num_obs = `num_rssdids' * `num_years'
display "Total observations to create: `num_obs'"

* Create an empty dataset with the right size
set obs `num_obs'

* Create the variables
generate ID_RSSD = .
generate exam_year = .

* Fill in the data
local obs = 1
foreach id of local bank {
    foreach y of local years {
            replace ID_RSSD = `id' in `obs'
            replace exam_year = `y' in `obs'
            local obs = `obs' + 1
        }
    }

merge 1:1 ID_RSSD exam_year using `bank_exam', assert(1 3)
g exam = (_merge == 3)
drop _merge
unique ID_RSSD exam_year

joinby ID_RSSD using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sod19.dta", unmatched(both)
tab _merge
drop _merge
unique ID_RSSD brnum exam_year 
ren tractbr tract
keep ID_RSSD exam_year exam uninumbr brnum cert asset cb tract
replace asset = asset/1000 /* in millions of USD */
assert asset != . & cb != .
g mega = (asset >= 100*1000)
g nonmega = (cb == 0 ) & (asset < 100*1000)

collapse (mean) pct_exam = exam pct_mega = mega pct_nonmega = nonmega pct_cb = cb (sum) nmega = mega nnonmega = nonmega ncb = cb (count) nbank = brnum, by(tract exam_year)
g test = nmega + nnonmega + ncb
assert test == nbank
drop test
// keep if pct_mega == 1
ren exam_year year
keep tract year pct_exam pct_mega nmega pct_nonmega nnonmega pct_cb ncb nbank
unique tract year

merge 1:1 tract year using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/cra_tract_2019_2022.dta"
unique tract if _merge == 1
drop if _merge == 1 /* 43 tracts without CRA loans */
foreach var of varlist pct_exam pct_mega pct_nonmega pct_cb nmega nnonmega ncb nbank{
replace `var' = 0 if _merge == 2
}

keep year tract pct_exam pct_mega nmega pct_nonmega nnonmega pct_cb ncb nbank NoSBL100k AmtSBL100k NoSBL100_250k AmtSBL100_250k NoSBL250k_1mil AmtSBL250k_1mil NoSBL_rev_0_1mil AmtSBL_rev_0_1mil lmi NoSBL NoSBL250k AmtSBL AmtSBL250k AvgSBL
unique tract year
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra_exam_tract", replace
