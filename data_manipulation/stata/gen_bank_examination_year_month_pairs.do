* Create rssdid-year-month dataframe
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_drug.dta", clear
keep if bank_branch_500yd ==1
keep placekey rssdid
drop if rssdid == 244149 | rssdid == 261146 | rssdid == 443353 | rssdid == 529341 | rssdid == 593771 | rssdid == 663955 | rssdid == 797140 | rssdid == 911973 | rssdid == 1492817 | rssdid == 2666400 | rssdid == 2838207 /* Drop banks with overlapping examination windows [-3, 3]. Affects only one nonbrand store */
duplicates drop
tempfile store_bank
save `store_bank', replace

qui levelsof rssdid, local(rssdid_list)
local years 2018 2019 2020 2021 2022 2023
local months 1 2 3 4 5 6 7 8 9 10 11 12

clear all
* Calculate total number of observations
local num_rssdids : word count `rssdid_list'
local num_years : word count `years'
local num_months = 12

local num_obs = `num_rssdids' * `num_years' * `num_months'
display "Total observations to create: `num_obs'"

* Create an empty dataset with the right size
set obs `num_obs'

* Create the variables
generate rssdid = .
generate year = .
generate month = .

* Fill in the data
local obs = 1
foreach id of local rssdid_list {
    foreach y of local years {
        foreach m of local months {
            replace rssdid = `id' in `obs'
            replace year = `y' in `obs'
            replace month = `m' in `obs'
            local obs = `obs' + 1
        }
    }
}

* Format the variables
format rssdid %12.0f

* Sort the data
sort rssdid year month

g quarter = .
replace quarter = 1 if month <= 3 
replace quarter = 2 if month >= 4 & month <= 6 
replace quarter = 3 if month >= 7 & month <= 9
replace quarter = 4 if month >= 10 & month <= 12

generate yearquarter = year * 4 + quarter


* Display summary of created dataset
describe
summarize
unique rssdid year month
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/rssdid_year_month.dta", replace