use placekey ID_RSSD namefull address city stalp dist_store_branch d_brand using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_banksize.dta", clear 
ren ID_RSSD rssdid
keep if dist_store_branch <= 500 & dist_store_branch != . & d_brand == 0
keep rssdid namefull address city stalp
duplicates drop
unique rssdid

* Obtain bank size classification based on closest bank branch
merge m:1 rssdid using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/banksize_sod.dta", assert(2 3)
drop if _merge == 2
drop _merge
ren *_asset *
ren rssdid idrssd
tempfile drug_bank
save `drug_bank', replace

* Figure7
cd "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/callreport/"
* Define local for dates

* Create empty frame to store results
clear
save "temp_callreport.dta", replace emptyok

* Loop through dates to process files
local dates "06302019 09302019 12312019 03312020 06302020 09302020 12312020 03312021 06302021 09302021 12312021 03312022 06302022 09302022 12312022"
foreach date of local dates {
// 	local date "06302019"
    * Read name data
    import delimited "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/callreport/FFIEC CDR Call Bulk All Schedules `date'/FFIEC CDR Call Bulk POR `date'.txt", ///
        delimiter(tab) clear
    
    * Keep relevant columns
    keep idrssd fdiccertificatenumber financialinstitutionname financialinstitutionaddress ///
         financialinstitutioncity financialinstitutionstate financialinstitutionzipcode ///
         financialinstitutionfilingtype
    
    * Check for duplicates
    duplicates report idrssd
    assert r(N) == r(unique_value)
    
    * Save temporarily
    tempfile name_data
    save `name_data', replace
    
    * Read RC schedule data
    import delimited "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/callreport/FFIEC CDR Call Bulk All Schedules `date'/FFIEC CDR Call Schedule RC `date'.txt", ///
        delimiter(tab) clear
    
    * Keep relevant columns
    keep idrssd rcon2200 rcon2170 rcfd2170
    
    * Drop header row
    drop if _n == 1
    
    * Merge with name data
    merge 1:1 idrssd using `name_data', assert(3)
    drop _merge
    
    * Add report date
    gen reportdate = "`date'"
    
    * Append to main dataset
    append using "temp_callreport.dta"
    save "temp_callreport.dta", replace
}

* Combine total assets filings for domestic branch only banks and domestic+foreign branch banks
destring rcon2200, replace
destring rcon2170, replace
destring rcfd2170, replace
assert (rcon2170 == . & rcfd2170 == .) == 0
assert (rcon2170 != . & rcfd2170 != .) == 0

gen asset = rcon2170
replace asset = rcfd2170 if rcon2170 == .
rename rcon2200 deposits

merge m:1 idrssd using `drug_bank'
keep if _merge == 3
drop _merge
save "temp_callreport.dta", replace

* Keep relevant columns
use "temp_callreport.dta", clear
keep idrssd reportdate mega cb nonmega deposits fdiccertificatenumber financialinstitutionname financialinstitutionaddress financialinstitutioncity financialinstitutionstate financialinstitutionzipcode financialinstitutionfilingtype

ren fdiccertificatenumber cert
ren cb community
unique idrssd reportdate
g size = ""
foreach var of varlist mega nonmega community{
replace size = "`var'" if `var'
}
keep idrssd deposits reportdate size

preserve
keep if reportdate == "12312019"
keep idrssd deposits
ren deposits deposits19 
tempfile deposit19
save `deposit19', replace
restore

merge m:1 idrssd using `deposit19', assert(1 3)
keep if _merge == 3
drop _merge
g deposits_std_ew = deposits/deposits19

collapse (sum) deposits deposits19 (mean) deposits_std_ew, by(size reportdate)
g deposits_std_vw = deposits/deposits19

g date = date(reportdate,"MDY")
format date %td
g date1 = qofd(date)
format date1 %tq
drop date
ren date1 date

bysort date: egen deposits_all = sum(deposits)
bysort date: egen deposits19_all = sum(deposits19)
bysort date: egen deposits_std_ew_all = mean(deposits_std_ew)
g deposits_std_vw_all = deposits_all/deposits19_all

keep if date >= yq(2019, 4) & date <= yq(2022,4)
twoway (connected deposits_std_ew_all date, lcolor(black) mcolor(black)) ///
	   (connected deposits_std_ew date if size == "mega", lcolor(red) mcolor(red)) ///
       (connected deposits_std_ew date if size == "nonmega", lcolor(blue) mcolor(blue)) ///
       (connected deposits_std_ew date if size == "community", lcolor(green) mcolor(green)), ///
       legend(order(1 "All" 2 "Mega Bank" 3 "Non-Mega Bank" 4 "Community Bank")) ///
	   ylab(1(0.1)1.5) ///
       title("Deposit Growth Over Time by Size (Equal-Weight)") ///
       xtitle("Date") ytitle("Deposits (Standardized)") ///
       xlabel(239 "2019Q4" 243 "2020Q4" 247 "2021Q4" 251 "2022Q4") ///
       xscale(range(239 251))
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/deposits_size_ew_drug.png", replace

twoway (connected deposits_std_vw_all date, lcolor(black) mcolor(black)) ///
	   (connected deposits_std_vw date if size == "mega", lcolor(red) mcolor(red)) ///
       (connected deposits_std_vw date if size == "nonmega", lcolor(blue) mcolor(blue)) ///
       (connected deposits_std_vw date if size == "community", lcolor(green) mcolor(green)), ///
       legend(order(1 "All" 2 "Mega Bank" 3 "Non-Mega Bank" 4 "Community Bank")) ///
	   ylab(1(0.1)1.5) ///
       title("Deposit Growth Over Time by Size") ///
       xtitle("Date") ytitle("Deposits (Standardized)") ///
       xlabel(239 "2019Q4" 243 "2020Q4" 247 "2021Q4" 251 "2022Q4") ///
       xscale(range(239 251))
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/deposits_size_vw_drug.png", replace


	