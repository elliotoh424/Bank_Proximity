* Merge Drug-SOD2019 with Bank M&A data between July 2019 and March 2025 (current). Create extra rows for placekey-acquiror banks.
* 26 stores nearby bank with overlapping CRA examination windows. Only 1 store nearby only these banks
use placekey ID_RSSD namefull address city stalp dist_store_branch d_brand using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_banksize.dta", clear 
ren ID_RSSD rssdid
keep if dist_store_branch <= 500 & dist_store_branch != . & d_brand == 0
keep placekey rssdid namefull address city stalp d_brand
duplicates drop
unique placekey rssdid
bysort rssdid: egen nstore = count(placekey)

replace rssdid = 852320 if rssdid == 675332 /* BB&T (852320) acquired Suntrust (675332) in Dec 2019. */

* Obtain bank size classification based on closest bank branch
merge m:1 rssdid using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/banksize_sod.dta", assert(2 3)
drop if _merge == 2
drop _merge
ren *_asset *

egen tag = tag(placekey rssdid)
bysort placekey: egen nbank = total(tag)
bysort placekey: egen mega500 = max(mega)
bysort placekey: egen nonmega500 = max(nonmega)
bysort placekey: egen cb500 = max(cb)

keep if mega == 1 & nonmega500 == 0 & cb500 == 0

tempfile store_mega
save `store_mega', replace

keep rssdid
duplicates drop
unique rssdid
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
keep idrssd reportdate deposits fdiccertificatenumber financialinstitutionname financialinstitutionaddress financialinstitutioncity financialinstitutionstate financialinstitutionzipcode financialinstitutionfilingtype
drop if regexm(reportdate,"2019")
g date = date(reportdate,"MDY")
format date %td
g yq = qofd(date)
format yq %tq

tab reportdate
ren fdiccertificatenumber cert
save "temp_callreport.dta", replace

preserve
keep if reportdate == "03312020"
keep idrssd deposits
ren deposits deposits_mar20
tempfile deposit_mar20
save `deposit_mar20', replace
restore

merge m:1 idrssd using `deposit_mar20', assert(3) nogen
g dln_deposits_mar20 = ln(deposits) - ln(deposits_mar20)

keep idrssd deposits deposits_mar20 dln_deposits_mar20 yq
xtset idrssd yq
ren yq lyq
g yq = F1.lyq
format yq %tq
drop if yq == .
order idrssd deposits yq lyq deposits_mar20 dln_deposits_mar20

bysort yq: egen depgr_p50 = pctile(dln_deposits_mar20), p(50)
bysort yq: egen depgr_p67 = pctile(dln_deposits_mar20), p(67)
bysort yq: egen depgr_p75 = pctile(dln_deposits_mar20), p(75)
bysort yq: egen depgr_p90 = pctile(dln_deposits_mar20), p(90)

generate Hdepgr = (dln_deposits_mar20 > depgr_p50)
generate Tdepgr = (dln_deposits_mar20 > depgr_p67)
generate Qdepgr = (dln_deposits_mar20 > depgr_p75)
generate Ddepgr = (dln_deposits_mar20 > depgr_p90)

drop *_p*
ren idrssd rssdid

tempfile deposit_rank
save `deposit_rank', replace

use `store_mega', clear 
joinby rssdid using `deposit_rank', unmatched(both)
assert _merge == 3
drop _merge

collapse (max) Hdepgr Tdepgr Qdepgr Ddepgr ///
(mean) mean_Hdepgr = Hdepgr mean_Tdepgr = Tdepgr mean_Qdepgr = Qdepgr mean_Ddepgr = Ddepgr ///
(sum) sum_Hdepgr = Hdepgr sum_Tdepgr = Tdepgr sum_Qdepgr = Qdepgr sum_Ddepgr = Ddepgr, by(placekey yq)

ren * *_only
ren placekey_only placekey
ren yq_only yq

generate year = year(dofq(yq))
generate quarter = quarter(dofq(yq))

unique placekey yq
unique placekey

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/onlymega_depgrowth_rank.dta", replace

