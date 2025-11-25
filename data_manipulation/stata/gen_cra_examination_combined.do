* Read Bank Examination schedule. Only for banks in drug-SOD 2019.
use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/bank_examination.dta", clear
unique ID_RSSD Exam_Date
ren *_* **
ren IDRSSD ID_RSSD
replace ExamDate = dofc(ExamDate)
format %td ExamDate
keep if ExamDate >= date("2018-01-01","YMD") & ExamDate <= date("2023-12-31","YMD")
g year = year(ExamDate)
g month = month(ExamDate)
gen quarter = ceil(month/3)
unique ID_RSSD year quarter
tempfile events
save `events'

* Step 2: Create quarter offset datasets for each window
* Use a mapping to avoid negative numbers in tempfile names
forvalues t = 1/7 {
    * Map 1-7 to offsets -3 to 3
    local offset = `t' - 4   /* 1->-3, 2->-2, 3->-1, 4->0, 5->1 , 6-> 2, 7-> 3*/
    
    use `events', clear
    
    * Store original values
    gen orig_year = year
    gen orig_quarter = quarter
    gen event_time = `offset'
    
    * Adjust quarter by offset
    replace quarter = quarter + `offset'
    
    * Fix year crossovers
    replace year = year + floor((quarter-1)/4)
    replace quarter = mod(quarter-1,4) + 1
    
    * Save temporary dataset for this offset
    tempfile qpos`t'
    save `qpos`t''
}

* Step 3: Combine all quarter offset datasets
use `qpos4', clear   /* Start with offset 0 (t=4) */
forvalues t = 1/7 {  
    append using `qpos`t''
}
* Step 4: Create a mapping dataset of bank-quarter to event windows
keep ID_RSSD Cert BankName City State year quarter event_time orig_year orig_quarter Agency
duplicates drop
sort ID_RSSD year quarter event_time
unique ID_RSSD year quarter

ren ID_RSSD rssdid
* Drop 11 banks with overlapping examination windows (actual examination not due to acquisitions)
drop if rssdid == 244149 | rssdid == 261146 | rssdid == 443353 | rssdid == 529341 | rssdid == 593771 | rssdid == 663955 | rssdid == 797140 | rssdid == 911973 | rssdid == 1492817 | rssdid == 2666400 | rssdid == 2838207
unique rssdid year quarter
// Cert BankName City State year quarter Agency overlap_bank
save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/rssdid_event_windows.dta", replace





