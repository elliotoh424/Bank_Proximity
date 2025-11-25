use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/adv_drug_nearby_nonbrand_stores.dta", clear
drop tract
g naics4 = substr(naics_code_micro, 1,4)
merge m:1 placekey date_range_start nvisits month year using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", keepusing(d_nbrand start npoi500 bank500 nbank500 mega500 nmega500 nonmega500 nnonmega500 cb500 ncb500 ndrug500 dln_brn500 lnpoi500 all_500yd_chg_exc4 lmedage lmedinc pctuniv pctmnty lpdsty lmedhome pct0to10 pctpublic)
keep if _merge == 3
drop _merge

merge m:1 placekey start using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_drugstore.dta", keepusing(d_pre* d_post* treated) assert(2 3)
drop if _merge == 2
drop _merge

preserve 
keep if date_range_start == "2020-03-01"
keep placekey_micro nvisits_micro
unique placekey_micro
ren nvisits_micro nvisits_micro_mar20
tempfile mar20
save `mar20', replace
restore

merge m:1 placekey_micro using `mar20', assert(3) nogen
g dln_visits_micro_mar20 = ln(nvisits_micro) - ln(nvisits_micro_mar20)

merge m:1 naics4 using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/sba/independents_sba.dta"
keep if _merge == 3
drop _merge

g cty = substr(tract_micro, 1, 5)
egen month_id = group(start)
egen cty_ym_id = group(cty start)
egen id_ad = group(placekey_micro)

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/craexam_independents_drug.dta", replace



