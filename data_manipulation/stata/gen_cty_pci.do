use "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/acs_drug_matched_month_radiusdef_forreg.dta", clear
keep if d_brand == 0 & mega500 == 1
keep cty
duplicates drop
tempfile ind_cty
save `ind_cty', replace

* Some independent cities in VA lumped together with other counties. Manually added rows for those missing counties
import excel using "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/bea/pci_cty_2019_revised.xlsx", cellrange(A6) firstrow clear
keep if regexm(Description, "Per capita personal income")
g cty = GeoFips
ren F pci_cty19
keep cty pci_cty19
replace pci_cty19 = "" if pci_cty19 == "(NA)"
destring pci_cty19, replace

merge 1:1 cty using `ind_cty', assert(1 3)
g sample = 1 if _merge == 3
replace sample = 0 if sample == .
drop _merge

tw (hist pci_cty19, color(blue%30) frac) (hist pci_cty19 if sample, color(red%30) frac), ///
legend(label(1 "All Counties") label(2 "Counties with Megabank-Independent Pharmacy")) xtitle("Per Capita Personal Income 2019") title("Per Capita Personal Income Distribution ")
graph export "C:/Users/elliotoh/Box/lodes_shared/pharmacy/results/figures/pci_cty_hist.png", replace

* Create indicators for top percentiles unconditionally across all variables
local var pci_cty19
count if `var' == .

* Get unconditional percentiles
quietly summarize `var', detail

* Create unconditional indicators for different thresholds
generate H`var'_nat_uncond = (`var' > `r(p50)')
generate Q`var'_nat_uncond = (`var' > `r(p75)')
generate D`var'_nat_uncond = (`var' > `r(p90)')

* For top 33%, we need to calculate the 67th percentile explicitly
quietly _pctile `var', p(67)
local p67 = r(r1)
generate T`var'_nat_uncond = (`var' > `p67')

* Label the variables
label variable H`var'_nat_uncond "Top 50% of `var' unconditionally"
label variable T`var'_nat_uncond "Top 33% of `var' unconditionally"
label variable Q`var'_nat_uncond "Top 25% of `var' unconditionally"
label variable D`var'_nat_uncond "Top 10% of `var' unconditionally"

keep cty pci_cty19 Hpci_cty19_nat_uncond Qpci_cty19_nat_uncond Dpci_cty19_nat_uncond Tpci_cty19_nat_uncond
ren *pci_cty19_nat_uncond *pciu
unique cty

save "C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/bea/all_cty_pci_rank.dta", replace

