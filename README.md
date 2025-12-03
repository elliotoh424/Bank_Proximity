# The 500-Yard Economy: Do Megabanks Help Small Businesses?
Repository to replicate tables and figures in [**Banks on the Block: Branch Proximity and Small Business Performance**](https://www.dropbox.com/scl/fi/yve14u53k5n2yw2iap7cw/Oh-Seungmin.JMP.pdf?rlkey=kuwfib5fvebwizs93zfi3up5a&st=mh0klfh5&dl=0)

## How to run
Execute master_script.sh. Requires having both python and stata. 

# Motivation
1.  **The Narrative Myth:** The common belief is that local Community Banks are the champions of small business support, while Megabanks are distant and impersonal.
2.  **The Counter-Intuitive Reality:** Analysis of foot traffic data reveals that **Megabanks** (Assets >$100B)—not community banks—were the primary drivers of recovery.
3.  **The "Window Opportunity":** Even in the digital age, physical presence determines performance. [cite_start]The economic boost is hyper-localized to a **500-yard radius** around a branch.

# Summary of Findings

### Finding 1 — Small business nearby bank branches Outperform competitors
Independent pharmacies nearby bank branches recovered 6-7% faster than bankless competitors. Branch effect hyperlocal: only affects independent pharmacies within 500 yards.:   
![Bank Proximity](https://github.com/elliotoh424/Bank_Proximity/blob/main/output/figures/bank_proximity_raw_figure.png)

### Finding 2 — Megabanks support small businesses growth 
Contrary to the "relationship banking" narrative, independent pharmacies located near Megabanks experienced a **7-8% surge** in customer traffic compared to their bankless competitors.
![Mega Bank Outperformance](https://github.com/elliotoh424/Bank_Proximity/blob/main/output/figures/megabank_proximity_coefficents.png)

### Finding 3 — Community banks had no effect on small businesses growth 
Independent pharmacies located near community banks recovered at the *same pace* as those with no bank.

![Community Bank No Effect](https://github.com/elliotoh424/Bank_Proximity/blob/main/output/figures/cb_proximity_coefficients.png)

### Finding 4 — Regulatory pressure forces megabanks to support small businesses 
Independent pharmacies located near mega banks recovered faster by 10-11% during bank examination periods. No effect outside of examination periods.

![Only around Examinations](https://github.com/elliotoh424/Bank_Proximity/blob/main/output/figures/examination_reg.png)
.
## 📁 Repository Structure
```
pharmacy-bank-proximity/
│
├── data_pipeline/              # Data construction (run in order)
│   ├── 01_sample_construction/
│   │   ├── 01_filter_pharmacy_sample.py
│   │   └── 02_calc_microarea_metrics.py
│   │
│   └── 02_dataset_preparation/
│       ├── 03_merge_analysis_datasets.do
│       └── 04_clean_regression_sample.do
│
└── analysis/                   # Analysis (no required order)
    ├── 01_tables/
    │   ├── table_bank_did.do          
    │   ├── table_banktype_did.do      
    │   └── table_banksize_did.do      
    │
    └── 02_figures/
        ├── fig_event_study_coefficients.do    
        ├── fig_raw_trends.do                   
        └── fig_distance_histogram_3d.py         
```

---

## 🔄 Pipeline Details
### Stage 1: Sample Construction (01_sample_construction/)
#### 01_filter_pharmacy_sample.py
Creates sample of pharmacies with reliable foot traffic data. Remove pharmacies in grocery stores, colocated with other stores, and with data quality issue. 


#### 02_calc_microarea_metrics.py (45 min)
Generate microarea (500 yard radius) business density variables for eah pharmacy

### Stage 2: Dataset Preparation (02_dataset_preparation/)
#### 03_merge_analysis_datasets.do
Combines pharmacy sample with demographic, lending, and business density data

**Data sources merged:**
1. CRA tract-level lending (2019) - small business loan volumes
2. ACS 5-year demographics (2019) - population density, education, income
3. Pharmacy foot traffic (2019-2022) - monthly visits from Stage 1
4. Bank proximity (2019) - distance to nearest community/non-community bank
5. Microarea metrics (2019-2022) - business density from Stage 1

#### 04_clean_regression_sample.do (10 min)
Standardize and log transform variables for analysis.

## 📈 Analysis Files

All analysis files run independently after data construction. No required execution order.

### Tables (01_tables/)
**Baseline Regression equation:**
```
dln(traffic_it) = β1(Bank_i × Period1_t) + β2(Bank_i × Period2_t) + β3(Bank_i × Period3_t) + φ_ct + ε_it for store i,county c, and time t. 
```
**Controls:** Time-varying Microarea foot traffic, cross-sectional demographics interacted with time dummies

#### table_bank_did.do
DID comparing independent pharmacies near banks to those without nearby bank

#### table_banktype_did.do
DID comparing independent pharmacies near different bank types (community banks vs non-community banks) to those without nearby bank

#### table_banksize_did.do
DID comparing independent pharmacies near different bank sizes (community banks vs non-mega banks vs. mega banks) to those without nearby bank

### Figures (02_figures/)

#### fig_event_study_coefficients.do
Event study plot showing month-by-month treatment effects (relative to brand pharmacies)

#### fig_raw_trends.do
Monthly raw average foot traffic by bank proximity (2019-2022)

#### fig_distance_histogram_3d.py
3D histogram of pharmacy locations by distance to nearest community bank (x-axis) vs non-community bank (y-axis)
