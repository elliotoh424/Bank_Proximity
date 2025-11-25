# Data construction steps
python /data_pipeline/01_sample_construction/01_filter_pharmacy_sample.py
python /data_pipeline/01_sample_construction/02_calc_microarea_metrics.py
stata /data_pipeline/02_dataset_preparation/03_merge_analysis_datasets.do
stata /data_pipeline/02_dataset_preparation/04_clean_regression_sample.do

# Analysis steps (All codes can be executed independently after data construction steps)
# Tables
stata /data_pipeline/analysis/01_tables/table_bank_did.do
stata /data_pipeline/analysis/01_tables/table_banktype_did.do      
stata /data_pipeline/analysis/01_tables/table_banksize_did.do      

# Figures
stata /data_pipeline/analysis/02_figures/fig_event_study_coefficients.do 
stata /data_pipeline/analysis/02_figures/fig_raw_trends.do 
stata /data_pipeline/analysis/02_figures/fig_distance_histogram_3d.py