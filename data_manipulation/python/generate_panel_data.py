# data_processing.py

import os, re, gc, time
import pathlib as path
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from datetime import timedelta

# ---------------------------
# Function Definitions
# ---------------------------


def load_adv2001(panel_path):
    """Load advan Jan 2020 panel and filter for US records."""
    adv2001 = ds.dataset(panel_path)
    adv2001 = adv2001.to_table().to_pandas()
    return adv2001[adv2001.iso_country_code=='US'].placekey

def load_monthly_data(file_path, filters=None, columns=None):
    """Load monthly parquet data with optional filtering and column selection."""
    return pd.read_parquet(file_path, filters=filters, columns=columns)


def process_initial_panels(adv2001):
    """Load the Jan and Dec 2019 panels, process them, and return cleaned dataframes."""
    # Define the columns to keep
    keep_cols = ['placekey','raw_visit_counts','poi_cbg']
    
    # Process January 2020 panel
    adv_jan2020 = load_monthly_data('D:/pharmacy/advan/monthlyfoottraffic/processed/FootTraffic_2020-01-01.parquet')
    # (This line is kept from the original code for testing CBG info)
    adv_jan2020 = adv_jan2020[adv_jan2020.visitor_home_cbgs.str.contains('\d', na=False)]
    adv_jan2020 = load_monthly_data(
        'D:/pharmacy/advan/monthlyfoottraffic/processed/FootTraffic_2020-01-01.parquet',
        filters=[('placekey', 'in', adv2001)], columns=keep_cols
    )
    adv_jan2020 = adv_jan2020[~adv_jan2020.poi_cbg.isnull()].drop('poi_cbg', axis=1)
    adv_jan2020.rename(columns={'raw_visit_counts': 'nvisits_jan2020'}, inplace=True)

    # Process December 2019 panel
    adv_dec2019 = load_monthly_data(
        'D:/pharmacy/advan/monthlyfoottraffic/processed/FootTraffic_2019-12-01.parquet',
        filters=[('placekey', 'in', adv2001)], columns=keep_cols
    )
    adv_dec2019 = adv_dec2019[~adv_dec2019.poi_cbg.isnull()].drop('poi_cbg', axis=1)
    adv_dec2019.rename(columns={'raw_visit_counts': 'nvisits_dec2019'}, inplace=True)
    
    return adv_jan2020, adv_dec2019

#########################################################################################################################################
# Create balanced panel for full sample and filtered sample (no polygon issues) per month and append to master panel file
#########################################################################################################################################
def process_monthly_panels(adv_datafiles, adv2001, adv_jan2020, adv_dec2019, full_data_path, filtered_data_path):
    """Loop over monthly files, merge header and data, and write out full and filtered panels."""
    t1 = time.time()
    for i, f in enumerate(adv_datafiles):
        date = re.search(r'\d{4}-\d{2}-01', f).group(0)
        cwfile = 'FootTraffic_data_' + date + '.csv.gz'
        mefile = 'FootTraffic_' + date + '.parquet'
        
        # All variables from raw file
        # cols = ['placekey', 'parent_placekey', 'safegraph_brand_ids', 'location_name', 'brands', 'store_id', 'top_category', 'sub_category', 'naics_code', 'latitude', 'longitude', 'street_address', 'city', 'region', 'postal_code', 'open_hours', 'category_tags', 'opened_on', 'closed_on', 'tracking_closed_since', 'websites', 'geometry_type', 'polygon_wkt', 'polygon_class', 'enclosed', 'phone_number', 'is_synthetic', 'includes_parking_lot', 'iso_country_code', 'wkt_area_sq_meters', 'date_range_start', 'date_range_end', 'raw_visit_counts', 'raw_visitor_counts', 'visits_by_day', 'poi_cbg', 'visitor_home_cbgs', 'visitor_home_aggregation', 'visitor_daytime_cbgs', 'visitor_country_of_origin', 'distance_from_home', 'median_dwell', 'bucketed_dwell_times', 'related_same_day_brand', 'related_same_month_brand', 'popularity_by_hour', 'popularity_by_day', 'device_type', 'normalized_visits_by_state_scaling', 'normalized_visits_by_region_naics_visits', 'normalized_visits_by_region_naics_visitors', 'normalized_visits_by_total_visits', 'normalized_visits_by_total_visitors']

        # Read Chenyang (cw) file
        cw_cols = ['placekey', 'raw_visit_counts', 'raw_visitor_counts', 'visits_by_day_std', 
                   'visitor_same_state', 'visitor_same_county', 'visitor_same_tract',
                   'n_us_visitors', 'n_nonus_visitors', 'n_dwell_times_5', 'n_dwell_times_5_20', 
                   'n_dwell_times_21_60', 'n_dwell_times_61_240', 'n_dwell_times_240_']
        a1 = pd.read_csv(os.path.join('D:/pharmacy/advan/monthlyfoottraffic/chenyang/processed/', cwfile), 
                         usecols=cw_cols, dtype=str)
        a1 = a1[a1.placekey.isin(adv2001)]
        for c in ['raw_visit_counts','raw_visitor_counts','visitor_same_county']:
            a1[c] = pd.to_numeric(a1[c])
        assert a1[a1.placekey.duplicated()].shape[0] == 0
        # Read metadata (me) file
        me_cols = ['placekey', 'parent_placekey', 'safegraph_brand_ids', 'location_name', 'brands',
                   'top_category', 'sub_category', 'naics_code', 'latitude', 'longitude', 'street_address', 
                   'city', 'region', 'postal_code', 'open_hours', 'category_tags', 'opened_on', 'closed_on',
                   'tracking_closed_since', 'websites', 'geometry_type', 'polygon_class', 'enclosed',
                   'phone_number', 'is_synthetic', 'wkt_area_sq_meters', 'date_range_start', 'date_range_end',
                   'raw_visit_counts', 'raw_visitor_counts', 'visits_by_day', 'poi_cbg', 'distance_from_home', 
                   'median_dwell', 'popularity_by_hour', 'popularity_by_day', 'device_type',
                   'normalized_visits_by_state_scaling', 'normalized_visits_by_region_naics_visits',
                   'normalized_visits_by_region_naics_visitors', 'normalized_visits_by_total_visits', 
                   'normalized_visits_by_total_visitors']
        a2 = pd.read_parquet(os.path.join('D:/pharmacy/advan/monthlyfoottraffic/processed/', mefile),
                             columns=me_cols)
        a2 = a2[a2.placekey.isin(adv2001)]
        for c in ['raw_visit_counts','raw_visitor_counts','distance_from_home', 'median_dwell']:
            a2[c] = pd.to_numeric(a2[c])
            
        # Merge the two dataframes
        a3 = a1.merge(a2, on=['placekey','raw_visit_counts','raw_visitor_counts'], how='outer', indicator=True)
        assert a3[a3._merge!='both'].shape[0] == 0, "Merge error between a1 and a2"
        a3.drop('_merge', axis=1, inplace=True)
        
        # Format date columns
        a3['date_range_start'] = pd.to_datetime(a3.date_range_start).dt.date.astype(str)
        a3['date_range_end'] = pd.to_datetime(a3.date_range_end).dt.date.astype(str)
        print(f, a3[a3.poi_cbg.isnull()].shape[0] / a3.shape[0])
        a3 = a3[~a3.poi_cbg.isnull()]
        a3['poi_cbg'] = a3.poi_cbg.str.zfill(12)
        
        # Merge with Jan and Dec 2019 panels
        a3 = a3.merge(adv_jan2020, on='placekey', how='outer', indicator=True)
        assert a3[a3._merge!='both'].shape[0] == 0, "Merge error with Jan 2020"
        a3.drop('_merge', axis=1, inplace=True)
        a3 = a3.merge(adv_dec2019, on='placekey', how='outer', indicator=True)
        assert a3[a3._merge!='both'].shape[0] == 0, "Merge error with Dec 2019"
        a3.drop('_merge', axis=1, inplace=True)
        for c in ['nvisits_jan2020','nvisits_dec2019']:
            a3[c] = pd.to_numeric(a3[c])
            
        a3.rename(columns={'postal_code': 'zip', 'raw_visit_counts':'nvisits'}, inplace=True)
        a3['tract'] = a3.poi_cbg.str[:11]
        a3['cty'] = a3.poi_cbg.str[:5]
        a3['date_range_start'] = pd.to_datetime(a3.date_range_start)
        a3['date_range_end'] = pd.to_datetime(a3.date_range_end)
        a3['year'] = a3.date_range_start.dt.year
        a3['date_range_start'] = a3.date_range_start.dt.date.astype(str)
        a3['date_range_end'] = a3.date_range_end.dt.date.astype(str)
        
        # Save full panel
        if i == 0:
            a3.to_parquet(full_data_path, engine='fastparquet', compression='zstd')
        else:
            a3.to_parquet(full_data_path, engine='fastparquet', append=True, compression='zstd')
            
        # Process filtered panel
        # a3 = a3[(~a3.zip.isnull()) & (a3.zip!='0')]
        # a3['zip'] = a3.zip.str.zfill(5)
        a3['date_range_start'] = pd.to_datetime(a3.date_range_start)
        a3['date_range_end'] = pd.to_datetime(a3.date_range_end)
        a3['year'] = a3.date_range_start.dt.year
        a3['month'] = a3.date_range_start.dt.month
        a3['d_brand'] = (~a3.safegraph_brand_ids.isnull()).astype(int)
        a3['nvisits_std1'] = a3.nvisits / a3.nvisits_jan2020
        assert a3[a3[['placekey','date_range_start']].duplicated()].shape[0] == 0, "Duplicates found!"
        a3 = a3[a3.nvisits_jan2020 >= 10]
        a3['nobs'] = a3.groupby(['placekey'])['date_range_start'].transform('count')
        a3['syear'] = a3.groupby('placekey')['date_range_start'].transform('min')
        a3['eyear'] = a3.groupby('placekey')['date_range_start'].transform('max')
        a3['nobs1'] = (a3.eyear.dt.year - a3.syear.dt.year) * 12 + (a3.eyear.dt.month - a3.syear.dt.month) + 1
        assert a3[a3.nobs != a3.nobs1].shape[0] == 0, "Observations count mismatch!"
        a3.drop(['nobs','syear','eyear','nobs1'], axis=1, inplace=True)
        a3['date_range_start'] = a3.date_range_start.astype(str)
        a3['date_range_end'] = a3.date_range_end.astype(str)
        
        keep_cols = ['placekey', 'parent_placekey', 'safegraph_brand_ids', 'location_name', 'brands',
                     'top_category', 'sub_category', 'naics_code', 'latitude', 'longitude', 'street_address',
                     'city', 'region', 'zip', 'open_hours', 'category_tags', 'opened_on', 'closed_on',
                     'tracking_closed_since', 'websites', 'geometry_type', 'polygon_class', 'enclosed',
                     'phone_number', 'is_synthetic', 'wkt_area_sq_meters', 'date_range_start', 'date_range_end',
                     'nvisits', 'nvisits_std1', 'nvisits_jan2020', 'nvisits_dec2019',
                     'raw_visitor_counts', 'visits_by_day', 'poi_cbg', 'distance_from_home', 'median_dwell',
                     'popularity_by_hour', 'popularity_by_day', 'device_type', 'normalized_visits_by_state_scaling',
                     'normalized_visits_by_region_naics_visits', 'normalized_visits_by_region_naics_visitors',
                     'normalized_visits_by_total_visits', 'normalized_visits_by_total_visitors',
                     'visits_by_day_std', 'visitor_same_state', 'visitor_same_county', 'visitor_same_tract',
                     'n_us_visitors', 'n_nonus_visitors', 'n_dwell_times_5', 'n_dwell_times_5_20',
                     'n_dwell_times_21_60', 'n_dwell_times_61_240', 'n_dwell_times_240_',
                     'tract', 'd_brand', 'year', 'month']
        a3 = a3[keep_cols]
        a3.columns = a3.columns.str.replace('normalized_', 'norm_')
        a3.columns = a3.columns.str.replace('_region_naics_', '_reg_nc_')
        
        if i == 0:
            a3.to_parquet(filtered_data_path, engine='fastparquet', compression='zstd')
        else:
            a3.to_parquet(filtered_data_path, engine='fastparquet', append=True, compression='zstd')
            
        del a1, a2, a3 
        gc.collect()
        t2 = time.time()
        print(f, (t2 - t1) / 60)

def create_header_file(header_cols, parquet_path, output_csv):
    """Create header CSV from a parquet dataset using a subset of columns and date formatting."""
    adv_header = ds.dataset(parquet_path)
    adv_header = adv_header.to_table(filter=((ds.field('year')>=2020)), columns=header_cols).to_pandas()
    adv_header['date_range_start'] = adv_header.date_range_start.dt.date.astype(str)
    adv_header['date_range_end'] = adv_header.date_range_end.dt.date.astype(str)
    adv_header.to_csv(output_csv, index=False)
    return adv_header

def aggregate_retailer_data(retailer_parquet_path, shared_polygon_path, output_csv, output_stata):
    """Aggregate retailer data to tract level and save to CSV and Stata formats."""
    save_cols = ['placekey','poi_cbg','tract','date_range_start','month','location_name',
                 'street_address', 'city', 'region','zip', 'brands','d_brand','nvisits',
                 'nvisits_2019','nvisits_jan2020','distance_from_home','naics_code','wkt_area_sq_meters',
                 'median_dwell']
    retailer_full = pd.read_parquet(retailer_parquet_path, columns=save_cols)
    retailer_full = retailer_full[~retailer_full.date_range_start.str.contains('2019', na=False)]
    
    # Check that shared polygons are not present.
    shared = pd.read_stata(shared_polygon_path)
    assert retailer_full[retailer_full.placekey.isin(shared.placekey_primary)].shape[0] == 0
    assert retailer_full[retailer_full.placekey.isin(shared.placekey_shared)].shape[0] == 0
    
    retailer_full.to_csv(output_csv, index=False)
    retailer_full.to_stata(output_stata, write_index=False, version=118)
    return retailer_full


# stata_writer.py

import os, re, gc
import pandas as pd
import numpy as np
import pyarrow.dataset as ds

def write_stata(header_cols, data_cols, naics, filename_full, filename_filtered):
    """Create industry-specific panel, scale visit counts and write to parquet files for Stata use."""
    # Filtered data processing
    ds_path_filtered = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_filtered.parquet'
    naics_list = ds.dataset(ds_path_filtered)
    naics_list = naics_list.to_table(filter=((ds.field('year')>=2020)), columns=['naics_code']).to_pandas()
    naics_list = naics_list[naics_list.naics_code.str.contains('^' + naics, na=False)]
    naics_list = naics_list.naics_code.drop_duplicates().sort_values().tolist()
    
    cols = list(set(header_cols + data_cols))
    data = ds.dataset(ds_path_filtered)
    data19 = data.to_table(
        filter=((ds.field('year')==2019)) & (ds.field('naics_code').isin(naics_list)),
        columns=['placekey','date_range_start','nvisits','raw_visitor_counts','distance_from_home']
    ).to_pandas()
    data19['raw_visitor_counts'] = pd.to_numeric(data19['raw_visitor_counts'])
    data19.rename(columns={'nvisits':'nvisits_2019', 'raw_visitor_counts':'nvisitors_2019',
                           'distance_from_home':'distance_from_home_2019'}, inplace=True)
    data19['date_range_start'] = pd.to_datetime(data19.date_range_start)
    data19['month'] = data19.date_range_start.dt.month
    data19 = data19[['placekey','month','nvisits_2019','nvisitors_2019','distance_from_home_2019']]
    
    data20 = data.to_table(filter=(ds.field('naics_code').isin(naics_list)), columns=cols).to_pandas()
    data20['date_range_start'] = pd.to_datetime(data20.date_range_start)
    data20['month'] = data20.date_range_start.dt.month
    # Exclude POINT POIs
    point_poi = data20[data20.geometry_type=='POINT'].placekey
    data20 = data20[data20.geometry_type!='POINT']
    data19 = data19[~data19.placekey.isin(point_poi)]
    data20 = data20.merge(data19, on=['placekey','month'], how='outer', indicator=True)
    assert data20[data20._merge!='both'].shape[0] == 0, "Merge error in filtered data"
    data20['nvisitors'] = pd.to_numeric(data20.raw_visitor_counts)
    data20['nvisits_std1'] = data20.nvisits / data20.nvisits_2019
    data20['nvisitors_std1'] = data20.nvisitors / data20.nvisitors_2019
    data20.drop('_merge', axis=1, inplace=True)
    data20['closed_on'] = data20.closed_on.astype(str)
    data20['d_brand'] = (~data20.safegraph_brand_ids.isnull()).astype(int)
    assert data20[data20[['placekey','date_range_start']].duplicated()].shape[0] == 0, "Duplicates found in filtered data"
    data20.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/' + filename_filtered + '.parquet', index=False, compression='zstd')
    data20.to_parquet('D:/pharmacy/advan/' + filename_filtered + '.parquet', index=False, compression='zstd')
    print('Filtered panel, unique placekeys:', data20.placekey.drop_duplicates().shape[0])
    
    del data19, data20
    gc.collect()
    
    # Full data processing (similar structure)
    ds_path_full = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet'
    naics_list = ds.dataset(ds_path_full)
    naics_list = naics_list.to_table(filter=((ds.field('year')>=2020)), columns=['naics_code']).to_pandas()
    naics_list = naics_list[naics_list.naics_code.str.contains('^' + naics, na=False)]
    naics_list = naics_list.naics_code.drop_duplicates().sort_values().tolist()
    
    cols = list(set(header_cols + data_cols))
    cols = [re.sub('norm_', 'normalized_', x) for x in cols]
    cols = [re.sub('_reg_nc_', '_region_naics_', x) for x in cols]
    
    data = ds.dataset(ds_path_full)
    data19 = data.to_table(
        filter=((ds.field('year')==2019)) & (ds.field('naics_code').isin(naics_list)),
        columns=['placekey','date_range_start','nvisits','raw_visitor_counts','distance_from_home']
    ).to_pandas()
    data19['raw_visitor_counts'] = pd.to_numeric(data19['raw_visitor_counts'])
    data19.rename(columns={'nvisits':'nvisits_2019','raw_visitor_counts':'nvisitors_2019',
                           'distance_from_home':'distance_from_home_2019'}, inplace=True)
    data19['date_range_start'] = pd.to_datetime(data19.date_range_start)
    data19['month'] = data19.date_range_start.dt.month
    data19 = data19[['placekey','month','nvisits_2019','nvisitors_2019','distance_from_home_2019']]
    
    data20 = data.to_table(filter=(ds.field('naics_code').isin(naics_list)), columns=cols).to_pandas()
    data20['date_range_start'] = pd.to_datetime(data20.date_range_start)
    data20['month'] = data20.date_range_start.dt.month
    point_poi = data20[data20.geometry_type=='POINT'].placekey
    data20 = data20[data20.geometry_type!='POINT']
    data19 = data19[~data19.placekey.isin(point_poi)]
    data20 = data20.merge(data19, on=['placekey','month'], how='outer', indicator=True)
    assert data20[data20._merge!='both'].shape[0] == 0, "Merge error in full data"
    data20['nvisitors'] = pd.to_numeric(data20.raw_visitor_counts)
    data20['nvisits_std1'] = data20.nvisits / data20.nvisits_2019
    data20['nvisitors_std1'] = data20.nvisitors / data20.nvisitors_2019
    data20.drop('_merge', axis=1, inplace=True)
    data20['closed_on'] = data20.closed_on.astype(str)
    data20['d_brand'] = (~data20.safegraph_brand_ids.isnull()).astype(int)
    assert data20[data20[['placekey','date_range_start']].duplicated()].shape[0] == 0, "Duplicates found in full data"
    
    lodes_shared
    data20.to_parquet('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/' + filename_full + '.parquet', index=False, compression='zstd')
    data20.to_parquet('D:/pharmacy/advan/' + filename_full + '.parquet', index=False, compression='zstd')
    print('Full panel, unique placekeys:', data20.placekey.drop_duplicates().shape[0])

def write_stata_tract(header_cols, data_cols, naics, filename_full, filename_filtered):
    """Create tract-level aggregated panel and write to parquet and Stata files."""
    ds_path_full = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet'
    naics_list = ds.dataset(ds_path_full)
    naics_list = naics_list.to_table(filter=((ds.field('year')>=2020)), columns=['naics_code']).to_pandas()
    naics_list = naics_list[naics_list.naics_code.str.contains('^' + naics, na=False)]
    naics_list = naics_list.naics_code.drop_duplicates().sort_values().tolist()
    
    data = ds.dataset(ds_path_full)
    point_poi = data.to_table(filter=(ds.field('naics_code').isin(naics_list)), 
                              columns=['placekey','geometry_type']).to_pandas()
    point_poi = point_poi[point_poi.geometry_type=='POINT'].placekey
    
    data19 = data.to_table(filter=(ds.field('naics_code').isin(naics_list)),
                           columns=['placekey','date_range_start','tract','nvisits','raw_visitor_counts','naics_code']
                          ).to_pandas()
    data19['raw_visitor_counts'] = pd.to_numeric(data19['raw_visitor_counts'])
    data19.rename(columns={'raw_visitor_counts':'nvisitors'}, inplace=True)
    data19['date_range_start'] = pd.to_datetime(data19.date_range_start)
    data19['month'] = data19.date_range_start.dt.month
    data19 = data19[~data19.placekey.isin(point_poi)]
    
    # Example aggregation: count POIs and sum visits per tract
    agg_dict = {
        'placekey': 'count',
        'nvisits': ['sum','mean'],
        'nvisitors': 'sum'
    }
    data19 = data19.groupby(['tract','date_range_start','month']).agg(agg_dict).reset_index()
    data19.columns = ['_'.join(col).strip() if type(col) is tuple else col for col in data19.columns.values]
    data19.rename(columns={'placekey_count': 'npoi'}, inplace=True)
    
    full_parquet = 'D:/pharmacy/advan/' + filename_full + '.parquet'
    data19.to_parquet(full_parquet, index=False, compression='zstd')
    data19.to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/' + filename_full + '.dta', write_index=False, version=118)
    print('Full tract-level panel, unique tracts:', data19['tract_'].drop_duplicates().shape[0])
    
    del data19
    gc.collect()
    
    ds_path_filtered = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_filtered.parquet'
    naics_list = ds.dataset(ds_path_filtered)
    naics_list = naics_list.to_table(filter=((ds.field('year')>=2020)), columns=['naics_code']).to_pandas()
    naics_list = naics_list[naics_list.naics_code.str.contains('^' + naics, na=False)]
    naics_list = naics_list.naics_code.drop_duplicates().sort_values().tolist()
    
    data = ds.dataset(ds_path_filtered)
    point_poi = data.to_table(filter=(ds.field('naics_code').isin(naics_list)),
                              columns=['placekey','geometry_type']).to_pandas()
    point_poi = point_poi[point_poi.geometry_type=='POINT'].placekey
    
    data19 = data.to_table(filter=(ds.field('naics_code').isin(naics_list)),
                           columns=['placekey','date_range_start','tract','nvisits','raw_visitor_counts','naics_code']
                          ).to_pandas()
    data19['raw_visitor_counts'] = pd.to_numeric(data19['raw_visitor_counts'])
    data19.rename(columns={'raw_visitor_counts':'nvisitors'}, inplace=True)
    data19['date_range_start'] = pd.to_datetime(data19.date_range_start)
    data19['month'] = data19.date_range_start.dt.month
    data19 = data19[~data19.placekey.isin(point_poi)]
    
    data19 = data19.groupby(['tract','date_range_start','month']).agg(agg_dict).reset_index()
    data19.columns = ['_'.join(col).strip() if type(col) is tuple else col for col in data19.columns.values]
    data19.rename(columns={'placekey_count': 'npoi'}, inplace=True)
    
    filtered_parquet = 'D:/pharmacy/advan/' + filename_filtered + '.parquet'
    data19.to_parquet(filtered_parquet, index=False, compression='zstd')
    data19.to_stata('C:/Users/elliotoh/Box/lodes_shared/data/office/advan/' + filename_filtered + '.dta', write_index=False, version=118)
    print('Filtered tract-level panel, unique tracts:', data19['tract_'].drop_duplicates().shape[0])
    del data19
    gc.collect()

# ---------------------------
# Execution Block for Testing
# ---------------------------
if __name__ == '__main__':
    # Sample execution for testing purposes.
    panel_name = 'D:/pharmacy/advan/monthlyfoottraffic/processed/FootTraffic_2020-01-01.parquet'
    full_data_path = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_full.parquet'
    filtered_data_path = 'D:/pharmacy/advan/all_foottraffic_monthly_201901_202211_filtered.parquet'
    header_csv_path = 'D:/pharmacy/advan/all_foottraffic_header.csv'
    
    # Load base data
    adv2001 = load_adv2001(panel_name)
    print("Loaded adv2001 with", len(adv2001), "records")
    
    # Process January and December panels
    adv_jan2020, adv_dec2019 = process_initial_panels(adv2001)
    print("Processed Jan and Dec panels")
    
    # Build list of monthly files
    data_dir = 'D:/pharmacy/advan/monthlyfoottraffic/processed/'
    adv_datafiles = [x for x in os.listdir(data_dir) 
                     if re.search('FootTraffic_data_202[012]', x) or re.search('FootTraffic_data_2019', x)]
    adv_datafiles = [x for x in adv_datafiles if not re.search('2022-12-01', x)]
    
    # Process monthly panels (this will loop over each file in adv_datafiles)
    process_monthly_panels(adv_datafiles, adv2001, adv_jan2020, adv_dec2019, full_data_path, filtered_data_path)
    print("Processed monthly panels")
    
    # Create header CSV
    header_cols = ['placekey', 'parent_placekey', 'safegraph_brand_ids', 'location_name', 'brands', 
                   'top_category', 'sub_category', 'naics_code', 'latitude', 'longitude', 'street_address', 
                   'city', 'region', 'zip', 'open_hours', 'category_tags', 'opened_on', 'closed_on', 
                   'tracking_closed_since', 'websites', 'geometry_type', 'polygon_class', 'enclosed', 
                   'phone_number', 'is_synthetic', 'wkt_area_sq_meters', 'date_range_start', 'date_range_end']
    create_header_file(header_cols, filtered_data_path, header_csv_path)
    print("Created header file")
    
    # (Optional) You could also call aggregate_retailer_data here with appropriate file paths.
    # Sample header and data column lists for testing
    header_cols = ['placekey', 'parent_placekey', 'safegraph_brand_ids', 'location_name', 'brands', 
                   'top_category', 'sub_category', 'naics_code', 'latitude', 'longitude', 'street_address', 
                   'city', 'region', 'zip', 'open_hours', 'category_tags', 'opened_on', 'closed_on', 
                   'tracking_closed_since', 'websites', 'geometry_type', 'polygon_class', 'enclosed', 
                   'phone_number', 'is_synthetic', 'wkt_area_sq_meters', 'date_range_start', 'date_range_end']
    data_cols = ['placekey', 'date_range_start', 'date_range_end', 'naics_code', 'wkt_area_sq_meters',
                 'tract', 'nvisits', 'nvisits_jan2020', 'nvisits_dec2019', 'raw_visitor_counts', 'visits_by_day',
                 'poi_cbg', 'distance_from_home', 'median_dwell', 'popularity_by_hour', 'popularity_by_day',
                 'device_type', 'norm_visits_by_state_scaling', 'norm_visits_by_reg_nc_visits', 
                 'norm_visits_by_reg_nc_visitors', 'norm_visits_by_total_visits', 'norm_visits_by_total_visitors',
                 'visits_by_day_std', 'visitor_same_state', 'visitor_same_county', 'visitor_same_tract',
                 'n_us_visitors', 'n_nonus_visitors', 'n_dwell_times_5', 'n_dwell_times_5_20',
                 'n_dwell_times_21_60', 'n_dwell_times_61_240', 'n_dwell_times_240_']
    
    # Execute the industry-specific panel function for a given NAICS pattern (e.g., for drugstores)
    write_stata(header_cols, data_cols, '446110', 'adv_drugstores_full', 'adv_drugstores_balanced')
    print("Executed write_stata for NAICS '446110'")
    
    # Execute the tract-level panel function
    write_stata_tract(header_cols, data_cols, '|', 'adv_allpoi_tract_full', 'adv_allpoi_tract_balanced')
    print("Executed write_stata_tract for tract-level aggregation")

