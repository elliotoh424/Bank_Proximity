import os
import pandas as pd
import numpy as np
from functools import reduce
import zipfile
import re
import patoolib
import tarfile
import geopandas as gpd

# Occupation codes
# 566: B24114,B24115, B24116, B24121, B24122, B24123, B24124, B24125,B24126 (not available at cbg level)
# 303: B24010 (not available at cbg level), 
# 114: B24020 (not available at cbg level)
# 23: C24010, C24020, B24012, B24022, B24011, B24021, C24060
# 3 digit NAICS: B24040 (not available at cbg level)
# more granular 2 digit NAICS: B24030 (not available at cbg level)
# NAICS less granular: B24031, B24032, C24040, B24050, C24050, B24070

# C24010 civilian 16+ years includes part-time.
# C24050 (NAICS 2 digit less granular by occupation 5).
# C24030 (NAICS 2 digits more granular)

name_vars = pd.read_csv('E:/pharmacy/acs/name_dict.csv', header=None)
name_vars.columns = ['Table\nID','stata_vars']
name_vars['Unique ID'] = name_vars['Table\nID'].str.replace('_','')
name_vars = name_vars.drop('Table\nID',axis=1) 

# Add table of interest to this file
acs_voi = pd.read_csv('E:/pharmacy/acs/acs_voi.csv', dtype=str)
drop_cols = [x for x in acs_voi.columns.tolist() if re.search('Unnamed',x)]
acs_voi = acs_voi.drop(drop_cols, axis=1)
acs_voi['Unique ID'] = acs_voi['Unique ID'].str.replace('_','')
name_vars = name_vars.merge(acs_voi, on='Unique ID', how='left', indicator=True)
assert name_vars[name_vars._merge!='both'].shape[0] == 0
name_vars = name_vars.drop('_merge',axis=1)
name_vars.to_csv('E:/pharmacy/acs/stata_vars.csv',index=False)

name_dict = pd.read_csv('E:/pharmacy/acs/name_dict.csv', header=None, index_col=0).to_dict()[1]

######
# Name variable of interest from ACS table here.
def acs_var_gen(df, table_id, lin_start, lin_end, var_name):
    group_cols = [table_id+'_'+str(i).zfill(3) for i in range(lin_start,lin_end+1)]
    if table_id == 'B25077': # Found error due to some values being .
        df[var_name] = df[group_cols].astype(str)
        df[var_name] = df[var_name].replace('.',np.nan)
        df[var_name] = df[var_name].astype(float)
        # df[var_name] = df[var_name].str.replace('.','0').astype(int)
    else:
        df[var_name] = df[group_cols].astype('float').sum(axis=1)
        # df[var_name] = df[group_cols].fillna(0).astype('int').sum(axis=1)
    df.drop(columns = group_cols, inplace=True)
    return print(f'{var_name} created.{group_cols} dropped.')



def gen_acs_file_name(file_type, year, period, state, sequence=None):
    st = state.lower()
    if file_type == 'g':
        return f'{file_type}{year}{period}{st}.csv'
    else:
        sequence = str(sequence).zfill(4)
        return f'{file_type}{year}{period}{st}{sequence}000.txt'

def process_acs(year,period):
    title = ['FILEID','FILETYPE','STUSAB','CHARITER','SEQUENCE','LOGRECNO']
    output_path = f'E:/pharmacy/acs/acs_5yr_processed/{year}'
    os.makedirs(output_path, exist_ok=True)
    #
    root_path = f'E:/pharmacy/acs/acs_summary/{year}/5_year_entire_sf'
    # check if f'{year}_ACS_Geography_Files.zip' and Tracts_Block_Groups_Only.zip exist
    geo_files = f'{year}_ACS_Geography_Files'
    data_files = 'Tracts_Block_Groups_Only'
    data_path = f'E:/pharmacy/acs/acs_summary/{year}/5_year_entire_sf/Tracts_Block_Groups_Only'
    data_path2 = f'E:/pharmacy/acs/acs_summary/{year}/5_year_entire_sf/{year}_ACS_Geography_Files'
    if not (os.path.exists(data_path) and os.path.exists(data_path2)):
        for file in os.listdir(root_path):
            if geo_files in file:
                patoolib.extract_archive(os.path.join(root_path,file), outdir=os.path.join(root_path,geo_files))
            elif data_files in file:
                try:
                    patoolib.extract_archive(os.path.join(root_path,file), outdir=os.path.join(root_path,data_files),verbosity=-1)
                except Exception as e:
                    tar = tarfile.open(os.path.join(root_path, file))
                    tar.extractall(path=os.path.join(root_path, data_files))
                    tar.close()
    # if f'{year}_ACS_Geography_Files.zip' in os.listdir(root_path):
    #     with zipfile.ZipFile(os.path.join(root_path, f'{year}_ACS_Geography_Files.zip'), 'r') as zip_ref:
    #         zip_ref.extractall(os.path.join(root_path,f'{year}_ACS_Geography_Files'))
    #     print(f'{year}_ACS_Geography_Files.zip extracted.')
    # else:
    #     print(f'{year}_ACS_Geography_Files.zip not found.')
    # if 'Tracts_Block_Groups_Only.zip' in os.listdir(root_path):
    #     with zipfile.ZipFile(os.path.join(root_path, 'Tracts_Block_Groups_Only.zip'), 'r') as zip_ref:
    #         zip_ref.extractall(os.path.join(root_path,'Tracts_Block_Groups_Only'))
    #     print('Tracts_Block_Groups_Only.zip extracted.')
    # a1 = pd.read_csv(r'D:\kilts\pharmacy\acs\acs_summary\2018\documentation\5_year\user_tools\ACS_5yr_Seq_Table_Number_Lookup.txt', encoding='unicode_escape')
    # a2 = pd.read_csv(r'D:\kilts\pharmacy\acs\acs_summary\2019\documentation\5_year\user_tools\ACS_5yr_Seq_Table_Number_Lookup.txt', encoding='unicode_escape')
    # a2 = a2.merge(acs_voi, left_on = 'Table ID', right_on ='Table\nID', how='outer', indicator=True)
    # a2._merge.value_counts()
    # a2[a2._merge=='right_only']['Table\nID'].drop_duplicates()
    # acs_voi
    # check if the file in data_path2 is a file
    while not os.path.isfile(os.path.join(data_path2, os.listdir(data_path2)[0])):
        data_path2 = os.path.join(data_path2, os.listdir(data_path2)[0])
    while not os.path.isfile(os.path.join(data_path, os.listdir(data_path)[0])):
        data_path = os.path.join(data_path, os.listdir(data_path)[0])
    #
    doc_path1 = 'E:/pharmacy/acs/'
    doc_path2 = f'E:/pharmacy/acs/acs_summary/{year}/documentation/user_tools'
    if not os.path.exists(doc_path2):
        doc_path2 = f'E:/pharmacy/acs/acs_summary/{year}/documentation/5_year/user_tools'
        os.makedirs(doc_path2, exist_ok=True)
    #
    # get all files in the directory
    unzipped_files = os.listdir(data_path)
    # unzip all files in data_path
    # os.makedirs(f'D:/pharmacy/acs/acs_summary/{year}/5_year_entire_sf/Tracts_Block_Groups_Only/',exist_ok=True)
    for file in unzipped_files:
        if file.endswith('.zip'):
            with zipfile.ZipFile(os.path.join(data_path, file), 'r') as zip_ref:
                zip_ref.extractall(path =f'E:/pharmacy/acs/acs_summary/{year}/5_year_entire_sf/Tracts_Block_Groups_Only/')
    del_files = [x for x in os.listdir(data_path) if re.search('.txt$|.csv$',x)]
    #
    # lookup table for variable names
    # check if the file exists
    if  os.path.exists(os.path.join(doc_path2, 'ACS_5yr_Seq_Table_Number_Lookup.txt')):
        var_lookup_f = os.path.join(doc_path2, 'ACS_5yr_Seq_Table_Number_Lookup.txt')
    else:
        var_lookup_f = os.path.join(doc_path2, 'Sequence_Number_and_Table_Number_Lookup.txt')
    var_lookup = pd.read_csv(var_lookup_f, encoding='latin1',dtype=str)
    # var_lookup.columns = var_lookup.columns.str.replace('^Line Number.+','Line Number')
    # remove rows with Line Number not missing
    var_lookup = var_lookup[~var_lookup['Line Number'].isnull()]
    # convert Line Number to numeric
    var_lookup['Line Number'] = pd.to_numeric(var_lookup['Line Number'], errors='coerce')
    var_lookup = var_lookup[~var_lookup['Line Number'].isnull()]
    # remove rows with Line Number not integer
    var_lookup = var_lookup[~var_lookup['Line Number'].apply(lambda x: x != round(x))]
    var_lookup['Line Number'] = var_lookup['Line Number'].astype(int)
    var_lookup['Line Number'] = var_lookup['Line Number'].apply(lambda x: str(x).zfill(3))
    var_lookup['vars'] = var_lookup['Table ID'] +'_' +var_lookup['Line Number']
    geo_cols = pd.read_csv(os.path.join(doc_path1,'gfile_cols.csv')).sort_values('start')
    # get the variables of interest
    acs_voi = pd.read_csv(os.path.join(doc_path1, 'acs_voi.csv'), dtype=str)
    # get tables of interest
    tables_voi = acs_voi['Table\nID'].unique().tolist()
    voi_df = pd.DataFrame({'Table ID':tables_voi})
    voi_df = voi_df.merge(var_lookup[['Table ID','Sequence Number','vars']], on='Table ID', how='left')
    # get all files in the directory
    # data_path = data_path + '/extracted/'
    files = os.listdir(data_path)
    # get all the states
    files_e = [f for f in files if f.startswith(f'e{year}{period}')]
    states_e = [f[6:8] for f in files_e]
    states_e = list(set(states_e))
    states_e = sorted(states_e)
    # get rid of 'us' from the list
    if 'us' in states_e:
        states_e.remove('us')
    print(states_e)
    #
    acs_df = []
    for st in states_e:
        print(f'Processing {st}...')
        # read the g file
        geo_df = pd.read_csv(os.path.join(data_path2, gen_acs_file_name('g',year,period,st)), dtype=str,header=None, encoding='latin1')
        # if the last three columns are all missing, drop them
        if all(geo_df.iloc[:,-3:].apply(lambda x: x.isnull().all())):
            geo_df = geo_df.iloc[:,:-3]
        # read the headers of g file
        geo_df.columns = geo_cols['vars'].tolist()
        geo_tract = geo_df[geo_df['SUMLEVEL'] == '140'][['FILEID','LOGRECNO','STATE','COUNTY','TRACT','GEOID','NAME']].copy()
        geo_tract['tract'] = geo_tract['STATE'] + geo_tract['COUNTY'] + geo_tract['TRACT']
        geo_tract.drop(['COUNTY','TRACT'], axis=1, inplace=True)
        dt_type = 'e'
        df_st_list = []
        for f_num in voi_df[~voi_df['Sequence Number'].isnull()]['Sequence Number'].unique():
            f = gen_acs_file_name(dt_type,year,period,st,int(f_num))
            col_names = var_lookup[var_lookup['Sequence Number'] == f_num]['vars'].tolist()
            col_names = title + col_names
            df = pd.read_csv(os.path.join(data_path, f), header=None, dtype=str, delimiter=',')
            df.columns = col_names
            cols_voi = voi_df[voi_df['Sequence Number'] == f_num]['vars'].tolist()
            df = df[['FILEID','LOGRECNO']+cols_voi]
            df_tract =  geo_tract.merge(df, on=['FILEID','LOGRECNO'], how='left')
            df_st_list.append(df_tract)
        df_st = reduce(lambda left,right: pd.merge(left,right,on=geo_tract.columns.to_list()), df_st_list)
        # create variables
        acs_var_gen(df_st,'B01001',3,6,'male_18below')
        acs_var_gen(df_st,'B01001',7,13,'male_18to39')
        acs_var_gen(df_st,'B01001',14,19,'male_40to64')
        acs_var_gen(df_st,'B01001',20,25,'male_65above')
        acs_var_gen(df_st,'B01001',27,30,'female_18below')
        acs_var_gen(df_st,'B01001',31,37,'female_18to39')
        acs_var_gen(df_st,'B01001',38,43,'female_40to64')
        acs_var_gen(df_st,'B01001',44,49,'female_65above')
        acs_var_gen(df_st,'B08303',2,3,'travelwork_time_0to10')
        acs_var_gen(df_st,'B08303',4,7,'travelwork_time_10to30')
        acs_var_gen(df_st,'B08303',8,11,'travelwork_time_30to60')
        acs_var_gen(df_st,'B08303',12,13,'travelwork_time_60above')       
        # df_st['male_18below']  = df_st[['B01001_'+str(i).zfill(3) for i in range(3,7)]].astype('int').sum(axis=1)
        # df_st['male_18to65'] = df_st[['B01001_'+str(i).zfill(3) for i in range(7,20)]].astype('int').sum(axis=1)
        # df_st['male_65above'] = df_st[['B01001_'+str(i).zfill(3) for i in range(20,26)]].astype('int').sum(axis=1)
        # df_st['female_18below']  = df_st[['B01001_'+str(i).zfill(3) for i in range(27,31)]].astype('int').sum(axis=1)
        # df_st['female_18to65'] = df_st[['B01001_'+str(i).zfill(3) for i in range(31,44)]].astype('int').sum(axis=1)
        # df_st['female_65above'] = df_st[['B01001_'+str(i).zfill(3) for i in range(44,50)]].astype('int').sum(axis=1)
        # df_st['travelwork_time_0to10'] = df_st[['B08303_'+str(i).zfill(3) for i in range(2,4)]].astype('int').sum(axis=1)
        # df_st['travelwork_time_10to30'] = df_st[['B08303_'+str(i).zfill(3) for i in range(4,8)]].astype('int').sum(axis=1)
        # df_st['travelwork_time_30to60'] = df_st[['B08303_'+str(i).zfill(3) for i in range(8,12)]].astype('int').sum(axis=1)
        # df_st['travelwork_time_60above'] = df_st[['B08303_'+str(i).zfill(3) for i in range(12,14)]].astype('int').sum(axis=1)
        acs_var_gen(df_st,'B15003',2,16,'edu_lesshs')
        acs_var_gen(df_st,'B15003',17,18,'edu_hsgrad')
        acs_var_gen(df_st,'B15003',19,21,'edu_somecollege')
        acs_var_gen(df_st,'B15003',22,22,'edu_bachelor')
        acs_var_gen(df_st,'B15003',23,24,'edu_master')
        acs_var_gen(df_st,'B15003',25,25,'edu_doctorate')
        acs_var_gen(df_st,'B19001',2,2,'income_0to10k')
        acs_var_gen(df_st,'B19001',3,10,'income_10to50k')
        acs_var_gen(df_st,'B19001',11,13,'income_50to100k')
        acs_var_gen(df_st,'B19001',14,16,'income_100to200k')
        acs_var_gen(df_st,'B19001',17,17,'income_200kabove')
        acs_var_gen(df_st,'B23025',1,1,'tot_pop_16_above')
        acs_var_gen(df_st,'B23025',2,2,'pop_labor_force')
        acs_var_gen(df_st,'B23025',3,3,'pop_civ_work_force')
        acs_var_gen(df_st,'B23025',4,4,'pop_employed')
        acs_var_gen(df_st,'B23025',5,5,'pop_unemployed')
        acs_var_gen(df_st,'B23025',6,6,'pop_armed')
        acs_var_gen(df_st,'B23025',7,7,'pop_not_labor_force')
        acs_var_gen(df_st,'B25077',1,1,'median_homevalue')
        if year >= 2017:
            acs_var_gen(df_st,'B28001',1,1,'pop_total_own_computer')
            acs_var_gen(df_st,'B28001',2,2,'pop_own_computer')
            acs_var_gen(df_st,'B28001',5,5,'pop_own_smartphone')
            acs_var_gen(df_st,'B28001',11,11,'pop_own_no_computer')
            acs_var_gen(df_st,'B28002',1,1,'pop_total_internet_access')
            acs_var_gen(df_st,'B28002',13,13,'pop_no_internet_access')
        df_st.rename(columns=name_dict, inplace=True)
        # select columns to keep
        header_cols = geo_tract.columns.tolist()
        renamed_cols = list(name_dict.values()) 
        generated_cols = [var for var in df_st.columns if (var not in name_dict.values()) & any([var.startswith(b) for b in ['male','female','edu','income','travelwork']]) ]
        df_st = df_st[header_cols+renamed_cols+generated_cols]
        df_st.to_csv(os.path.join(output_path, f'acs_{st}_{period}yr_{year}.csv'), index=False)
        print(f'acs_{st}_{period}yr_{year}.csv created.')
        acs_df.append(df_st)
    acs_df = pd.concat(acs_df, axis=0).reset_index(drop=True)
    for c in acs_df.columns.tolist()[6:]:
        if acs_df[c].dtype == 'O':
            acs_df[c] = acs_df[c].str.replace('^\.$','',regex=True)
            acs_df[c] = pd.to_numeric(acs_df[c])
    acs_df.info(verbose=True)
    acs_df.to_parquet('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/5yr/acs_5yr_'+str(year)+'.parquet', index=False, compression='zstd')
    # acs_df.columns[acs_df.columns.str.len()>32]
    for f in del_files:
        os.remove(data_path+'/'+f)
    return(acs_df)

acs19 = process_acs(2019, 5)
acs18 = process_acs(2018, 5)
acs17 = process_acs(2017, 5)
acs16 = process_acs(2016, 5)
acs15 = process_acs(2015, 5)
acs14 = process_acs(2014, 5)
acs13 = process_acs(2013, 5)
acs12 = process_acs(2012, 5)
acs11 = process_acs(2011, 5)
acs10 = process_acs(2010, 5)

# Process ACS variables
# 11: agriculture/fishing/forestry, 21: mining/oil/gas, 22: utilities, 23: construction, 31-33: mfg, 42: wholesale trade, 44-45: retail, 48-49: transportation/storage, 51: information, 52: finance+insurance, 53: real estate, 54: professional/science, 55: management, 56: admin/support/waste management/remediation, 61: education, 62: health care, 71: arts/recreation, 72: accomodation/food services, 81: other services, 92: public admin
def gen_acs():
    acs = pd.read_parquet("C:/Users/elliotoh/Box/lodes_shared/pharmacy/acs/acs_5yr_2019.parquet")
    acs.columns = acs.columns.str.lower()
    acs['geoid'] = acs.geoid.str[7:]

    acs['pop_age_65'] = (acs.male_65above + acs.female_65above)
    acs['pop_age_1839'] = (acs.male_18to39 + acs.female_18to39)
    acs['pop_age_4064'] = (acs.male_40to64 + acs.female_40to64)
    acs['pop_age_1864'] = (acs.male_18to39 + acs.male_40to64 + acs.female_18to39 + acs.female_40to64)
    acs['pop_edu_college'] = (acs.edu_bachelor + acs.edu_master + acs.edu_doctorate)
    acs['pop_income_100k'] = (acs.income_100to200k + acs.income_200kabove)
    acs['pop_income_200k'] = (acs.income_200kabove)
    acs['pop_minority'] = (acs.race_black + acs.race_asian + acs.race_native + acs.race_hawaii)
    acs['pop_black'] = (acs.race_black)
    acs['pop_asian'] = (acs.race_asian)
    acs['pop_selfemp'] = (acs.class_worker_selfemp_own_incorp + acs.class_worker_selfemp_own_unincorp + acs.class_worker_unpaid_family)
    #
    acs['pct_age_1839'] = acs.pop_age_1839/acs.tot_pop_age
    acs['pct_age_4064'] = acs.pop_age_4064/acs.tot_pop_age
    acs['pct_age_65'] = acs.pop_age_65/acs.tot_pop_age
    acs['pct_edu_college'] = acs.pop_edu_college/acs.edu_tot_pop
    acs['pct_income_100k'] = acs.pop_income_100k/acs.income_tot_pop
    acs['pct_income_200k'] = acs.pop_income_200k/acs.income_tot_pop
    acs['pct_minority'] = acs.pop_minority/acs.race_tot_pop
    acs['pct_black'] = acs.pop_black/acs.race_tot_pop
    acs['pct_asian'] = acs.pop_asian/acs.race_tot_pop
    acs['pct_selfemp'] = acs.pop_selfemp/acs.class_worker_tot_pop
    acs['pct_noncitizen'] = acs.noncitizen_pop/acs.tot_pop
    #
    acs['pop_working'] = acs.pop_labor_force
    acs['emp_res_ratio'] = acs.pop_labor_force/acs.tot_pop_16_above
    acs['pct_unemployed'] = acs.pop_unemployed/acs.tot_pop_16_above
    #
    assert acs[acs.occ_tot_pop != acs.occ_tot_pop_m + acs.occ_tot_pop_f].shape[0] == 0
    acs['occ_53-0000_m'] = acs['occ_53-0000_1_m'] + acs['occ_53-0000_2_m']
    acs['occ_53-0000_f'] = acs['occ_53-0000_1_f'] + acs['occ_53-0000_2_f']
    drop_cols = [x for x in acs.columns.tolist() if re.search('occ_53-0000_\d_[mf]',x)]
    acs = acs.drop(drop_cols, axis=1)
    occ_list = ['11-0000', '13-0000', '15-0000', '17-0000', '19-0000', '21-0000', '23-0000', '25-0000', '27-0000', '29-0000', '31-0000', '33-0000', '35-0000', '37-0000', '39-0000', '41-0000', '43-0000', '45-0000', '47-0000', '49-0000', '51-0000', '53-0000']
    for occ in occ_list:
        acs['occ_'+occ] = (acs[f'occ_{occ}_m'] + acs[f'occ_{occ}_f'])
    cols = ['occ_'+x for x in occ_list]
    acs['test'] = acs[cols].apply(sum,axis=1)
    assert acs[acs.occ_tot_pop != acs.test].shape[0] == 0
    naics_list = ['11','21','22','23','31-33','42','44-45','48-49','51','52','53','54','55','56','61','62','71','72','81','92']
    for nc in naics_list:
        acs['naics_'+nc] = (acs[f'naics_{nc}_m'] + acs[f'naics_{nc}_f'])
    cols = ['naics_'+x for x in naics_list]
    acs['test'] = acs[cols].apply(sum,axis=1)
    assert acs[acs.naics_tot_pop != acs.test].shape[0] == 0
    #################################################################################################################
    # Incorporate occupation code-based WFH score from Dingel and Neiman (2020)
    #################################################################################################################
    # Create tract-occcode level data and match with WFH score from Dingel and Neiman (2020)
    occ_cols = ['geoid'] + [x for x in acs.columns.tolist() if re.search('occ_\d{2}-\d{4}$',x)]
    occ_df = acs[occ_cols]
    occ_df = pd.melt(occ_df, id_vars='geoid', var_name='occ_code', value_name='occ_pop')
    occ_df['occ_code'] = occ_df['occ_code'].str.replace('^occ_','')
    occ_df = occ_df.merge(acs[['geoid','occ_tot_pop']], on=['geoid'], how='outer', indicator=True)
    assert occ_df[occ_df._merge!='both'].shape[0] == 0
    occ_df = occ_df.drop('_merge',axis=1)
    assert occ_df[occ_df[['geoid','occ_code']].duplicated()].shape[0] == 0
    occ_df['occ_pop_wt'] = occ_df.occ_pop/occ_df.occ_tot_pop
    #
    wfh = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/wfh/onet_teleworkable_blscodes.csv')
    wfh.columns = wfh.columns.str.lower()
    oes = pd.read_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/wfh/national_M2018_dl.xlsx', usecols = ['OCC_CODE', 'OCC_TITLE', 'TOT_EMP']) # Use 2018 due to OCC code consistency with WFH
    oes['occ_broad'] = oes.occ_code.str.extract('(^\d+)-\d{4}')
    #
    oes_broad = oes[oes.occ_code.str.contains('-0000', na=False)]
    oes_broad = oes_broad[['occ_broad','tot_emp']]
    oes_broad.rename(columns = {'tot_emp':'occ_broad_emp'}, inplace=True)
    oes_tot = oes_broad[oes_broad.occ_broad=='00'].occ_broad_emp.tolist()[0]
    oes_broad['occ_tot_pop'] = oes_tot
    oes_broad = oes_broad[oes_broad.occ_broad!='00']
    assert oes_broad[oes_broad.occ_broad.duplicated()].shape[0] == 0
    oes_broad['wfh_category'] = np.nan
    oes_broad.loc[oes_broad.occ_broad.str.contains('^29|^3[1357]|^4[579]|^5[13]',na=False), 'wfh_category'] = 0 # low
    oes_broad.loc[oes_broad.occ_broad.str.contains('^21|^39|^41',na=False), 'wfh_category'] = 1 #mid-low
    oes_broad.loc[oes_broad.occ_broad.str.contains('^1[79]|^27|^43',na=False), 'wfh_category'] = 2 #mid-high
    oes_broad.loc[oes_broad.occ_broad.str.contains('^1[135]|^2[35]',na=False), 'wfh_category'] = 3 #high
    assert oes_broad[oes_broad.wfh_category==''].shape[0] == 0
    #
    wfh = wfh.merge(oes, on =['occ_code'], how='outer', indicator=True)
    wfh._merge.value_counts()
    wfh = wfh[wfh._merge=='both'].drop('_merge',axis=1)
    assert wfh[wfh.oes_title != wfh.occ_title].shape[0] == 0
    wfh.rename(columns = {'tot_emp':'occ_emp'}, inplace=True)
    wfh = wfh.merge(oes_broad, on=['occ_broad'], how='outer', indicator=True)
    assert wfh[wfh._merge!='both'].shape[0] == 0
    wfh['occ_emp_weight'] = wfh.occ_emp/wfh.occ_broad_emp
    assert wfh[wfh.occ_emp_weight>1].shape[0] == 0
    wfh['teleworkable_wt'] = wfh.teleworkable * wfh.occ_emp_weight
    #
    wfh = wfh.groupby('occ_broad').agg({'teleworkable_wt':'sum', 'wfh_category':'mean'}).reset_index()
    wfh['occ_code'] = wfh.occ_broad
    wfh.loc[wfh.occ_code.str.len()==2,'occ_code'] = wfh.loc[wfh.occ_code.str.len()==2,'occ_code'] + '-0000'
    # wfh.loc[wfh.occ_code.str.len()==4,'occ_code'] = wfh.loc[wfh.occ_code.str.len()==4,'occ_code'] + '000'
    assert wfh[wfh.occ_code.str.len()!=7].shape[0] == 0
    wfh = wfh.merge(occ_df, on=['occ_code'], how='outer', indicator=True)
    assert wfh[wfh._merge!='both'].shape[0] == 0
    wfh['wfh_score'] = wfh.teleworkable_wt * wfh.occ_pop_wt
    wfh.wfh_score.describe()
    #
    wfh_cat = wfh.groupby(['geoid','wfh_category']).agg({'occ_pop':'sum','occ_tot_pop':'mean'}).reset_index()
    wfh_cat = wfh_cat.pivot_table(index=['geoid', 'occ_tot_pop'], columns='wfh_category', values='occ_pop', aggfunc='first').reset_index()
    wfh_cat.columns = wfh_cat.columns.astype(str)
    wfh_cat.columns = wfh_cat.columns.str.replace('\.0$','', regex=True)
    wfh_cat.rename(columns = lambda x: 'occ_pop_'+x if re.search('\d',x) else x, inplace=True) 
    for c in ['occ_pop_'+ str(x) for x in [0, 1, 2, 3]]:
        wfh_cat[c] = wfh_cat[c]/wfh_cat.occ_tot_pop
    #
    wfh_cat.columns = wfh_cat.columns.str.replace('0','low')
    wfh_cat.columns = wfh_cat.columns.str.replace('1','midlow')
    wfh_cat.columns = wfh_cat.columns.str.replace('2','midhigh')
    wfh_cat.columns = wfh_cat.columns.str.replace('3','high')
    wfh_cat = wfh_cat.drop(['occ_tot_pop'], axis=1)
    #
    wfh = wfh.groupby('geoid')['wfh_score'].sum().reset_index()
    wfh.wfh_score.describe()
    wfh = wfh.merge(wfh_cat, on='geoid', how='outer', indicator=True)
    assert wfh[wfh._merge!='both'].shape[0] == 0
    wfh = wfh.drop('_merge',axis=1)
    #
    # Merge occupation level WFH with ACS
    acs = acs.merge(wfh, on='geoid', how='outer', indicator=True)
    assert acs[acs._merge!='both'].shape[0] == 0
    acs = acs.drop('_merge',axis=1)
    acs.columns = acs.columns.str.replace('-0000','',regex=False)
    #
    # Calculate industry-level WFH score
    # Create tract-occcode level data and match with WFH score from Dingel and Neiman (2020)
    naics_cols = ['geoid'] + [x for x in acs.columns.tolist() if re.search('naics_\d{2}$',x) or re.search('naics_\d{2}-\d{2}$',x)]
    naics_df = acs[naics_cols]
    naics_df = pd.melt(naics_df, id_vars='geoid', var_name='naics_code', value_name='naics_pop')
    naics_df['naics_code'] = naics_df['naics_code'].str.replace('^naics_','')
    naics_df = naics_df.merge(acs[['geoid','naics_tot_pop']], on=['geoid'], how='outer', indicator=True)
    assert naics_df[naics_df._merge!='both'].shape[0] == 0
    naics_df = naics_df.drop('_merge',axis=1)
    assert naics_df[naics_df[['geoid','naics_code']].duplicated()].shape[0] == 0
    naics_df['naics_pop_wt'] = naics_df.naics_pop/naics_df.naics_tot_pop
    #
    wfh = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/wfh/NAICS_workfromhome.csv')
    wfh.columns = wfh.columns.str.lower()
    wfh.rename(columns = {'naics':'naics_code'}, inplace=True)
    wfh = wfh.merge(naics_df, on =['naics_code'], how='outer', indicator=True)
    wfh._merge.value_counts() # 92 (government) and 99 not in 
    wfh = wfh[wfh._merge=='both'].drop('_merge',axis=1)
    wfh.rename(columns = {'naics_tot_pop':'naics_emp'}, inplace=True)
    wfh['teleworkable_wt'] = wfh.teleworkable_emp * wfh.naics_pop_wt
    #
    wfh = wfh.groupby('geoid').agg({'teleworkable_wt':'sum'}).reset_index()
    wfh['wfh_score_naics'] = wfh.teleworkable_wt
    wfh = wfh[['geoid','wfh_score_naics']]
    wfh.wfh_score_naics.describe()
    # Merge with ACS
    acs = acs.merge(wfh, on='geoid', how='outer', indicator=True)
    assert acs[acs._merge!='both'].shape[0] == 0
    acs = acs.drop('_merge',axis=1)
    #################################################################################################################
    # Incorporate industry-aggregated WFH Survey from Barrero, Bloom, Buckman, and Davis (2024).
    #################################################################################################################
    naics_df.loc[naics_df.naics_code.str.contains('^56|^81',na=False),'naics_code'] = '5681'
    naics_df['naics_code'] = naics_df.naics_code.str.replace('-','',regex=False)
    naics_df = naics_df.groupby(['geoid','naics_code']).agg({'naics_pop':'sum','naics_tot_pop':'sum'}).reset_index()
    naics_df['naics_pop_wt'] = naics_df.naics_pop/naics_df.naics_tot_pop
    #
    wfh = pd.read_stata("C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/wfh/wfhvar_survey.dta")
    wfh.rename(columns = {'naics':'naics_code'}, inplace=True)
    wfh = wfh.merge(naics_df, on =['naics_code'], how='outer', indicator=True)
    wfh._merge.value_counts() # 55 management of companies and enterprises not in data. In ACS, naics 55 accounts for 0.11% of population. Only 10 tracts have more than 5% of working residents in NAICS 55 (5-8%).
    wfh = wfh[wfh._merge=='both'].drop('_merge',axis=1)
    wfh.rename(columns = {'naics_tot_pop':'naics_emp'}, inplace=True)
    for c in ['wfh_frac','wfh_frac2020','wfh_frac2021','wfh_frac2022']:
        wfh[f'{c}_wt'] = wfh[f'{c}'] * wfh.naics_pop_wt
    #
    wfh = wfh.groupby('geoid').agg({'wfh_frac_wt':'sum', 'wfh_frac2020_wt':'sum', 'wfh_frac2021_wt':'sum', 'wfh_frac2022_wt':'sum'}).reset_index()
    wfh.rename(columns = {'wfh_frac_wt':'wfh_svy', 'wfh_frac2020_wt':'wfh_svy20', 'wfh_frac2021_wt':'wfh_svy21', 'wfh_frac2022_wt':'wfh_svy22'}, inplace=True)
    wfh[['wfh_svy','wfh_svy20','wfh_svy21','wfh_svy22']].describe()
    # Merge with ACS
    acs = acs.merge(wfh, on='geoid', how='outer', indicator=True)
    assert acs[acs._merge!='both'].shape[0] == 0
    acs = acs.drop('_merge',axis=1)
    # #################################################################################################################
    # # Incorporate brand presence in tract from Advan Drug store data. Multiple brand definitions and subdefinitions.
    # #################################################################################################################
    # brand = pd.read_stata("D:/pharmacy/advan/advan_drugs_tract_brand.dta")
    # brand.rename(columns = {'tract':'geoid'}, inplace=True)
    # # Merge with ACS
    # acs = acs.merge(brand, on='geoid', how='outer', indicator=True)
    # print('Match ACS with tract-level brand store availability: ', acs._merge.value_counts())
    # acs = acs[acs._merge!='right_only'].drop('_merge',axis=1)
    # # Fill in null brand dummies as 0. Means there is no drug store in tract.
    # for c in ['d_brand', 'd_top3', 'd_grocery','d_franchise', 'd_hmart', 'd_othfranch', 'd_small', 'd_brandexg']:
    #     acs.loc[acs[c].isnull(),c] = 0
    # Deal with OCC and rest
    for c in ['low','midlow','midhigh','high']:
        acs[f'pct_occ_{c}'] = acs[f'occ_pop_{c}']/acs.occ_tot_pop
        assert acs[(~acs[f'pct_occ_{c}'].between(0,1)) & (~acs[f'pct_occ_{c}'].isnull())].shape[0] == 0
    for c in ['occ_11', 'occ_13', 'occ_15', 'occ_17', 'occ_19', 'occ_21', 'occ_23', 'occ_25', 'occ_27', 'occ_29', 'occ_31', 'occ_33', 'occ_35', 'occ_37', 'occ_39', 'occ_41', 'occ_43', 'occ_45', 'occ_47', 'occ_49', 'occ_51', 'occ_53']:
        acs[f'pct_{c}'] = acs[f'{c}']/acs.occ_tot_pop
        assert acs[(~acs[f'pct_{c}'].between(0,1)) & (~acs[f'pct_{c}'].isnull())].shape[0] == 0
    for c in ['11','21','22','23','31-33','42','44-45','48-49','51','52','53','54','55','56','61','62','71','72','81','92']:
        acs[f'pct_naics_{c}'] = acs[f'naics_{c}']/acs.naics_tot_pop
        assert acs[(~acs[f'pct_naics_{c}'].between(0,1)) & (~acs[f'pct_naics_{c}'].isnull())].shape[0] == 0
    acs.columns = acs.columns.str.replace('-','_')
    return(acs)

acs_df = gen_acs()
# Tract shapefile
gdf = gpd.read_file('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_tract_2010.shp')
gdf.columns = gdf.columns.str.lower()
gdf.columns = gdf.columns.str.replace('10','')
gdf['tract_aland'] = gdf.aland*(3.8610215854*10**(-7)) # square meter to square miles
gdf = gdf[['geoid','tract_aland']]
assert gdf[gdf.geoid.duplicated()].shape[0] == 0
acs_df = acs_df.merge(gdf, on='geoid', how='outer', indicator=True)
acs_df._merge.value_counts()
acs_df = acs_df[acs_df._merge=='both'].drop('_merge',axis=1)

# County shapefile
gdf = gpd.read_file('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_county_2010.shp')
gdf.columns = gdf.columns.str.lower()
gdf.columns = gdf.columns.str.replace('10','')
gdf['cty_aland'] = gdf.aland*(3.8610215854*10**(-7)) # square meter to square miles
gdf.rename(columns={'geoid':'ctyid'}, inplace=True)
gdf = gdf[['ctyid','cty_aland']]
assert gdf[gdf.ctyid.duplicated()].shape[0] == 0
acs_df['ctyid'] = acs_df.geoid.str[:5]
acs_df = acs_df.merge(gdf, on='ctyid', how='outer', indicator=True)
acs_df._merge.value_counts()
acs_df = acs_df[acs_df._merge=='both'].drop('_merge',axis=1)

acs_df = acs_df[acs_df.tot_pop_age>0] # 372 tracts with no population
acs_df = acs_df[acs_df.tract_aland>0] # 1  tracts with no land area

acs_df['pop_density'] = acs_df.tot_pop_age/acs_df.tract_aland
acs_df.pop_density.describe()

acs_df['pct_car'] = acs_df.travelwork_car/acs_df.travelwork_tot_pop
acs_df['pct_public'] = acs_df.travelwork_public/acs_df.travelwork_tot_pop
acs_df['pct_walk'] = acs_df.travelwork_walk/acs_df.travelwork_tot_pop
acs_df['pct_other'] = acs_df.travelwork_other/acs_df.travelwork_tot_pop

acs_df['pct_0to10'] = acs_df.travelwork_time_0to10/acs_df.travelwork_tot_pop
acs_df['pct_10to30'] = acs_df.travelwork_time_10to30/acs_df.travelwork_tot_pop
acs_df['pct_30to60'] = acs_df.travelwork_time_30to60/acs_df.travelwork_tot_pop
acs_df['pct_60above'] = acs_df.travelwork_time_60above/acs_df.travelwork_tot_pop

acs_df[['d_brand','d_grocery','d_top3','d_franchise','d_small','d_brandexg']].describe()
for c in acs_df.columns.tolist():
    print(c, acs_df[c].describe())
acs_df.rename(columns = {'d_brand':'d_brandtr', 'd_top3':'d_top3tr', 'd_brandexg':'d_brandexgtr'}, inplace=True)

keep_cols = ['geoid', 'median_age', 'income_median', 
            'pop_age_1839', 'pop_age_4064', 'pop_age_65', 'pop_age_1864', 'pct_age_1839', 'pct_age_4064', 'pct_age_65', 'tot_pop_age', 'pop_density', 
            #'pop_edu_college', 
            'edu_tot_pop', 'pct_edu_college', 
            #'pop_income_100k', 
            'pop_income_200k', 'income_tot_pop', 'pct_income_100k', 'pct_income_200k', 
            #'pop_minority', 'pop_black', 'pop_asian',
            'race_tot_pop', 'pct_minority', 'pct_black', 'pct_asian',
            'median_homevalue', # Median home value
            'wfh_score', 'occ_tot_pop', # civilian 16+ workers
            # 'occ_pop_low','occ_pop_midlow','occ_pop_midhigh', 'occ_pop_high', 'pct_occ_low','pct_occ_midlow','pct_occ_midhigh', 'pct_occ_high',
            # 'pct_occ_11', 'pct_occ_13', 'pct_occ_15', 'pct_occ_17', 'pct_occ_19', 'pct_occ_21', 'pct_occ_23', 'pct_occ_25', 'pct_occ_27', 'pct_occ_29', 'pct_occ_31', 'pct_occ_33', 'pct_occ_35', 'pct_occ_37', 'pct_occ_39', 'pct_occ_41', 'pct_occ_43', 'pct_occ_45', 'pct_occ_47', 'pct_occ_49', 'pct_occ_51', 'pct_occ_53',
            'tot_pop_16_above', # Population 16+ (Used for employment stat)
            'pop_working', 'emp_res_ratio', # pop_labor_force/tot_pop_16_above
            'pct_unemployed', #pop_unemployed/tot_pop_16_above
            'naics_tot_pop', 'wfh_score_naics', # Civilian 16+ workers
            'naics_11', 'naics_21', 'naics_22', 'naics_23', 
            'naics_31_33', 
            # 'naics_42', 'naics_44_45', 'naics_48_49', 'naics_51', 'naics_52', 'naics_53', 'naics_54', 'naics_55', 'naics_56', 'naics_61', 'naics_62', 'naics_71', 'naics_72', 'naics_81', 'naics_92', 
            'pct_naics_11', 'pct_naics_21', 'pct_naics_22', 'pct_naics_23', 
            'pct_naics_31_33', 'pct_naics_42', 'pct_naics_44_45', 'pct_naics_48_49', 'pct_naics_51', 'pct_naics_52', 'pct_naics_53', 'pct_naics_54', 'pct_naics_55', 'pct_naics_56', 'pct_naics_61', 'pct_naics_62', 'pct_naics_71', 'pct_naics_72', 'pct_naics_81', 'pct_naics_92',
            'pct_selfemp', # class_worker_tot_pop is same as tot_pop_work_16
            # 'pct_noncitizen', 'tot_pop',
            'wfh_svy', 'wfh_svy20', 'wfh_svy21', 'wfh_svy22', # variables generated from WFH survey
            'd_brandtr', 'd_top3tr', 'd_brandexgtr', # variables generated from Advan drug store filtered data
            'tract_aland', 'cty_aland',
            'pct_0to10', 'pct_10to30', 'pct_30to60', 'pct_60above' , 'travelwork_tot_pop',
            'pct_car', 'pct_public', 'pct_walk', 'pct_other',
            'tot_pop_work_16_w_earnings', 'tot_pop_work_25_w_earnings', 'tot_pop_work_16_civ_ft', 'placework_tot_pop'
            ]
acs_df = acs_df[keep_cols]
acs_df.rename(columns={'placework_tot_pop':'tot_pop_work_16'}, inplace=True)
acs_df.rename(columns = {'median_age':'medage', 'income_median':'medinc', 'tot_pop_age':'tot_pop', 'pct_edu_college':'pct_collegeup', 'pct_income_100k':'pct_inc_100k', 'pct_income_200k':'pct_inc_200k', 'wfh_score':'wfh_occ', 'pct_naics_31_33':'pct_mfg', 'wfh_score_naics':'wfh_ind'}, inplace=True)
acs_df.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/acs/final/acs_5yr_2019_final.dta', write_index=False, version=118)

# Generate CBG size to match with Advan POI data.
gdf = gpd.read_file('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/gis/US_blck_grp_2010.shp')
gdf.columns = gdf.columns.str.lower()
gdf.columns = gdf.columns.str.replace('10','')
gdf['cbg_aland'] = gdf.aland*(3.8610215854*10**(-7)) # square meter to square miles
gdf = gdf[['geoid','cbg_aland']]
assert gdf[gdf.geoid.duplicated()].shape[0] == 0
assert gdf[gdf.geoid.str.len()!=12].shape[0] == 0
gdf.rename(columns = {'geoid':'poi_cbg'}, inplace=True)
gdf.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/advan/cbg_aland.dta', write_index=False, version=118)
