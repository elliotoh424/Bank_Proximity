
import csv
import time, os, re
import pandas as pd, numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sys
sys.path.append(r"C:\Users\elliotoh\Box\lodes_shared\pharmacy\data\cra\examination\occ\raw\excel")
from process_call_reports import process_call_reports

# Initialize a global browser instance
_browser = None

def get_browser():
    """Get or create a browser instance"""
    global _browser
    if _browser is None:
        options = Options()
        # options.add_argument("--headless")  # Run in headless mode (no UI)
        options.add_argument("--start-maximized")  # Start maximized
        options.add_argument("--disable-extensions")  # Disable extensions for better performance
        options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-browser-side-navigation")
        options.page_load_strategy = 'eager'  # Don't wait for all resources to load
        
        # Use webdriver_manager to handle driver installation
        _browser = webdriver.Chrome(options=options)
    
    return _browser

def close_browser():
    """Close the browser when done"""
    global _browser
    if _browser:
        _browser.quit()
        _browser = None

def fast_extract_table_data(browser, file_path):
    """Extract table data using a hybrid approach for better performance"""
    print("Starting data extraction...")
    start_time = time.time()
    
    # Get the HTML content after manual interaction
    html_content = browser.page_source
    
    # Use BeautifulSoup for faster parsing of the already loaded page
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', id='resultsTable')
    
    if not table:
        print("Table not found!")
        return []
    
    # Get all rows (skip header)
    rows = table.find_all('tr')[1:]
    
    bank_data = []
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 8:
            continue
        
        # Extract evaluation date and link
        eval_date_cell = cells[4]
        link = ""
        eval_date = eval_date_cell.text.strip()
        
        a_tag = eval_date_cell.find('a')
        if a_tag and a_tag.has_attr('href'):
            link = a_tag['href']
        
        # Extract all data
        data = {
            "charter_docket": cells[0].text.strip(),
            "bank": cells[1].text.strip(),
            "city": cells[2].text.strip(),
            "state": cells[3].text.strip(),
            "evaluation_date": eval_date,
            "public_date": cells[5].text.strip(),
            "rating": cells[6].text.strip(),
            "examination": cells[7].text.strip(),
            "eval_date_link": link
        }
        bank_data.append(data)
    
    # Convert to DataFrame
    df = pd.DataFrame(bank_data)
    
    duration = time.time() - start_time
    print(f"Extracted {len(bank_data)} records in {duration:.2f} seconds")
    for c in df.columns.tolist():
        print(c, df[df[c].str.len()==0].shape[0], df.shape[0])
    print('first two and last two observations')
    print(df.head(2))
    print(df.tail(2))
    df.to_excel(file_path, index=False)
    return df


# Downloaded from OCC rating
browser = get_browser()
url = 'https://occ.gov/publications-and-resources/tools/index-cra-search.html'
browser.get(url)    
time.sleep(1)
filedir = 'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ/'
fast_extract_table_data(browser, filedir + 'occ_examination_needimprove.xlsx')
fast_extract_table_data(browser, filedir + 'occ_examination_outstanding.xlsx')
fast_extract_table_data(browser, filedir + 'occ_examination_satisfactory.xlsx')
fast_extract_table_data(browser, filedir + 'occ_examination_substantialnoncompliance.xlsx')
#
filedir = 'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ/'
NI = pd.read_excel(filedir + 'occ_examination_needimprove.xlsx')
O = pd.read_excel(filedir + 'occ_examination_outstanding.xlsx') 
S = pd.read_excel(filedir + 'occ_examination_satisfactory.xlsx') 
SN = pd.read_excel(filedir + 'occ_examination_substantialnoncompliance.xlsx') 
occ_cra = pd.concat([NI, O, S, SN], axis=0)
occ_cra = occ_cra[occ_cra.evaluation_date.str.contains('20[12][0-9]',na=False)] # Contains OTS dockets too.
occ_cra = occ_cra[~occ_cra.evaluation_date.str.contains('201[01234567]')]
assert occ_cra[occ_cra.eval_date_link.str.contains('/OTS/',na=False)].shape[0] == 0
occ_cra = occ_cra.drop(['eval_date_link','public_date'],axis=1).drop_duplicates()
assert occ_cra[occ_cra[['charter_docket','evaluation_date']].duplicated()].shape[0] == 0
occ_cra['evaluation_date'] = pd.to_datetime(occ_cra['evaluation_date'], format='%m/%d/%Y')
# Merge with contemporaneous call report to get Cert and RSSD_ID
callreport = process_call_reports()
callreport = callreport[callreport.reportdate=='06302019']
callreport.rename(columns = {'FDIC Certificate Number':'Cert', 'OCC Charter Number':'charter_docket', 'IDRSSD':'ID_RSSD'}, inplace=True)
callreport = callreport[['ID_RSSD', 'Cert', 'charter_docket']]
occ_cra = occ_cra.merge(callreport, on='charter_docket', how='left', indicator=True)
print('Match with call report:', occ_cra._merge.value_counts())
occ_cra.rename(columns = {'_merge':'match_cr'}, inplace=True)
unmatched_occ = occ_cra[occ_cra.match_cr=='left_only'][['charter_docket','bank','city','state']].drop_duplicates()
unmatched_occ.to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ/occ_cra_unmatched_callreport.csv',index=False)
#
unmatched_manual = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ/occ_cra_unmatched_callreport_manual.csv')
unmatched_manual = unmatched_manual[['charter_docket','Cert','ID_RSSD']].drop_duplicates()
# unmatched_manual = unmatched_manual[unmatched_manual.drug_sod_match==1][['charter_docket','Cert','ID_RSSD']]
occ_cra = occ_cra.merge(unmatched_manual, on='charter_docket', how='outer', indicator=True)
assert occ_cra[occ_cra._merge=='right_only'].shape[0] == 0
occ_cra.loc[occ_cra._merge=='both','Cert_x'] = occ_cra.loc[occ_cra._merge=='both','Cert_y']
occ_cra.loc[occ_cra._merge=='both','ID_RSSD_x'] = occ_cra.loc[occ_cra._merge=='both','ID_RSSD_y']
assert occ_cra[(occ_cra.Cert_x.isnull())].shape[0] == 0 & occ_cra[(occ_cra.ID_RSSD_x.isnull())].shape[0] == 0
occ_cra = occ_cra.drop(['Cert_y','ID_RSSD_y','_merge','match_cr'],axis=1)
occ_cra.columns = occ_cra.columns.str.replace('_x','')
# Restrict to banks in SOD 2019.
cols = ['placekey','rssdid', 'cert', 'insured', 'bank_branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_drug.dta', columns = cols)
drug_sod = drug_sod[drug_sod.bank_branch_500yd==1].drop('bank_branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.rssdid.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert','rssdid':'ID_RSSD'}, inplace=True)
occ_cra1 = occ_cra.merge(drug_sod, on =['Cert','ID_RSSD'])
occ_cra1.rename(columns = {'bank':'Bank Name','city':'City','state':'State','evaluation_date':'Exam Date','rating':'CRA Rating','examination':'Exam Criteria'}, inplace=True)
occ_cra1['agency'] = 'OCC'
occ_cra1.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination.dta', write_index=False, version=118)
# occ_cra.to_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination.xlsx', index=False)

# Restrict to banks in SOD 2019.
cols = ['ID_RSSD', 'cert', 'branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_mindist.dta', columns = cols)
drug_sod = drug_sod[drug_sod.branch_500yd==1].drop('branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.ID_RSSD.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert'}, inplace=True)
drug_sod['Cert']= pd.to_numeric(drug_sod.Cert)
occ_cra2 = occ_cra.merge(drug_sod, on =['Cert','ID_RSSD'])
occ_cra2.rename(columns = {'bank':'Bank Name','city':'City','state':'State','evaluation_date':'Exam Date','rating':'CRA Rating','examination':'Exam Criteria'}, inplace=True)
occ_cra2['agency'] = 'OCC'
occ_cra2.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination_mindist.dta', write_index=False, version=118)
# occ_cra.to_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination.xlsx', index=False)
