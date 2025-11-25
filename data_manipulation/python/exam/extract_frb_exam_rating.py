import time, os, re
import pandas as pd, numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def open_browser():
    """
    Open a Chrome browser instance without service manager.
    Returns:
        WebDriver: Browser instance
    """
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-browser-side-navigation")
    options.page_load_strategy = 'eager'
    
    browser = webdriver.Chrome(options=options)
    return browser

def close_browser(browser):
    """
    Close the browser instance.
    Args:
        browser: WebDriver instance to close
    """
    if browser:
        browser.quit()
        print("Browser closed.")


browser = open_browser()
url = "https://crapes.fdic.gov"
browser.get(url)

#
frb_cra = pd.read_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/FRB_CRA_Data.csv')
msa_exams = frb_cra[~frb_cra['MSA/State'].isnull()][['IDRSSD','Exam Date']].drop_duplicates()
nomsa_exams = frb_cra[frb_cra['MSA/State'].isnull()][['IDRSSD','Exam Date']].drop_duplicates()
merged = msa_exams.merge(nomsa_exams, on = ['IDRSSD','Exam Date'], how='outer', indicator=True)
assert merged[merged._merge=='left_only'].shape[0] == 0 # All exams in MSAs within non-MSA exams.
frb_cra = frb_cra[frb_cra['MSA/State'].isnull()]
cols = ['IDRSSD', 'Bank Name', 'City', 'State', 'Exam Method', 'Exam Date', 'Public Date', 'Overall Rating', 'Asset Size(In Thousands)']
frb_cra = frb_cra[cols]
frb_cra['Exam Date'] = frb_cra['Exam Date'].str.replace('/','')
frb_cra['year'] = frb_cra['Exam Date'].str[-4:]
frb_cra['Link'] = 'https://www.federalreserve.gov/apps/CRAPubWeb/CRA/DownloadPDF/' + frb_cra.IDRSSD.astype(str) + '_' + frb_cra.year + frb_cra['Exam Date'].str[:4]
frb_cra['Exam Date'] = pd.to_datetime(frb_cra['Exam Date'], format='%m%d%Y')
frb_cra = frb_cra[frb_cra['Exam Date']>='2018-01-01']
frb_cra.reset_index(drop=True, inplace=True)
frb_cra.rename(columns = {'IDRSSD':'ID_RSSD'}, inplace=True)
#
# Restrict to banks in SOD 2019.
cols = ['rssdid', 'cert', 'insured', 'bank_branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_drug.dta', columns = cols)
drug_sod = drug_sod[drug_sod.bank_branch_500yd==1].drop('bank_branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.rssdid.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert','rssdid':'ID_RSSD'}, inplace=True)
frb_cra1 = frb_cra1.merge(drug_sod, on =['ID_RSSD'])
frb_cra1.rename(columns = {'Public Date':'Release Date','Overall Rating':'CRA Rating','Exam Method':'Exam Criteria'}, inplace=True) 
frb_cra1['agency'] = 'FRB'
frb_cra1.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/frb_examination.dta', write_index=False, version=118)
# frb_cra.to_excel('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/frb_examination.xlsx', index=False)

#
cols = ['ID_RSSD', 'cert', 'branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_mindist.dta', columns = cols)
drug_sod = drug_sod[drug_sod.branch_500yd==1].drop('branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.ID_RSSD.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert'}, inplace=True)
drug_sod['Cert']= pd.to_numeric(drug_sod.Cert)
#
frb_cra2 = frb_cra.merge(drug_sod, on='ID_RSSD')
frb_cra2.rename(columns = {'Public Date':'Release Date','Overall Rating':'CRA Rating','Exam Method':'Exam Criteria'}, inplace=True) 
frb_cra2['agency'] = 'FRB'
assert frb_cra2[frb_cra2[['ID_RSSD','Exam Date']].duplicated()].shape[0] == 0
frb_cra2['agency'] = 'FRB'
frb_cra2.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/frb_examination_mindist.dta', write_index=False, version=118)






# browser = open_browser()
# for i in range(0,frb_cra.shape[0],50):
#     l = frb_cra.Link.tolist()[i]
#     browser.get(l)
#     time.sleep(1)
#     if i % 100 == 0:
#         print(i, l)

