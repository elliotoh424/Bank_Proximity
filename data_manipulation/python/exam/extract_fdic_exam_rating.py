import time, os, re
import pandas as pd, numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys
sys.path.append(r"C:\Users\elliotoh\Box\lodes_shared\pharmacy\data\cra\examination\occ\raw\excel")
from process_call_reports import process_call_reports


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

def scrape_fdic_table(browser,filename):
    """
    Scrape an Angular Material table (FDIC format).
    Args:
        browser: WebDriver instance
    Returns:
        DataFrame: Scraped data
    """
    print("Starting table extraction...")
    start_time = time.time()
    
    # Wait for Angular Material table to load
    wait = WebDriverWait(browser, 10)
    table_rows = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "mat-row")))
    time.sleep(2)  # Additional wait for all data to render
    
    # Extract data
    data = []
    
    # Get all rows
    rows = browser.find_elements(By.TAG_NAME, "mat-row")
    print(f"Found {len(rows)} rows in table")
    
    for row in rows:
        row_data = {}
        
        # Get the cells in this row
        cells = row.find_elements(By.TAG_NAME, "mat-cell")
        
        # Extract text from each cell
        if len(cells) >= 13:  # Ensure we have all columns
            row_data["RowNumber"] = cells[0].text.strip()
            row_data["Cert"] = cells[1].text.strip()
            row_data["Release Date"] = cells[2].text.strip()
            row_data["Status"] = cells[3].text.strip()
            row_data["Bank Name"] = cells[4].text.strip()
            row_data["Street"] = cells[5].text.strip()
            row_data["City"] = cells[6].text.strip()
            row_data["State"] = cells[7].text.strip()
            row_data["Zip"] = cells[8].text.strip()
            row_data["CRA Rating"] = cells[9].text.strip()
            row_data["Rating Points"] = cells[10].text.strip()
            row_data["Asset Size"] = cells[11].text.strip()
            row_data["Exam Criteria"] = cells[12].text.strip()
            row_data["PE"] = cells[13].text.strip()
            
            # Try to get links
            try:
                # Get the second link in the row (usually the PDF)
                links = row.find_elements(By.CSS_SELECTOR, "a.ng-star-inserted")
                if len(links) > 1:
                    row_data["Link"] = links[1].get_attribute("href")
                else:
                    row_data["Link"] = ""
                    
                # If there's a link in the first cell (Cert), also get the href
                try:
                    cert_link = cells[0].find_element(By.TAG_NAME, "a")
                    row_data["Cert_Link"] = cert_link.get_attribute("href")
                except:
                    row_data["Cert_Link"] = ""
            except Exception as e:
                print(f"Error extracting links: {e}")
                row_data["Link"] = ""
                row_data["Cert_Link"] = ""
                
            data.append(row_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    duration = time.time() - start_time
    print(f"Extracted {len(data)} records in {duration:.2f} seconds")
    df.to_excel(filename, index=False)    
    return df

# Generate excel sheet 
# Generate a date range from Jan 2018 to Dec 2023
date_range = pd.date_range(start='2018-01-01', end='2025-03-31', freq='MS')  # MS = Month Start
df = pd.DataFrame({'date': date_range})
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['month_year'] = df['date'].dt.strftime('%m/%Y')
df['filename'] = df['date'].dt.strftime('%m%Y')
df = df[['year', 'month', 'month_year', 'filename']]
df.to_csv('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic/examination_date_for_scraping.csv',index=False)

# Navigate to the page (replace with your URL)
browser = open_browser()
url = "https://crapes.fdic.gov"
browser.get(url)
filename = '18012503_inactive.xlsx'
# scrape_fdic_table(browser, 'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic/'+filename)

files = [x for x in os.listdir('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic/') if re.search('\d',x)]
fdic_cra = []
for f in files:
    a = pd.read_excel(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic/{f}')
    fdic_cra.append(a)
fdic_cra = pd.concat(fdic_cra, axis=0)
assert fdic_cra[~fdic_cra.Cert_Link.isnull()].shape[0] == 0
fdic_cra = fdic_cra.drop('Cert_Link',axis=1)
fdic_cra['Exam Date'] = '20'+ fdic_cra.Link.str.extract('_(\d+).PDF$')
fdic_cra.loc[fdic_cra.Link=='https://crapes.fdic.gov/publish/2022/PPE22081-000000008.PDF','Exam Date'] = '20220411'
fdic_cra.loc[fdic_cra.Link=='https://crapes.fdic.gov/publish/2022/PPE5649-000000013.PDF','Exam Date'] = '20220307'
fdic_cra = fdic_cra[~fdic_cra['Exam Date'].str.contains('2017',na=False)]
fdic_cra['Exam Date'] = pd.to_datetime(fdic_cra['Exam Date'], format= '%Y%m%d')
fdic_cra.loc[fdic_cra['Bank Name']=='Border Bank','Cert'] = 15684 # Standardize to pre-merger
#
# Use Pharmacy-SOD 2019 to convert Cert to RSSDID as of June 2019. Only use intersecting banks. 
cols = ['rssdid', 'cert', 'insured', 'bank_branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/store_bank_drug.dta', columns = cols)
drug_sod = drug_sod[drug_sod.bank_branch_500yd==1].drop('bank_branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.rssdid.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert','rssdid':'ID_RSSD'}, inplace=True)
#
fdic_cra1 = fdic_cra.merge(drug_sod, on='Cert')
cols = ['ID_RSSD', 'Cert', 'Release Date', 'Status', 'Bank Name', 'Street', 'City', 'State', 'Zip', 'CRA Rating', 'Rating Points', 'Asset Size', 'Exam Criteria', 'PE', 'Link', 'Exam Date']
fdic_cra1 = fdic_cra1[cols].drop_duplicates()
fdic_cra1.reset_index(drop=True, inplace=True)
assert fdic_cra1[fdic_cra1[['ID_RSSD','Exam Date']].duplicated()].shape[0] == 0
fdic_cra1['agency'] = 'FDIC'
fdic_cra1.to_stata('C:/Users/pharmacyoh/Box/lodes_shared/elliot/data/cra/examination/fdic_examination.dta', write_index=False, version=118)
# fdic_cra.to_excel('C:/Users/pharmacyoh/Box/lodes_shared/elliot/data/cra/examination/fdic_examination.xlsx', index=False)

# Use Pharmacy-SOD 2019 to convert Cert to RSSDID as of June 2019. Only use intersecting banks. 
cols = ['ID_RSSD', 'cert', 'branch_500yd']
drug_sod = pd.read_stata(f'C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/office/advan/drugstore_sod_mindist.dta', columns = cols)
drug_sod = drug_sod[drug_sod.branch_500yd==1].drop('branch_500yd',axis=1).drop_duplicates()
assert drug_sod[drug_sod.ID_RSSD.duplicated()].shape[0] == 0
drug_sod.rename(columns = {'cert':'Cert'}, inplace=True)
drug_sod['Cert']= pd.to_numeric(drug_sod.Cert)
#
fdic_cra2 = fdic_cra.merge(drug_sod, on='Cert')
cols = ['ID_RSSD', 'Cert', 'Release Date', 'Status', 'Bank Name', 'Street', 'City', 'State', 'Zip', 'CRA Rating', 'Rating Points', 'Asset Size', 'Exam Criteria', 'PE', 'Link', 'Exam Date']
fdic_cra2 = fdic_cra2[cols].drop_duplicates()
fdic_cra2.reset_index(drop=True, inplace=True)
assert fdic_cra2[fdic_cra2[['ID_RSSD','Exam Date']].duplicated()].shape[0] == 0
fdic_cra2['agency'] = 'FDIC'
fdic_cra2.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic_examination_mindist.dta', write_index=False, version=118)



# 11 banks not in call report between 2018 and 2024


# browser = open_browser()
# for i in range(0,fdic_cra.shape[0],50):
#     l = fdic_cra.Link.tolist()[i]
#     browser.get(l)
#     time.sleep(1)
#     if i % 100 == 0:
#         print(i, l)