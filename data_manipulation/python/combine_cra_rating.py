import csv
import time, os, re
import pandas as pd, numpy as np
import sys
sys.path.append(r"C:\Users\elliotoh\Box\lodes_shared\pharmacy\data\cra\examination\occ\raw\excel")
from process_call_reports import process_call_reports

# FDIC, OCC, FRB CRA examination data.
common_cols = ['ID_RSSD', 'Cert', 'Exam_Date', 'Bank_Name', 'City', 'State', 'CRA_Rating', 'Exam_Criteria','agency']
fdic_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic_examination.dta', columns=common_cols)
occ_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination.dta', columns=common_cols)
frb_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/frb_examination.dta', columns=common_cols)
bank_cra = pd.concat([fdic_cra, occ_cra, frb_cra],axis=0)
bank_cra.rename(columns = {'agency':'Agency'}, inplace=True)
bank_cra.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/bank_examination.dta', write_index=False, version=118)

x = 2907439
bank_cra[bank_cra.ID_RSSD==x]
bank_cra1[bank_cra1.ID_RSSD==x]

# FDIC, OCC, FRB CRA examination data based on drug-SOD mindist
common_cols = ['ID_RSSD', 'Cert', 'Exam_Date', 'Bank_Name', 'City', 'State', 'CRA_Rating', 'Exam_Criteria','agency']
fdic_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/fdic_examination_mindist.dta', columns=common_cols)
occ_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/occ_examination_mindist.dta', columns=common_cols)
frb_cra = pd.read_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/frb_examination_mindist.dta', columns=common_cols)
bank_cra1 = pd.concat([fdic_cra, occ_cra, frb_cra],axis=0)
bank_cra1.rename(columns = {'agency':'Agency'}, inplace=True)
bank_cra1.to_stata('C:/Users/elliotoh/Box/lodes_shared/pharmacy/data/cra/examination/bank_examination_mindist.dta', write_index=False, version=118)