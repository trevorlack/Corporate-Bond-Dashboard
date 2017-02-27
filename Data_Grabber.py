#import pandas as pd
#import numpy as np
#import mysql as db
#import glob
import win32com.client as win32


excel = win32.gencache.EnsureDispatch('Excel.Application')
wb = excel.Workbooks.Open('HY_Citi_Index_020217.xlsm')
excel.Visible = True

#all_data = pd.DataFrame()
#for f in glob.glob('*.xlsm'):
#    df = pd.read_excel(f)
#    all_data = all_data.append(df, ignore_index=True)

#all_data.describe()