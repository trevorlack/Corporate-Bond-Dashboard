import pandas as pd
import xlrd
from Bloomberg_Pass_Through import *
from IVY_Mini_Functions import *
import glob
import os
from dateutil.relativedelta import relativedelta
from datetime import date

maturity_cutoff = date.today() + relativedelta(months=+18)
print(maturity_cutoff)
os.chdir('R:/Fixed Income/IVY/Index Holdings')

weight_filter = float(input(('Please enter your fund vs index weighting threshold. \n'
                 ' (eg. "0.1" for only names that are .10% overweight or less and "0.0" for names \n'
                 'only names that are underweight) \n'
                 '____')))
DateList = []

for file in list(glob.glob("*SP5MAIG*.S*")):
    DateList.append(file[0:8])

Max_Date = str(max(DateList))
path = str('R:/Fixed Income/IVY/Index Holdings/'+Max_Date+'_SP5MAIG_CLS.SPFIC')
print(path)
index = pd.read_csv(path, sep='\t')
index = index[index.DESCRIPTION !='CASH USD 0.00%']
index_cap = index['MARKET VALUE'].sum()
index['Market_Cap'] = index_cap
index['Weight'] = index['MARKET VALUE']/index['Market_Cap']
index = index[['EFFECTIVE DATE', 'CUSIP', 'PRICE', 'PRICE WITH ACCRUED', 'MATURITY DATE', 'MARKET VALUE', 'Market_Cap', 'Weight', 'PAR AMOUNT']]
index = index.rename(columns={'MATURITY DATE':'MATURITY_DATE'})
index['MATURITY_DATE'] = pd.to_datetime(index['MATURITY_DATE'], format='%Y%m%d')

'''
Grab holdings data from excel models
'''
lastrow = 1030
book = xlrd.open_workbook("R:\Fixed Income\IVY\Trading Model\IVY Master.xlsm")
sheet = book.sheet_by_name("IG")
PSA_Bond_Cap = str(sheet.cell(4,5).value)

Cusip=[]
for r in range(18,lastrow):
    Cusip.append(str(sheet.cell(r,1).value))

IIV_Par=[]
for r in range(18,lastrow):
    try:
        IIV_Par.append(int(sheet.cell(r,6).value))
    except ValueError:
        if sheet.cell(r,6).value == "-":
            IIV_Par.append(0)

ProShares_holdings = pd.DataFrame({'CUSIP': Cusip, 'PSA_PAR_HOLDING': IIV_Par})
ProShares_holdings['PSA_Weights'] = ''
ProShares_holdings['PSA_Market_Value'] = ''
ProShares_holdings['PSA_Bond_Cap'] = PSA_Bond_Cap

'''
Merge PSA holdings table with Index Data
'''

index = index.merge(ProShares_holdings, on='CUSIP')
index['PSA_Market_Value'] = index['PSA_PAR_HOLDING'] * index['PRICE WITH ACCRUED']
index['PSA_Bond_Cap'] = index['PSA_Market_Value'].sum()
index['PSA_Weights'] = index['PSA_Market_Value'].divide(index['PSA_Bond_Cap'])
CUSIPs = index['CUSIP']

'''
Retrieve Bloomberg data by passing CUSIP Column.  First Bloomberg run gets tickers to determine weighting
differences between PSA and Index.  In order to limit the Bloomberg data usage, the idea is to
retrieve as few fields to apply the initial filters so that you can reduce the number of names you
pull more and more data fields.
'''

blp_Tickers = Bloomberg_First_Pass(CUSIPs)
index = index.merge(blp_Tickers, on='CUSIP')
Ticker_Weightings = Ticker_Weights_Sum(index)
MASTER = index.merge(Ticker_Weightings, on='TICKER', how='left')
'''Eliminate any name that does not have a "FIXED" Coupon'''
MASTER = MASTER[MASTER.CPN_TYP == 'FIXED']
''''''
MASTER['IVYxTREV'] = MASTER['CUSIP']
MASTER['2017'] = MASTER['PAR AMOUNT']/1000
MASTER = MASTER[['EFFECTIVE DATE', 'CUSIP', 'PAR AMOUNT', 'PRICE WITH ACCRUED', 'MATURITY_DATE','PSA_PAR_HOLDING', 'TICKER', 'MIN_PIECE', 'MIN_INCREMENT', 'Index_Weight', 'Fund_Weight', 'Weight_Difference', 'IVYxTREV', '2017']]

MASTER = MASTER[MASTER.Weight_Difference <= weight_filter]
MASTER = MASTER[MASTER.MIN_PIECE <= 99000]
MASTER = MASTER[MASTER.MATURITY_DATE >= maturity_cutoff]

os.chdir('C:/Users/tlack/Documents/Python Scripts/Yieldbook Interface')
MASTER.to_csv('ivy_spx_modified_index.csv')

'''
Things to consider:
-Would it be better to run the entire index through bloomberg and store the requested bloomberg
data in the database?  (How import would this data be long term vs. how many times would this
script be run on a daily basis)

'''
#blp_DataFrame = Bloomberg_Data(CUSIPs)
#print(blp_DataFrame)
