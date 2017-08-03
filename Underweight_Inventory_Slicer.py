import pandas as pd
import xlrd
from Bloomberg_Pass_Through import *
from IVY_Mini_Functions import *
import glob
import os
from dateutil.relativedelta import relativedelta
from datetime import date
from datetime import datetime

maturity_cutoff = date.today() + relativedelta(months=+18)
print(maturity_cutoff)
os.chdir('R:/Fixed Income/IVY/Index Holdings')


rebal_consideration = str(input('Would you like to filter CUSIPS being deleted in the \n'
                                'upcoming rebalance?\n'
                                ''))
rebal_day = str(input('Is it Rebal Day? \n'
                      ''))


weight_filter = float(input('Please enter your fund vs index weighting threshold. \n'
                 ' (eg. "0.1" for only names that are .10% overweight or less and "0.0" for names \n'
                 'only names that are underweight) \n'
                 ''))
issuer_filter = str(input('Type issuer tickers to omit from the universe you wish to upload \n'
                          'Separate tickers with a comma and no spaces!  eg. WFC,BAC,JPM \n'
                          '')).split(',')
#run_plots = str(input('Would you like to run the IVY Graphics routine? \n'
#                      '(This will use ~2,000 Bloomberg pulls \n'
#                      ''))

print(issuer_filter)

DateList = []
for file in list(glob.glob("*SP5MAIG*.S*")):
    DateList.append(file[0:8])

Max_Date = str(max(DateList))
if rebal_day == 'yes':
    path = str('R:/Fixed Income/IVY/Index Holdings/' + Max_Date + '_SP5MAIG_PRO.SPFIC')
else:
    path = str('R:/Fixed Income/IVY/Index Holdings/'+Max_Date+'_SP5MAIG_CLS.SPFIC')
print(path)
index = pd.read_csv(path, sep='\t')
index = index[index.DESCRIPTION !='CASH USD 0.00%']
index_cap = index['MARKET VALUE'].sum()
index['Market_Cap'] = index_cap
index['Weight'] = index['MARKET VALUE']/index['Market_Cap']
#index = index[['EFFECTIVE DATE', 'CUSIP', 'PRICE', 'PRICE WITH ACCRUED', 'MATURITY DATE', 'MARKET VALUE', 'Market_Cap', 'Weight', 'PAR AMOUNT']]
index = index.rename(columns={'MATURITY DATE':'MATURITY_DATE'})
index['MATURITY_DATE'] = pd.to_datetime(index['MATURITY_DATE'], format='%Y%m%d')

'''
Grab holdings data from excel models
'''
lastrow = 1038
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

'''Create new version of the MASTER dataset before it is filtered down'''
MASTER_FULL = MASTER

'''Eliminate any name that does not have a "FIXED" Coupon'''
MASTER = MASTER[MASTER.CPN_TYP == 'FIXED']
MASTER = MASTER[MASTER.CALC_TYP == 1.0]

MASTER['IVYxTREV'] = MASTER['CUSIP']
DATER = datetime.strftime(date.today(), '%Y%m%d')
MASTER[DATER] = MASTER['PAR AMOUNT']/1000
MASTER = MASTER[['EFFECTIVE DATE', 'CUSIP', 'PAR AMOUNT', 'PRICE WITH ACCRUED', 'MATURITY_DATE','PSA_PAR_HOLDING', 'TICKER', 'MIN_PIECE', 'MIN_INCREMENT', 'Index_Weight', 'Fund_Weight', 'Weight_Difference', 'IVYxTREV', DATER]]

minimum_piece = 99000
MASTER = MASTER[MASTER.Weight_Difference <= weight_filter]
MASTER = MASTER[MASTER.MIN_PIECE <= minimum_piece]
MASTER = MASTER[MASTER.MATURITY_DATE >= maturity_cutoff]
if issuer_filter != '':
    for x in range(0, len(issuer_filter)):
        print(issuer_filter[x])
        MASTER = MASTER[MASTER.TICKER != issuer_filter[x]]

os.chdir('C:/Users/tlack/Documents/Python Scripts/Yieldbook Interface')

if rebal_consideration == 'yes':
    drop_cusips = Include_Rebal(index, Max_Date)
    for z in range(0, len(drop_cusips)):
        MASTER = MASTER[MASTER.CUSIP != drop_cusips[z]]

os.chdir('C:/Users/tlack/Documents/Python Scripts/Yieldbook Interface')
MASTER.to_csv('ivy_spx_modified_index.csv')

print('Filter Summary \n'
      'Minimum Piece Size Cutoff:', str(minimum_piece) +'\n'
      'Account for upcoming rebal deletes? (yes/no):', rebal_consideration + '\n'
      'Is it REBAL DAY? (yes/no):', rebal_day +'\n'
      'Maximum Current Issuer Weight Cut-off:', str(weight_filter) +'\n'
      'List of Specific Issuers to Omit:', issuer_filter +'\n'
      'Index File Used:', path)

