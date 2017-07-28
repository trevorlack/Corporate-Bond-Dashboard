'''

"HYHG_MASTER" -->  Column Values:
['date' 'CUSIP' 'ISIN' 'Parent' 'Ticker' 'Coupon' 'Maturity_Date' 'Rating'
 'GLIC' 'COBS' 'PAR' 'MKV' 'AVLF' 'ACRI' 'Price' 'B_YTM' 'BYTW' 'MODD'
 'EDUR' 'DURW' 'GSPRED' 'SPRDWCALL' 'OAS' 'CONVX' 'EFFCNVX' 'PrincRtn'
 'IntRtn' 'RIRtn' 'TotalRtn' 'linked_CUSIP' 'rebal_action'
 'rebal_add_ref_price' 'iBENTIFIER_x' 'corporate_action' 'description'
 'Indx_Dly_Tot_MKV' 'Indx_Weight' 'psa_par' 'iBENTIFIER_y' 'Price_y'
 'PCT_Change' 'PSA_MKV' 'PSA_Dly_Tot_MKV' 'PSA_Weight']

'''


import pandas as pd
import datetime
import sys

from MASTER_Grabber import *
from HYHG_Analysis_2 import *
from HYHG_Headlines import *
#Analysis_Type = str(input('Time Series Analysis?'))


dstart = datetime.date(2017,2,10)
dend = datetime.date(2017,2,10)
date_pass = dend.strftime("%m/%d/%Y")

raw_index = hyhg_index_query(dstart, dend)
raw_psa = hyhg_psa_holding_query(dstart, dend)

#headlines_hyhg(raw_index, dend)


HYHG_MASTER = combine_hyhg(raw_index,raw_psa)
print(HYHG_MASTER.columns.values)
'''Analyze by Issuer'''
#HYHG_Issuer = issuer_compress(HYHG_MASTER)



hyhg_industry_dot_plots(HYHG_MASTER, date_pass)
hyhg_issuer_dot_plots(HYHG_MASTER, date_pass)
hyhg_maturity_dot_plots(HYHG_MASTER, date_pass)
hyhg_rating_dot_plots(HYHG_MASTER, date_pass)


'''
Left off with Maturity dot plot finished but not if the data should be horizontal instead

Rating is going to require going to the database to grab the rating matrix

OAS, Duration, YTM, Coupon will require bins

'''