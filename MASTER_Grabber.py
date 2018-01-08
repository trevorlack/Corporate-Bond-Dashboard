'''
This is the master module to pull all data and compile into one dataframe.  The main data
sets are the index definition and the fund holdings

Master Combined Column Values
['date' 'CUSIP' 'ISIN' 'Parent' 'Ticker' 'Coupon' 'Maturity_Date' 'Rating'
 'GLIC' 'COBS' 'PAR' 'MKV' 'AVLF' 'ACRI' 'Price' 'B_YTM' 'BYTW' 'MODD'
 'EDUR' 'DURW' 'GSPRED' 'SPRDWCALL' 'OAS' 'CONVX' 'EFFCNVX' 'PrincRtn'
 'IntRtn' 'RIRtn' 'TotalRtn' 'linked_CUSIP' 'rebal_action'
 'rebal_add_ref_price' 'iBENTIFIER_x' 'corporate_action' 'description'
 'Indx_Dly_Tot_MKV' 'Indx_Weight' 'letter_rating' 'psa_par' 'iBENTIFIER_y'
 'Price_y' 'PCT_Change' 'PSA_MKV' 'PSA_Dly_Tot_MKV' 'PSA_Weight']


ISSUES:
1--Start Dates:
    First data series has to be one day prior than what the user wants in order to have
    price return data for the first date.

2--Fund holdings differ from Index:
    Errors could arise if the fund holds a name not included in the index OR if
    the fund holds a name under a different CUSIP from what the index database knows
3--Last day of MONTH!!!  If the query is run for the last day of the month, it will
    include the PDC ADDs placeholders.
'''

import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=, user='', passwd=access_token, db='')
engine = create_engine('mysql+pymysql://:%s@localhost/' %(access_token))

conn2 = pymysql.connect(host='localhost', port=, user='', passwd=access_token, db='algo_data')
engine2 = create_engine('mysql+pymysql://:%s@localhost/algo_data' %(access_token))


def hyhg_index_query(dstart, dend):
    hyhg_index_sql = "SELECT * \
                FROM bens_desk.hyhg_index \
                WHERE As_Of_Date >= \'%s\' and As_Of_Date <= \'%s\' and MKV > 0" % (dstart, dend)
    hyhg_index_raw = pd.DataFrame(pd.read_sql(hyhg_index_sql, conn))
    '''Add GLIC Description Column'''
    glic = pd.DataFrame(pd.read_csv('glic_map.csv', encoding='utf8', index_col=0))  # GLIC data
    hyhg_index_raw = pd.merge(hyhg_index_raw, glic, on=['GLIC'], how='left')
    '''Index Adjustments to create weights'''
    daily_mkt_cap = hyhg_index_raw[['As_Of_Date', 'MKV']].groupby(['As_Of_Date']).sum()
    daily_mkt_cap = daily_mkt_cap.reset_index()
    hyhg_index_raw = pd.merge(hyhg_index_raw, daily_mkt_cap, on=['As_Of_Date'], how='left')
    hyhg_index_raw = hyhg_index_raw.rename(columns={'MKV_x': 'MKV', 'MKV_y': 'Indx_Dly_Tot_MKV', 'As_Of_Date': 'date'})
    hyhg_index_raw['Indx_Weight'] = hyhg_index_raw['MKV'] / hyhg_index_raw['Indx_Dly_Tot_MKV']
    hyhg_index_raw['date'] = pd.to_datetime(hyhg_index_raw['date'])
    '''Add S&P Letter Rating'''
    ratings = pd.DataFrame(pd.read_sql("SELECT * FROM bens_desk.yieldbook_credit_ratings", conn))
    hyhg_index_raw = pd.merge(hyhg_index_raw, ratings, on=['Rating'], how='left')
    hyhg_index_raw = hyhg_index_raw.rename(columns={'Rating_x':'Rating'})
    #hyhg_index_raw = hyhg_index_raw.drop(['Rating_y', 'Price_y'], axis=1)

    '''Calculate Percent Changes'''
    hyhg_index_raw = hyhg_index_raw.sort_values(['CUSIP', 'date']).reset_index(drop=True)
    hyhg_index_raw['PCT_Change'] = hyhg_index_raw.groupby('CUSIP')['Price'].pct_change()

    '''Adjust Raw Index for CUSIP Changes/corporate actions.  This sequence has to happen 
    after percent changes are calculated.  The main cusip column in the raw index feed needs 
    to be replaced by any value that exists in the 'linked_CUSIP' in order for the psa holdings 
    to perfectly match into the index dataframe.'''
    hyhg_index_raw['CUSIP'] = np.where(hyhg_index_raw['linked_CUSIP'].isnull(), hyhg_index_raw['CUSIP'],
                                       hyhg_index_raw['linked_CUSIP'])

    return hyhg_index_raw


def hyhg_psa_holding_query(dstart, dend):
    hyhg_psa_holding_sql = "SELECT * \
                FROM bens_desk.psa_hyhg_holdings \
                WHERE date >= \'%s\' and date <= \'%s\'" % (dstart, dend)
    hyhg_psa_holding_raw = pd.DataFrame(pd.read_sql(hyhg_psa_holding_sql, conn))
    '''PSA holdings adjustments'''
    hyhg_psa_holding_raw['CUSIP'] = hyhg_psa_holding_raw['CUSIP'].str[0:8]
    hyhg_psa_holding_raw['date'] = pd.to_datetime(hyhg_psa_holding_raw['date'])
    return hyhg_psa_holding_raw

'''Merge dataframes'''
def combine_hyhg(index,psa):
    MASTER_combined = pd.merge(index, psa, how='outer', on=['CUSIP', 'date'])

    '''Construct PSA Portfolio Weight Column'''
    MASTER_combined['PSA_MKV'] = (MASTER_combined['Price_x'] + MASTER_combined['ACRI']) * MASTER_combined['psa_par']
    PSA_daily_mkt_cap = MASTER_combined[['date', 'PSA_MKV']].groupby(['date']).sum()
    PSA_daily_mkt_cap = PSA_daily_mkt_cap.reset_index()
    MASTER_combined = pd.merge(MASTER_combined, PSA_daily_mkt_cap, on=['date'], how='left')
    MASTER_combined = MASTER_combined.rename(columns={'PSA_MKV_x': 'PSA_MKV', 'PSA_MKV_y': 'PSA_Dly_Tot_MKV', 'Price_x': 'Price'})
    MASTER_combined['PSA_Weight'] = MASTER_combined['PSA_MKV'] / MASTER_combined['PSA_Dly_Tot_MKV']

    return MASTER_combined

#def issuer_compress(MASTER):


