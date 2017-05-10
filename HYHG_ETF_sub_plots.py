import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
import numpy as np
from pandas.tools.plotting import scatter_matrix
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.mlab as mlab
import seaborn as sns
from scipy.stats import kendalltau
from scipy.stats import kstest, levene
from MySQL_Authorization import MySQL_Auth

dstart = datetime.date(2015,1,1)
#CUSIPT = '02155FAC9'

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

hyhg_etf = pd.DataFrame(pd.read_sql("SELECT Date, Ticker, NAV, Closing_Price, Volume, Shares_Outstanding, Bid, Ask, CR_RD, Average_Bid_Ask FROM bens_desk.hyhg_etf \
    WHERE Date > %s", conn, params={dstart}))

print(hyhg_etf.head(20))

mkt_cap = pd.DataFrame(hyhg_etf['Closing_Price']*hyhg_etf['Shares_Outstanding'], columns=['HYHG_Mkt_Cap'])
hyhg_etf = pd.concat([hyhg_etf,mkt_cap], axis=1)
hyhg_market_premium = pd.DataFrame((hyhg_etf['Closing_Price']-hyhg_etf['NAV'])/hyhg_etf['NAV'] * 100, columns=['HYHG_Market_Premium'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_market_premium], axis=1)
hyhg_spread = pd.DataFrame(hyhg_etf['Ask']-hyhg_etf['Bid'], columns=['HYHG_Spread'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_spread], axis=1)
hyhg_spread_pct = pd.DataFrame(hyhg_etf['HYHG_Spread']/ hyhg_etf['Closing_Price'] * 100, columns=['HYHG_Spread_pct'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_spread_pct], axis=1)
hyhg_etf['Date'] = pd.to_datetime(hyhg_etf['Date'])
hyhg_etf = hyhg_etf.set_index(['Date'])

hyhg_etf_Week = hyhg_etf.resample('W-WED', how={'CR_RD': np.sum, 'HYHG_Spread': np.mean, 'HYHG_Market_Premium': np.mean, 'HYHG_Spread_pct': np.mean, 'Volume': np.sum})
hyhg_etf_Week = hyhg_etf_Week.reset_index()

Dealer_Data = pd.read_csv('C:/Users/tlack/Documents/Python Scripts/Dealer_Weekly_Data.csv', index_col=False)
Dealer_Data['Date'] = pd.to_datetime(Dealer_Data['Date'])
#Dealer_Data = pd.DataFrame(Dealer_Data[['Date','Plus 10 Years_Inventory', '13 Months to 5 Years_Inventory', '5 to 10 Years_Inventory','Less than 13 Months_Inventory']])

hyhg_data_set = pd.merge(hyhg_etf_Week, Dealer_Data, on=['Date'], how='left')

hyhg_data_set = hyhg_data_set.set_index(['Date'])

top = plt.subplot2grid((12,6), (0,0), rowspan=3, colspan=6)
top.plot(hyhg_data_set.index, hyhg_data_set['Plus 10 Years_Inventory'], label='>10 Years')
top.plot(hyhg_data_set.index, hyhg_data_set['13 Months to 5 Years_Inventory'], label='>13 Months < 5 Years')
top.plot(hyhg_data_set.index, hyhg_data_set['5 to 10 Years_Inventory'], label='>5 Years < 10 Years')
top.plot(hyhg_data_set.index, hyhg_data_set['Less than 13 Months_Inventory'], label='< 13 Months')
plt.title('Weekly Dealer INVENTORY As Reported to NY Federal Reserve')
plt.legend(loc=2)
bottom3=plt.subplot2grid((12,6), (3,0), rowspan=3, colspan=6)
bottom3.plot(hyhg_data_set.index, hyhg_data_set['Plus 10 Years_Trade'], label='>10 Years Trade')
bottom3.plot(hyhg_data_set.index, hyhg_data_set['13 Months to 5 Years_Trade'], label='>13 Months < 5 Years Trade')
bottom3.plot(hyhg_data_set.index, hyhg_data_set['5 to 10 Years_Trade'], label='>5 Years < 10 Years Trade')
bottom3.plot(hyhg_data_set.index, hyhg_data_set['Less than 13 Months_Trade'], label='< 13 Months Trade')
plt.title('Weekly Dealer TRADING As Reported to NY Federal Reserve')
#plt.legend(loc=2)
bottom=plt.subplot2grid((12,6), (6,0), rowspan=2, colspan=6)
bottom.bar(hyhg_data_set.index, hyhg_data_set['CR_RD'])
plt.title('HYHG CR/RD Activity (Weekly Sum)')
bottom1=plt.subplot2grid((12,6), (8,0), rowspan=2, colspan=6)
bottom1.bar(hyhg_data_set.index, hyhg_data_set['HYHG_Spread_pct'])
plt.title('HYHG Bid/Ask Spread (Weekly Average)')
bottom2=plt.subplot2grid((12,6), (10,0), rowspan=2, colspan=6)
bottom2.bar(hyhg_data_set.index, hyhg_data_set['HYHG_Market_Premium'])
plt.title('HYHG Market Premium (Weekly Average)')

plt.gcf().set_size_inches(30,24)
plt.subplots_adjust(hspace=1.25)
plt.show()

'''
sns.set(style="ticks")
x = hyhg_data_set['Volume']
y = hyhg_data_set['HYHG_Spread']

sns.jointplot(x, y, kind="hex", stat_func=kendalltau, color="#4CB391")
sns.plt.show()
'''