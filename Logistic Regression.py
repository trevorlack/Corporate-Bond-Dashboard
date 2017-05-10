import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
from sklearn import linear_model, metrics, cross_validation
from numpy import *
from sklearn.preprocessing import MinMaxScaler
from pandas.tools.plotting import scatter_matrix
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.mlab as mlab
import seaborn as sns
import pylab
from scipy.stats import kstest, levene
import scipy.cluster.hierarchy as hac
from MySQL_Authorization import MySQL_Auth

dstart = datetime.date(2016,5,3)
#CUSIPT = '02155FAC9'

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

hyhg_liquid_data = pd.DataFrame(pd.read_sql("SELECT Date, Volume, ID_CUSIP, Index_CUSIP, Bid, Ask FROM bens_desk.hyhg_liquidity \
                                            WHERE Date > %s and TRADE_STATUS = 'Y'", conn, params={dstart}))
hyhg_outstanding = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP, Outstanding_Amount FROM bens_desk.hyhg_outstanding_amounts \
                                            WHERE  Outstanding_Amount > 1000000000 AND As_Of_Date > %s", conn, params={dstart}))
hyhg_etf = pd.DataFrame(pd.read_sql("SELECT Date, Ticker, NAV, Closing_Price, Volume, Shares_Outstanding, Bid, Ask, CR_RD, Average_Bid_Ask FROM bens_desk.hyhg_etf \
                                            WHERE Date > %s", conn, params={dstart}))
hyhg_IDC_price = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP, Price, MKV FROM bens_desk.hyhg_index \
                                            WHERE As_Of_Date > %s", conn, params={dstart}))
hyhg_etf = hyhg_etf.rename(columns={'Volume':'HYHG_Volume'})
hyhg_IDC_price = hyhg_IDC_price.rename(columns={'CUSIP':'Index_CUSIP'})

'''HYHG Index Table Builder'''
spread = pd.DataFrame(hyhg_liquid_data['Ask']-hyhg_liquid_data['Bid'], columns=['Spread'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread], axis=1)
hyhg_liquid_data = hyhg_liquid_data.rename(columns={'ID_CUSIP': 'CUSIP'})
midpoint = (pd.DataFrame((hyhg_liquid_data['Ask']+hyhg_liquid_data['Bid'])/2, columns=['Midpoint']))
hyhg_liquid_data = pd.concat([hyhg_liquid_data,midpoint], axis=1)
spread_pct = pd.DataFrame(hyhg_liquid_data['Spread']/hyhg_liquid_data['Midpoint'] * 100, columns=['Spread_pct'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread_pct], axis=1)
spread_cost = pd.DataFrame(hyhg_liquid_data['Spread']*hyhg_liquid_data['Volume'], columns=['Spread_Cost'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread_cost], axis=1)

'''HYHG Index Outstanding Amount'''
hyhg_outstanding = hyhg_outstanding.rename(columns={'As_Of_Date': 'Date'})
hyhg_outstanding['Outstanding_Amount'] = hyhg_outstanding['Outstanding_Amount'].abs()
#hyhg_outstanding['Outstanding_Amount'] = hyhg_outstanding['Outstanding_Amount']/1000
hyhg_outstanding['Date'] = pd.to_datetime(hyhg_outstanding['Date'])
hyhg_liquid_data = pd.merge(hyhg_liquid_data, hyhg_outstanding, on=['Date', 'CUSIP'], how='left')
turnover_pct = pd.DataFrame(hyhg_liquid_data['Volume']*1000/hyhg_liquid_data['Outstanding_Amount']*100, columns=['Turnover_pct'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,turnover_pct], axis=1)
#hyhg_liquid_data = hyhg_liquid_data.drop(['Bid', 'Ask', 'CUSIP', 'Spread', 'Midpoint', 'Outstanding_Amount'], axis=1)
hyhg_liquid_data = hyhg_liquid_data.rename(columns={'Date_x':'Date'})

'''HYHG ETF Table Builder'''
hyhg_spread = pd.DataFrame(hyhg_etf['Ask']-hyhg_etf['Bid'], columns=['HYHG_Spread'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_spread], axis=1)
hyhg_midpoint = (pd.DataFrame((hyhg_etf['Ask']+hyhg_etf['Bid'])/2, columns=['HYHG_Midpoint']))
hyhg_etf = pd.concat([hyhg_etf,hyhg_midpoint], axis=1)
hyhg_spread_pct = pd.DataFrame(hyhg_etf['HYHG_Spread']/hyhg_etf['HYHG_Midpoint'] * 100, columns=['HYHG_Spread_pct'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_spread_pct], axis=1)
hyhg_market_premium = pd.DataFrame((hyhg_etf['Closing_Price']-hyhg_etf['NAV'])/hyhg_etf['NAV'] * 100, columns=['HYHG_Market_Premium'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_market_premium], axis=1)
hyhg_turnover = pd.DataFrame(hyhg_etf['HYHG_Volume']/hyhg_etf['Shares_Outstanding'] * 100, columns=['HYHG_Turnover_pct'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_turnover], axis=1)
hyhg_spread_cost = pd.DataFrame(hyhg_etf['HYHG_Volume']*hyhg_etf['HYHG_Spread'], columns=['HYHG_Spread_Cost'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_spread_cost], axis=1)
hyhg_turnover = pd.DataFrame(hyhg_etf['HYHG_Volume']/hyhg_etf['Shares_Outstanding'] * 100, columns=['HYHG_Turnover_pct'])
hyhg_etf = pd.concat([hyhg_etf,hyhg_turnover], axis=1)
#hyhg_Creation = hyhg_etf[hyhg_etf.CR_RD > 0]
#print(hyhg_Creation)

'''Combine Tables adn Groupby'''
hyhg_liquid_data = pd.merge(hyhg_liquid_data, hyhg_etf, on=['Date'], how='left')
hyhg_liquid_data5 = pd.DataFrame(hyhg_liquid_data[['Date','CUSIP','Turnover_pct', 'Volume', 'Outstanding_Amount']].sort_values(by='Turnover_pct', ascending=False))
hyhg_etf_feeder = hyhg_etf.drop(['Ticker', 'NAV', 'Closing_Price', 'Shares_Outstanding', 'Bid', 'Ask', 'CR_RD', 'Average_Bid_Ask', 'HYHG_Spread', 'HYHG_Midpoint'], axis=1)
hyhg_etf_feeder = hyhg_etf_feeder.set_index('Date')
hyhg_MASTER_Spreader = hyhg_liquid_data['Spread_pct'].groupby(hyhg_liquid_data['Date'])
hyhg_MASTER_Spread = pd.DataFrame(hyhg_MASTER_Spreader.mean())
hyhg_MASTER_Turnoverer = hyhg_liquid_data['Turnover_pct'].groupby(hyhg_liquid_data['Date'])
hyhg_MASTER_Turnover = pd.DataFrame(hyhg_MASTER_Turnoverer.mean())
hyhg_MASTER_SpreadCoster = hyhg_liquid_data['Spread_Cost'].groupby(hyhg_liquid_data['Date'])
hyhg_MASTER_Spread_Cost = pd.DataFrame(hyhg_MASTER_SpreadCoster.mean())
hyhg_MASTER = hyhg_MASTER_Spread.join(hyhg_MASTER_Turnover)
hyhg_MASTER = hyhg_MASTER.join(hyhg_MASTER_Spread_Cost)
hyhg_MASTER = hyhg_MASTER.join(hyhg_etf_feeder)

hyhg_lag = hyhg_MASTER['HYHG_Spread_Cost'].shift(2)
hyhg_lag = pd.DataFrame(hyhg_lag)
hyhg_lag = hyhg_lag.rename(columns={'HYHG_Spread_Cost':'HYHG_Spread_Cost_lag'})

hyhg_MASTER = pd.merge(hyhg_MASTER, hyhg_lag, left_index=True, right_index=True)

hyhg_MASTER = hyhg_MASTER.dropna(how='any', axis=0)

hyhg_MASTER_Standardized = hyhg_MASTER
#hyhg_MASTER_Standardized['Trade_cap'] = (hyhg_MASTER_Standardized['Trade_cap']-hyhg_MASTER_Standardized['Trade_cap'].mean())/hyhg_MASTER_Standardized['Trade_cap'].std()
hyhg_MASTER_Standardized['HYHG_Volume'] = (hyhg_MASTER_Standardized['HYHG_Volume']-hyhg_MASTER_Standardized['HYHG_Volume'].mean())/hyhg_MASTER_Standardized['HYHG_Volume'].std()
hyhg_MASTER_Standardized['Spread_Cost'] = (hyhg_MASTER_Standardized['Spread_Cost']-hyhg_MASTER_Standardized['Spread_Cost'].mean())/hyhg_MASTER_Standardized['Spread_Cost'].std()
hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'] = (hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag']-hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'].mean())/hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'].std()

lm = linear_model.SGDRegressor()
lm.fit(hyhg_MASTER_Standardized[['Spread_Cost', 'Turnover_pct', 'Spread_pct']], hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'])

print('Gradient Descent R2:', lm.score(hyhg_MASTER_Standardized[['Spread_Cost', 'Turnover_pct', 'Spread_pct']], hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag']))
print('Gradient Descent MSE:', metrics.mean_squared_error(hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'], lm.predict(hyhg_MASTER_Standardized[['Spread_Cost', 'Turnover_pct', 'Spread_pct']])))

cmap = sns.diverging_palette(10, 220, as_cmap=True)
correlations = hyhg_MASTER.corr()
plt.show()
print(correlations)
sns.heatmap(correlations, cmap=cmap)
sns.plt.show()

lin = pd.np.polyfit(hyhg_MASTER_Standardized['Spread_pct'], hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'],2)
print(lin)

x = hyhg_MASTER_Standardized['Spread_pct'].as_matrix()
y = hyhg_MASTER_Standardized['HYHG_Spread_Cost_lag'].as_matrix()
print(type(x))
# fit the data with a 4th degree polynomial
z4 = polyfit(x, y, 4)
p4 = poly1d(z4) # construct the polynomial


z5 = polyfit(x, y, 6)
p5 = poly1d(z5)

#xx = linspace(min(x), max(x), 100)
xx = linspace(.4, max(x), 100)
pylab.plot(x, y, 'o', xx, p4(xx),'-g', xx, p5(xx),'-b')
pylab.legend(['data to fit', '4th degree poly', '6th degree poly'])
#Axes.autoscale(enable=True, axis='both')
#pylab.axis.autoscale()
#pylab.axis([-2,2,-2,2])
pylab.show()

