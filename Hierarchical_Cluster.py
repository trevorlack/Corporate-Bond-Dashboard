import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
from sklearn.decomposition import PCA as sklearnPCA
from pandas.tools.plotting import scatter_matrix
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.mlab as mlab
import seaborn as sns
from scipy.stats import kstest, levene
from MySQL_Authorization import MySQL_Auth

dstart = datetime.date(2016,5,3)
#CUSIPT = '02155FAC9'

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

hyhg_liquid_data = pd.DataFrame(pd.read_sql("SELECT Date, Volume, ID_CUSIP, Index_CUSIP, Bid, Ask FROM bens_desk.hyhg_liquidity \
                                            WHERE Date > %s and TRADE_STATUS = 'Y'", conn, params={dstart}))
hyhg_outstanding = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP, Outstanding_Amount FROM bens_desk.hyhg_outstanding_amounts \
                                            WHERE As_Of_Date > %s", conn, params={dstart}))
hyhg_etf = pd.DataFrame(pd.read_sql("SELECT Date, Ticker, NAV, Closing_Price, Volume, Shares_Outstanding, Bid, Ask, CR_RD, Average_Bid_Ask FROM bens_desk.hyhg_etf \
                                            WHERE Date > %s", conn, params={dstart}))
hyhg_IDC_price = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP, Price, MKV FROM bens_desk.hyhg_index \
                                            WHERE As_Of_Date > %s", conn, params={dstart}))
hyhg_etf = hyhg_etf.rename(columns={'Volume':'HYHG_Volume'})

'''HYHG Index Table Builder'''
spread = pd.DataFrame(hyhg_liquid_data['Ask']-hyhg_liquid_data['Bid'], columns=['Spread'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread], axis=1)
hyhg_liquid_data = hyhg_liquid_data.rename(columns={'ID_CUSIP': 'CUSIP'})
midpoint = (pd.DataFrame((hyhg_liquid_data['Ask']+hyhg_liquid_data['Bid'])/2, columns=['Midpoint']))
hyhg_liquid_data = pd.concat([hyhg_liquid_data,midpoint], axis=1)
spread_pct = pd.DataFrame(hyhg_liquid_data['Spread']/hyhg_liquid_data['Midpoint'] * 100, columns=['Spread_pct'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread_pct], axis=1)

hyhg_outstanding = hyhg_outstanding.rename(columns={'As_Of_Date': 'Date'})
hyhg_outstanding['Outstanding_Amount'] = hyhg_outstanding['Outstanding_Amount'].abs()
#hyhg_outstanding['Outstanding_Amount'] = hyhg_outstanding['Outstanding_Amount']/1000
hyhg_outstanding['Date'] = pd.to_datetime(hyhg_outstanding['Date'])
hyhg_liquid_data = pd.merge(hyhg_liquid_data, hyhg_outstanding, on=['Date', 'CUSIP'], how='left')

turnover_pct = pd.DataFrame(hyhg_liquid_data['Volume']*1000/hyhg_liquid_data['Outstanding_Amount']*100, columns=['Turnover_pct'])

hyhg_liquid_data = pd.concat([hyhg_liquid_data,turnover_pct], axis=1)
#hyhg_liquid_data = hyhg_liquid_data.drop(['Bid', 'Ask', 'CUSIP', 'Spread', 'Midpoint', 'Outstanding_Amount'], axis=1)
hyhg_liquid_data = hyhg_liquid_data.rename(columns={'Date_x':'Date'})

hyhg_IDC_price = hyhg_IDC_price.rename(columns={'CUSIP':'Index_CUSIP'})


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

hyhg_liquid_data = pd.merge(hyhg_liquid_data, hyhg_etf, on=['Date'], how='left')
hyhg_liquid_data5 = pd.DataFrame(hyhg_liquid_data[['Date','CUSIP','Turnover_pct', 'Volume', 'Outstanding_Amount']].sort_values(by='Turnover_pct', ascending=False))
hyhg_liquid_data5.head(200).to_csv('output.csv')
hyhg_etf.head(200).to_csv('output_HYHG.csv')

hyhg_etf_feeder = hyhg_etf.drop(['Ticker', 'NAV', 'Closing_Price', 'Shares_Outstanding', 'Bid', 'Ask', 'CR_RD', 'Average_Bid_Ask', 'HYHG_Spread', 'HYHG_Midpoint'], axis=1)
hyhg_etf_feeder = hyhg_etf_feeder.set_index('Date')
hyhg_MASTER = hyhg_liquid_data['Spread_pct'].groupby(hyhg_liquid_data['Date'])
hyhg_MASTER_Spread = pd.DataFrame(hyhg_MASTER.mean())
hyhg_MASTER = hyhg_liquid_data['Turnover_pct'].groupby(hyhg_liquid_data['Date'])
hyhg_MASTER_Turnover = pd.DataFrame(hyhg_MASTER.mean())

hyhg_MASTER = hyhg_MASTER_Spread.join(hyhg_MASTER_Turnover)
hyhg_MASTER = hyhg_MASTER.join(hyhg_etf_feeder)

hyhg_IDC_price = hyhg_IDC_price.rename(columns={'CUSIP':'Index_CUSIP','As_Of_Date':'Date'})
hyhg_IDC_price2 = pd.merge(hyhg_liquid_data, hyhg_IDC_price, on=['Date', 'Index_CUSIP'], how='left')
#print(hyhg_IDC_price2.head(20))
trade_cap = pd.DataFrame(hyhg_IDC_price2['Volume'] * hyhg_IDC_price2['Price'], columns=['Trade_cap'])
hyhg_IDC_price2 = pd.concat([hyhg_IDC_price2,trade_cap], axis=1)
hyhg_MASTER3 = hyhg_IDC_price2['Trade_cap'].groupby(hyhg_IDC_price2['Date'])
hyhg_MASTER3 = pd.DataFrame(hyhg_MASTER3.sum())
hyhg_MASTER = hyhg_MASTER.join(hyhg_MASTER3)
#print(hyhg_MASTER)
hyhg_PCA = hyhg_MASTER.drop(['HYHG_Volume', 'Trade_cap'], axis=1)


hyhg_liquid_data = hyhg_liquid_data.drop(['Index_CUSIP'], axis=1)


sklearn_pca = sklearnPCA(n_components=5)
Y_sklearn = sklearn_pca.fit_transform(hyhg_PCA)
print(Y_sklearn)

with plt.style.context('seaborn-whitegrid'):
    plt.figure(figsize=(6, 4))
    for lab, col in zip(('Iris-setosa', 'Iris-versicolor', 'Iris-virginica'),
                        ('blue', 'red', 'green')):
        plt.scatter(Y_sklearn[y==lab, 0],
                    Y_sklearn[y==lab, 1],
                    label=lab,
                    c=col)
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.legend(loc='lower center')
    plt.tight_layout()
    plt.show()





