import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as md
import seaborn as sns
from scipy.stats import kstest, levene

dstart = datetime.date(2017,1,1)
#CUSIPT = '02155FAC9'

conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:Porsche911!@localhost/bens_desk')

hyhg_liquid_data = pd.DataFrame(pd.read_sql("SELECT Date, Volume, ID_CUSIP, Bid, Ask FROM bens_desk.hyhg_liquidity \
                                            ", conn))

trade_basket = pd.DataFrame(pd.read_csv('basket.csv'))

#hyhg_outstanding = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP, Outstanding_Amount, Bid, Ask FROM bens_desk.hyhg_outstanding_amounts \
#                                             WHERE As_Of_Date > %s", conn, params={dstart}))
#print(trade_basket.columns.values)
#print(trade_basket.head(5))

spread = pd.DataFrame(hyhg_liquid_data['Ask']-hyhg_liquid_data['Bid'], columns=['Spread'])
hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread], axis=1)
hyhg_liquid_data = hyhg_liquid_data.rename(columns={'ID_CUSIP': 'CUSIP'})
hyhg_liquid_data = hyhg_liquid_data.merge(trade_basket, on='CUSIP', how = 'left')

#print(hyhg_liquid_data.head(5))
grouped = hyhg_liquid_data['Volume'].groupby(hyhg_liquid_data['CUSIP'])
#print(grouped.mean())
av_volume = pd.DataFrame(grouped.mean())
av_volume = av_volume.reset_index()
av_volume = av_volume.merge(hyhg_liquid_data, on= 'CUSIP', how='left')
av_volume = av_volume.dropna(subset=['Par'])
av_volume = av_volume.drop_duplicates(['CUSIP'])
ratio = pd.DataFrame(av_volume['Par']/av_volume['Volume_x'], columns=['Ratio'])
av_volume = pd.concat([av_volume,ratio], axis=1)


print(av_volume)

av_volume.to_csv('output.csv')

