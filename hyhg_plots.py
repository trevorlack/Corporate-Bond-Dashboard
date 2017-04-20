import pandas as pd
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
import matplotlib.dates as md
import seaborn as sns
from scipy.stats import kstest, levene

CUSIPT = '02155FAC9'

conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:Porsche911!@localhost/bens_desk')

hyhg_liquid_data = pd.DataFrame(pd.read_sql("SELECT Date, Volume, ID_CUSIP, Index_CUSIP, Bid, Ask FROM bens_desk.hyhg_liquidity \
                                            WHERE ID_CUSIP = %s", conn, params={CUSIPT}))
print(hyhg_liquid_data.head(20))

spread = pd.DataFrame(hyhg_liquid_data['Ask']-hyhg_liquid_data['Bid'], columns=['Spread'])
print(spread)

hyhg_liquid_data = pd.concat([hyhg_liquid_data,spread], axis=1)
hyhg_liquid_data = hyhg_liquid_data.set_index(['Date'])
print(hyhg_liquid_data.head(20))
corr_mat = hyhg_liquid_data.corr()[hyhg_liquid_data.corr() !=1]
print(corr_mat)
#hyhg_liquid_data['Date'] = md.dates.date2num(hyhg_liquid_data(['Date']))

#plt.show(hyhg_liquid_data.plot(x='Date', y='Volume', c='Spread', kind='scatter'))
#plt.show(hyhg_liquid_data['Volume'].hist(bins=20))

top = plt.subplot2grid((4,4), (0,0), rowspan=3, colspan=4)
top.plot(hyhg_liquid_data.index, hyhg_liquid_data['Spread'], label='Spreads')
plt.title('Spread Data for CUSIP %s' % (CUSIPT))
plt.legend(loc=2)
bottom=plt.subplot2grid((4,4), (3,0), rowspan=1, colspan=4)
bottom.bar(hyhg_liquid_data.index, hyhg_liquid_data['Volume'])
plt.title('Daily Volume')
plt.gcf().set_size_inches(15,12)
plt.subplots_adjust(hspace=.75)
plt.show()