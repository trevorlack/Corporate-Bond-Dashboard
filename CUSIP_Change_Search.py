import pymysql
import pandas as pd
import datetime
from sqlalchemy import create_engine

'''
sqlalchemy
mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
'''
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:Porsche911!@localhost/bens_desk')

dstart = datetime.date(2017,2,6)

cusiplist = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP FROM bens_desk.hyhg_index \
    WHERE As_Of_Date > %s", conn, params={dstart}))
print(cusiplist)
