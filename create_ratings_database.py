import pymysql
import pandas as pd
from sqlalchemy import create_engine

'''
sqlalchemy
mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
'''
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:Porsche911!@localhost/bens_desk')

lister = pd.DataFrame(pd.read_csv("Yieldbook matrix.csv"))
#lister.columns=['numerical_code','letter_rating']
print(lister)
lister.to_sql('yieldbook_credit_ratings', engine, if_exists='append', index=False)