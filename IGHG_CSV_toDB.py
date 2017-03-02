import MySQLdb
import sys
import xlrd
import datetime
import glob
import os
import csv

FileList=[]
os.chdir("S:\Trevor\MySQL Data\IG Index\DB Loader\Ready for DB")
try:
    db = MySQLdb.connect(
        host='localhost',
        user='******',
        passwd='******',
        db='bens_desk'
    )

except Exception as e:
    sys.exit('cant connect')
cursor = db.cursor()

for file in list(glob.glob("*.csv")):
    print(file)

    f = open(file)
    z= csv.reader(f)
    for row in z:
        #print row[1:]

        cursor.execute("INSERT INTO ighg_index (As_Of_Date, CUSIP, ISIN, Parent, Ticker, Coupon, Maturity_Date, Rating, GLIC, COBS, PAR, MKV, AVLF, ACRI, Price, B_YTM, BYTW, MODD, EDUR, DURW, GSPRED, SPRDWCALL, OAS, CONVX, EFFCNVX, PrincRtn, IntRtn, RIRtn, TotalRtn) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row[1:30])

        db.commit()

cursor.close()
