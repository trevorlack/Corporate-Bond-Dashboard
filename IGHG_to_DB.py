import MySQLdb
import sys
import datetime
import glob
import os
import csv

FileList=[]
os.chdir("S:\Trevor\MySQL Data\IG Index\DB Loader\Ready for DB")

readablefile = csv.reader(sys.stdin)
writablefile = csv.writer(sys.stdout)

try:
    db = MySQLdb.connect(
        host='localhost',
        user='tlack',
        passwd='Porsche911!',
        db='bens_desk'
    )

except Exception as e:
    sys.exit('cant connect')
cursor = db.cursor()

for file in sorted(glob.glob("*.csv")):
    print(file)
    tempfile = file + ".new"
    #print(tempfile)
    with open(file, 'rb') as infile, open(tempfile, 'wb') as outfile:
        writer = csv.writer(outfile)
        for row in csv.reader(infile):
            cols = list(row)
            #cols[1] = cols[1].strptime("%d/%m/%Y")
            datet = datetime.datetime.strptime(cols[1],"%m/%d/%Y")
            dater = datet.strftime("%Y:%m:%d %H:%M:%S")
            datet2 = datetime.datetime.strptime(cols[7], "%m/%d/%Y")
            dater2 = datet2.strftime("%Y:%m:%d %H:%M:%S")
            #print(datet)
            #print(dater)
            cols[1] = dater
            cols[7] = dater2
            #print(cols)
            partest = float(cols[11].replace(',',''))
            cols[11] = partest
            partest2 = float(cols[12].replace(',', ''))
            cols[12] = partest2
            partest3 = float(cols[8])
            cols[8] = partest3
            writer.writerow(cols)

    os.remove(file)
    os.rename(tempfile, file)

    f = open(file)
    z = csv.reader(f)
    for row in z:
    # print row[1:]

        cursor.execute("INSERT INTO ighg_index (As_Of_Date, CUSIP, ISIN, Parent, Ticker, Coupon, Maturity_Date, Rating, GLIC, COBS, PAR, MKV, AVLF, ACRI, Price, B_YTM, BYTW, MODD, EDUR, DURW, GSPRED, SPRDWCALL, OAS, CONVX, EFFCNVX, PrincRtn, IntRtn, RIRtn, TotalRtn) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",row[1:30])

        db.commit()
    #FileList.append(file)
cursor.close()
#print(FileList)