import MySQLdb
import sys
import xlrd
import datetime
import glob
import os
import csv
import time
import calendar

FileList=[]
os.chdir("S:\Trevor\MySQL Data\IG Index\DB Loader\Ready for DB")

readablefile = csv.reader(sys.stdin)
writablefile = csv.writer(sys.stdout)

for file in list(glob.glob("*.csv")):
    print(file)
    tempfile = "IGHG.csv.new"
    print(tempfile)
    with open(file, 'rb') as infile, open(tempfile, 'wb') as outfile:
        writer = csv.writer(outfile)
        for row in csv.reader(infile):
            cols = list(row)
            print cols[1]
            #cols[1] = cols[1].strptime("%d/%m/%Y")
            datet = datetime.datetime.strptime(cols[1],"%m/%d/%Y")
            print(datet)
            cols[1] = datet
            writer.writerow(cols)
    os.rename(tempfile, file)
