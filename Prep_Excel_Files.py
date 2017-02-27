import win32com.client
import os
import sys
import glob
from win32com.client import constants

#Folder that the code will search in
os.chdir("S:\Trevor\MySQL Data")
FileList=[]
x1 = win32com.client.DispatchEx("Excel.Application")
x1.Visible = True

for file in list(glob.glob("*.xlsm")):
    Checker = "REPO"
    BlankRow = ""
    print(file)
    #DispatchEx launches a new instance of excel.  The below worked whereas the genCache method would crash

    x1.Workbooks.Open(os.path.join(os.getcwd(), file))

    wb = x1.Workbooks(file)
    ws = wb.Worksheets("HY Index Holdings")

    ws.Range("E5").Select()
    Cell = x1.Sheets("HY Index Holdings").Range("E5").Value
    # Check and see if Repo line item is still in the file to make sure it doesnt get prepped twice.
    if Cell == "REPO":
        i=1
        for i in range(1,6):
            ws.Rows(1).Delete()
        ws.Range("A1").Formula = "=Counta(B1:B1000)"
        cc = x1.Sheets("HY Index Holdings").Range("A1").Value
        c=int(cc)
        r=1
        for r in range(1,c):
            #Search for empty rows of data and delete them
            CUSIP=x1.Sheets("HY Index Holdings").Range("C%d" % r).Value
            if CUSIP == None:
                ws.Rows(r).Delete()
    else:
        wb.Close(True)
        sys.exit()

    ws.Range("A1").Formula = "=Counta(B1:B1000)"
    x1.ActiveWorkbook.Save()
    x1.ActiveWorkbook.Close(True)
    FileList.append(file)
print(FileList)

