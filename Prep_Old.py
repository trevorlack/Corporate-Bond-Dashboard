import win32com.client
import os
import sys
import glob

#namer = "HY_Citi_Index_020217.xlsx"
os.chdir("S:\Trevor\MySQL Data")

for file in list(glob.glob("*.xlsm")):
    namer = file
    Checker = "REPO"
    x1 = win32com.client.Dispatch("Excel.Application")
    x1.Visible = False
    x1.Workbooks.Open(os.path.join(os.getcwd(), namer))

    wb = x1.Workbooks(namer)
    ws = wb.Worksheets("HY Index Holdings")

    ws.Range("E5").Select()
    Cell = x1.Sheets("HY Index Holdings").Range("E5").Value
    print(Cell)

    if Cell == "REPO":
        i=1
        for i in range(1,6):
            ws.Rows(1).Delete()
        ws.Range("A1").Formula = "=Counta(B1:B1000)"
    else:
        wb.Close(True)
        sys.exit()

    ws.Range("A1").Formula = "=Counta(B1:B1000)"
    x1.Activeworkbook.Save()
    x1.Activeworkbook.Close(True)
