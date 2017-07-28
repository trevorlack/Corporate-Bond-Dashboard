'''
['date' 'CUSIP' 'ISIN' 'Parent' 'Ticker' 'Coupon' 'Maturity_Date' 'Rating'
 'GLIC' 'COBS' 'PAR' 'MKV' 'AVLF' 'ACRI' 'Price' 'B_YTM' 'BYTW' 'MODD'
 'EDUR' 'DURW' 'GSPRED' 'SPRDWCALL' 'OAS' 'CONVX' 'EFFCNVX' 'PrincRtn'
 'IntRtn' 'RIRtn' 'TotalRtn' 'linked_CUSIP' 'rebal_action'
 'rebal_add_ref_price' 'iBENTIFIER_x' 'corporate_action' 'description'
 'Indx_Dly_Tot_MKV' 'Indx_Weight' 'psa_par' 'iBENTIFIER_y' 'Price_y'
 'PCT_Change' 'PSA_MKV' 'PSA_Dly_Tot_MKV' 'PSA_Weight']
'''


from bokeh.layouts import row
from bokeh.plotting import figure, show, output_file
import pandas as pd


'''Industry Dot Plot'''
def hyhg_industry_dot_plots(HYHG_MASTER, date):

    HYHG_Industry = HYHG_MASTER[['description', 'Indx_Weight', 'PSA_Weight']].groupby('description').sum()
    HYHG_Industry['Industry_Weight_Diff'] = (HYHG_Industry['PSA_Weight'] - HYHG_Industry['Indx_Weight']) * 10000
    HYHG_Industry = HYHG_Industry.reset_index()
    print(HYHG_Industry)
    '''
    HYHG_Issuer =
    HYHG_Maturity =
    HYHG_ Rating =
    HYHG_OAS =
    HYHG_YTM =
    HYHG_Duration =
    '''
    Industry_List = HYHG_Industry['description'].tolist()
    Weight_List = HYHG_Industry['Industry_Weight_Diff'].tolist()
    factors = Industry_List
    x = Weight_List

    dot = figure(title="Fund vs Index by Sector bps (+)Fund Overweight / (-)Fund Underweight  As Of: " + date, tools="hover", toolbar_location=None,
            y_range=factors, x_range=[min(Weight_List)*1.05, max(Weight_List)*1.05])
    dot.yaxis.axis_label_text_font_size = '20pt'
    dot.segment(0, factors, x, factors, line_width=2, line_color="green", )
    dot.circle(x, factors, size=15, fill_color="orange", line_color="green", line_width=3, )
    #p.yaxis.major_label_orientation =

    output_file("HYHG_Industry.html", title="HYHG_Industry_CS.py")
    show(row(dot, sizing_mode="scale_width"))  # open a browser

'''Issuer Dot Plot'''
def hyhg_issuer_dot_plots(HYHG_MASTER, date):
    HYHG_Issuer = HYHG_MASTER[['Ticker', 'Indx_Weight', 'PSA_Weight']].groupby('Ticker').sum()
    HYHG_Issuer = HYHG_Issuer.fillna(value=0)
    HYHG_Issuer['Ticker_Weight_Diff'] = (HYHG_Issuer['PSA_Weight'] - HYHG_Issuer['Indx_Weight']) * 10000
    HYHG_Issuer = HYHG_Issuer.reset_index()
    print(HYHG_Issuer)

    Issuer_List = HYHG_Issuer['Ticker'].tolist()
    Weight_List = HYHG_Issuer['Ticker_Weight_Diff'].tolist()
    factors = Issuer_List
    x = Weight_List

    dot = figure(title="Fund vs Index by Issuer bps (+)Fund Overweight / (-)Fund Underweight -) As Of: " + date, tools="hover", toolbar_location=None,
            y_range=factors, x_range=[min(Weight_List)*1.05, max(Weight_List)*1.05])
    dot.yaxis.axis_label_text_font_size = '20pt'
    dot.segment(0, factors, x, factors, line_width=2, line_color="green", )
    dot.circle(x, factors, size=15, fill_color="orange", line_color="green", line_width=3, )
    #p.yaxis.major_label_orientation =

    output_file("HYHG_Issuer.html", title="HYHG_Issuer_CS.py")
    show(row(dot, sizing_mode="scale_width"))  # open a browser

'''Maturity'''
def hyhg_maturity_dot_plots(HYHG_MASTER, date):
    HYHG_MASTER['Maturity_Date'] = pd.to_datetime(HYHG_MASTER['Maturity_Date'])
    HYHG_Maturity = HYHG_MASTER[['Maturity_Date', 'Indx_Weight', 'PSA_Weight']].set_index('Maturity_Date')
    HYHG_Maturity2 = HYHG_Maturity.groupby((HYHG_Maturity.index.year//2)*2).sum()
    #HYHG_Maturity = HYHG_MASTER[['Maturity_Date', 'Indx_Weight', 'PSA_Weight']].groupby(pd.Grouper(key='Maturity_Date', freq='2AS')).sum()

    HYHG_Maturity = HYHG_Maturity2.fillna(value=0)
    HYHG_Maturity['Maturity_Bucket_Diff'] = (HYHG_Maturity['PSA_Weight'] - HYHG_Maturity['Indx_Weight']) * 10000
    HYHG_Maturity = HYHG_Maturity.reset_index()
    HYHG_Maturity = HYHG_Maturity.rename(columns={'index':'Maturity_Date'})

    HYHG_Maturity['Maturity_Date'] = HYHG_Maturity['Maturity_Date'].astype(int)
    HYHG_Maturity['Maturity_Date'] = HYHG_Maturity['Maturity_Date'].astype(str)
    Mat_offset = HYHG_Maturity['Maturity_Date'].shift(1)
    Mat_offset = Mat_offset.fillna(value='Present')
    HYHG_Maturity['Maturity_Date'] = Mat_offset+' to '+HYHG_Maturity['Maturity_Date']

    Issuer_List = HYHG_Maturity['Maturity_Date'].tolist()
    Weight_List = HYHG_Maturity['Maturity_Bucket_Diff'].tolist()

    factors = Issuer_List
    x = Weight_List

    dot = figure(title="Fund vs Index by Maturity bps (+)Fund Overweight / (-)Fund Underweight -) As Of: " + date, tools="hover", toolbar_location=None,
            y_range=factors, x_range=[min(Weight_List)*1.05, max(Weight_List)*1.05])
    dot.yaxis.axis_label_text_font_size = '20pt'
    dot.segment(0, factors, x, factors, line_width=2, line_color="green", )
    dot.circle(x, factors, size=15, fill_color="orange", line_color="green", line_width=3, )
    #p.yaxis.major_label_orientation =

    output_file("HYHG_Maturity.html", title="HYHG_Maturity_CS.py")
    show(row(dot, sizing_mode="scale_width"))  # open a browser


'''Rating'''
def hyhg_rating_dot_plots(HYHG_MASTER, date):
    HYHG_rating = HYHG_MASTER[['letter_rating', 'Indx_Weight', 'PSA_Weight']].groupby('letter_rating').sum()
    HYHG_rating = HYHG_rating.fillna(value=0)
    HYHG_rating['Rating_Weight_Diff'] = (HYHG_rating['PSA_Weight'] - HYHG_rating['Indx_Weight']) * 10000
    HYHG_rating = HYHG_rating.reset_index()
    print(HYHG_rating)

    Rating_List = HYHG_rating['letter_rating'].tolist()
    Weight_List = HYHG_rating['Rating_Weight_Diff'].tolist()
    factors = Rating_List
    x = Weight_List

    dot = figure(title="Fund vs Index by Rating bps (+)Fund Overweight / (-)Fund Underweight -) As Of: " + date, tools="hover", toolbar_location=None,
            y_range=factors, x_range=[min(Weight_List)*1.05, max(Weight_List)*1.05])
    dot.yaxis.axis_label_text_font_size = '20pt'
    dot.segment(0, factors, x, factors, line_width=2, line_color="green", )
    dot.circle(x, factors, size=15, fill_color="orange", line_color="green", line_width=3, )
    #p.yaxis.major_label_orientation =

    output_file("HYHG_rating.html", title="HYHG_rating_CS.py")
    show(row(dot, sizing_mode="scale_width"))  # open a browser
