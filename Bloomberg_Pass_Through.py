from Bloomberg_Pandas import BLPInterface
import pandas as pd

def Bloomberg_Data(CUSIPs):
    blp = BLPInterface()
    blp_CUSIPs = pd.DataFrame(CUSIPs, columns=['CUSIP'])
    blp_CUSIPs['blp_CUSIP'] = '/CUSIP/' + blp_CUSIPs['CUSIP'] + '@TRAC'

    blp_Bloomberg_IDs = pd.DataFrame(blp.referenceRequest(blp_CUSIPs['blp_CUSIP'], \
        ['GICS_SECTOR_NAME', 'GICS_INDUSTRY_GROUP_NAME', \
         'VOLUME_AVG_30D', 'VOLUME_AVG_5D', 'PREVIOUS_TOTAL_VOLUME']))

    blp_Bloomberg_IDs = pd.DataFrame(blp_Bloomberg_IDs.to_records())
    blp_Bloomberg_IDs = blp_Bloomberg_IDs.rename(columns={'Security': 'CUSIP'})
    blp_CUSIPs = blp_Bloomberg_IDs
    blp_CUSIPs['CUSIP'] = blp_Bloomberg_IDs['CUSIP'].str[7:16]

    return blp_CUSIPs

def Bloomberg_First_Pass(CUSIPs):
    blp = BLPInterface()
    blp_CUSIPer = pd.DataFrame(CUSIPs, columns=['CUSIP'])
    blp_CUSIPer['blp_CUSIP'] = '/CUSIP/' + blp_CUSIPer['CUSIP'] + '@TRAC'
    blp_First_Passer = pd.DataFrame(blp.referenceRequest(blp_CUSIPer['blp_CUSIP'], \
        ['TICKER', 'MIN_PIECE', 'MIN_INCREMENT', 'CPN_TYP']))

    blp_First_Pass = pd.DataFrame(blp_First_Passer.to_records())
    blp_First_Pass = blp_First_Pass.rename(columns={'Security': 'CUSIP'})
    blp_CUSIPer = blp_First_Pass
    blp_CUSIPer['CUSIP'] = blp_CUSIPer['CUSIP'].str[7:16]

    return blp_CUSIPer