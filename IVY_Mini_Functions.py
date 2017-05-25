import pandas as pd

def Ticker_Weights_Sum(index):
    Index_Weighter = index['Weight'].groupby(index['TICKER'])
    Index_Weights = pd.DataFrame(Index_Weighter.sum())
    Index_Weights = Index_Weights.rename(columns={'Weight':'Index_Weight'})

    Ticker_Weighter = index['PSA_Weights'].groupby(index['TICKER'])
    Ticker_Weights = pd.DataFrame(Ticker_Weighter.sum())
    Ticker_Weights = Ticker_Weights.rename(columns={'PSA_Weights':'Fund_Weight'})

    Ticker_Matrix = Index_Weights.join(Ticker_Weights)
    Ticker_Matrix = Ticker_Matrix.reset_index()
    Ticker_Matrix['Weight_Difference'] = (Ticker_Matrix['Fund_Weight'] - Ticker_Matrix['Index_Weight']) * 100

    return Ticker_Matrix