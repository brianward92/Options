import pandas as pd

def load_data(drop_dups = False, mrg_liq = True):
    '''
    This function loads a dataset for downstream analysis.
    
    :param bool drop_dups: whether or not to drop duplicates
    :param bool mrg_liq: whether or not to merge liquidity
    '''
    
    ids_to_drop = {31622275}

    df = pd.read_csv('./dat/spx_option_prices.csv')
    df = df[['date','exdate','strike_price','best_bid','best_offer','volume','open_interest','optionid']]
    df = df[~df['optionid'].isin(ids_to_drop)]
    df.rename({'best_offer':'best_ask'}, axis = 1, inplace = True)

    df['exdate'] = pd.to_datetime(df['exdate'].astype(str))
    df['date'] = pd.to_datetime(df['date'].astype(str))

    df['daysToExpiry'] = ((df['exdate'] - df['date']).astype(int)/1e9/3600/24).astype(int)

    df_lt = df.groupby('optionid')['daysToExpiry'].max().reset_index().rename(
        {'daysToExpiry':'lifeTime'},axis=1)
    df = df.merge(df_lt, how = 'left', on = 'optionid', validate = 'm:1')

    slct = ['date','exdate','strike_price','lifeTime']
    calc = ['volume','open_interest','best_bid','best_ask','daysToExpiry']
    df_ct = df.groupby(slct)['volume']
    df_ct = df_ct.count().reset_index().rename({'volume':'ct'},axis=1)

    if drop_dups:
        df = df.merge(df_ct, how = 'left', on = slct, validate = 'm:1')
        df = df[df['ct'] == 1].drop('ct', axis=1)
    else:
        if mrg_liq:
            field_map = {'volume':'sum','open_interest':'sum','best_bid':'max','best_ask':'min'}
            df = df.groupby(slct + ['daysToExpiry']).agg(field_map).reset_index()
        #else: # Then pass and keep the data. (Ordered different than the documentation)

    return df[slct + calc]