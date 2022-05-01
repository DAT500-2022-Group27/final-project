import pandas as pd

# ASSUMES all_ticks_gen IS ONLY TICKS FOR ONE period/PERIOD AND ONE TICKER
def summarize_ticks(all_ticks, period='1h'):
    # split ticks into two lists, one for TRADE lines and one for BIDASK lines
    trades = []
    bidask = []
    for line in all_ticks:
        if len(line[0]) > 1:
            if line[1] == 'TRADES':
                trades.append(line)
            elif line[1] == 'BIDASK':
                bidask.append(line)
    
    # check both types of ticks exist in period... otherwise can be a low volume stock or outside of market hours
    if len(trades) > 0 and len(bidask) > 0: 
        df_trades = get_trades_df(trades)
        df_bidask = get_bidask_df(bidask)

        # df_rs should only ever be 1 record because the time period set here must correspond to the key emitted from the mapper
        df_rs = summarize_fields(
            df_trades[['Price', 'Size']], 
            field_types=['price', 'quantity'], 
            period=period)
        df_rs = df_rs.join(summarize_fields(
            df_bidask[['PriceBid', 'PriceAsk', 'SizeBid', 'SizeAsk']], 
            field_types=['price', 'price', 'quantity', 'quantity'], 
            period=period), how='inner')
        df_rs = df_rs.astype('object').reset_index().iloc[0]
        py_date = df_rs['Time'].to_pydatetime()
        df_rs = df_rs.drop('Time')

        return [py_date] + list(df_rs)
    else:
        return None


# calculate summary statistics on ALL columns of a DateTime indexed DataFrame, df_fields,
# with the type of field ('price' or 'quantity') determining which stats are returned
def summarize_fields(df_fields, field_types=[], period='1h'):

    if len(df_fields.columns) != len(field_types):
        return None

    idx = pd.DataFrame(index=df_fields.index).resample(period).last().index
    df_rs = pd.DataFrame(index=idx)
    for i, field in enumerate(df_fields.columns):
        if field_types[i] == 'price':
            df_ohlc = df_fields[field].resample(period).ohlc()
            df_ohlc.columns = field + '_' + df_ohlc.columns
            df_rs = df_rs.join(df_ohlc, how='inner')
            df_rs[field + '_mean'] = df_fields[field].resample(period).mean()        
            df_rs[field + '_median'] = df_fields[field].resample(period).median() 
            df_rs[field + '_sd'] = df_fields[field].resample(period).std() 
        elif field_types[i] == 'quantity':
            df_ohlc = df_fields[field].resample(period).ohlc()
            df_ohlc.columns = field + '_' + df_ohlc.columns
            df_rs = df_rs.join(df_ohlc, how='inner')
            df_rs[field + '_sum'] = df_fields[field].resample(period).sum()
            df_rs[field + '_count'] = df_fields[field].resample(period).count()
            df_rs[field + '_mean'] = df_fields[field].resample(period).mean()        
            df_rs[field + '_median'] = df_fields[field].resample(period).median()
            df_rs[field + '_sd'] = df_fields[field].resample(period).std()
        else:
            return None

    return df_rs if len(df_rs) > 0 else None

def get_trades_df(lst_trades):
    df_trades = pd.DataFrame(lst_trades)
    df_trades.rename(columns={0:'Ticker', 1:'LineType', 2:'Time', 3:'TimeStamp', 4:'TickAttribLast', 5:'Price', 6:'Size', 7:'Exchange', 8:'SpecialConditions'}, inplace=True)
    time_idx = pd.DatetimeIndex(pd.to_datetime(df_trades['Time'], utc=True)).tz_convert('Europe/Oslo')
    df_trades.set_index(time_idx, inplace=True, drop=True)
    df_trades.drop('Time', axis=1, inplace=True)
    df_trades['Price'] = pd.to_numeric(df_trades['Price'])
    df_trades['Size'] = pd.to_numeric(df_trades['Size'])
    return df_trades

def get_bidask_df(lst_bidask):
    df_bid_ask = pd.DataFrame(lst_bidask)
    df_bid_ask.rename(columns={0:'Ticker', 1:'LineType', 2:'Time', 3:'TimeStamp', 4:'TickAttribBidAsk', 5:'PriceBid', 6:'PriceAsk', 7:'SizeBid', 8:'SizeAsk'}, inplace=True)
    time_idx = pd.DatetimeIndex(pd.to_datetime(df_bid_ask['Time'], utc=True)).tz_convert('Europe/Oslo')
    df_bid_ask.set_index(time_idx, inplace=True)
    df_bid_ask.drop('Time', axis=1, inplace=True)
    df_bid_ask['PriceBid'] = pd.to_numeric(df_bid_ask['PriceBid'])
    df_bid_ask['PriceAsk'] = pd.to_numeric(df_bid_ask['PriceAsk'])
    df_bid_ask['SizeBid'] = pd.to_numeric(df_bid_ask['SizeBid'])
    df_bid_ask['SizeAsk'] = pd.to_numeric(df_bid_ask['SizeAsk'])
    return df_bid_ask
