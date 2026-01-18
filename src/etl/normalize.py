def compute_adj_from_yahoo(df_prices, df_yahoo):
    # df_yahoo must have 'Date' and 'Adj Close' and 'Close' — compute factor and apply
    merged = df_prices.merge(df_yahoo[['Date','Adj Close','Close']], left_on='trade_date', right_on='Date', how='left')
    merged['factor'] = merged['Adj Close'] / merged['Close']
    for col in ('open','high','low','close'):
        merged['adj_' + col] = merged[col] * merged['factor']
    merged['adj_close'] = merged['adj_close'] = merged['adj_close'].fillna(merged['close'])
    return merged