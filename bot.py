import json, re, requests
from typing import Type
import pandas as pd
from datetime import datetime
import time

def cbpGetHistoricRates(market='BTC-GBP', granularity=86400, iso8601start='', iso8601end=''):
    #Retrieves historical rates from Coinbase Pro API and stores it as a Pandas DataFrame

    p = re.compile(r"^[A-Z]{3,4}\-[A-Z]{3,4}$")
    if not p.match(market):
        raise TypeError('Coinbase Pro market required')
    if not isinstance(granularity, int):
        raise TypeError('Granularity integer required.')
    if not granularity in [60, 300, 900,  3600, 21600, 86400]:
        raise TypeError('Granularity options: 60, 300, 900, 3600, 21600, 86400')
    if not isinstance(iso8601start, str):
        raise TypeError('ISO8601 start integer as string required')
    if not isinstance(iso8601end, str):
        raise TypeError('ISO8601 end integer as string required')

    api = 'https://api.pro.coinbase.com/products/' + market + '/candles?granularity=' + \
        str(granularity) + '&start=' + iso8601start + '&end=' + iso8601end
    resp = requests.get(api)
    if resp.status_code != 200:
        raise Exception('GET ' + api + ' {}'.format(resp.status_code))

    df = pd.DataFrame(resp.json(), columns = [ 'iso8601', 'low', 'high', 'open', 'close', 'volume' ])
    df = df.iloc[::-1].reset_index()


    for index, row in df.iterrows():
        iso8601 = datetime.fromtimestamp(row['iso8601'])
        df.at[index, 'datetime'] = pd.to_datetime(iso8601, format="%Y-%m-%d %H:%M:%S")

    df = df.reindex(columns = [ 'iso8601', 'datetime', 'low', 'high', 'open', 'close', 'volume' ])

    print(df)

    return df

def addMovingAverages(df=pd.DataFrame()):
    #Appends CMA, EMA12, EMA26, SMA20, SMA50, and SMA200 moving averages to a dataframe

    if not isinstance(df, pd.DataFrame):
        raise TypeError('Pandas DataFrame required')
    if not 'close' in df.columns:
        raise AttributeError("Pandas DataFrame 'close' column required")
    if not df['close'].dtype == 'float64' and not df['close'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'close' column not int64 or float64")

    #calculate cumulative moving average
    df['cma'] = df.close.expanding().mean()

    #calculate exponential moving averages
    df['ema12'] = df.close.ewm(span=12, adjust=False).mean()
    df['ema26'] = df.close.ewm(span=26, adjust=False).mean()

    #calculate simple moving averages
    df['sma20'] = df.close.rolling(20, min_periods=1).mean()
    df['sma50'] = df.close.rolling(50, min_periods=1).mean()
    df['sma200'] = df.close.rolling(200, min_periods=1).mean()

    return df

def calculateRelativeStrengthIndex(series, interval=14):
    #calculate the RSI on a Pandas series of closing prices

    if not isinstance(series, pd.Series):
        raise TypeError('Pandas Series Required')
    if not isinstance(interval, int):
        raise TypeError('Interval integer required')
    if(len(series) < interval):
        raise IndexError('Pandas Series smaller than interval')
    
    diff = series.diff(1).dropna()
    
    sum_gains = 0 * diff
    sum_gains[diff > 0] = diff[diff > 0]
    avg_gains = sum_gains.ewm(com=interval-1, min_periods=interval).mean()

    sum_losses = 0 * diff
    sum_losses[diff < 0] = diff[diff < 0]
    avg_losses = sum_losses.ewm(com=interval-1, min_periods=interval).mean()

    rs = abs(avg_gains / avg_losses)
    rsi = 100 - 100 / (1 + rs)

    return rsi

def addMomentumIndicators(df=pd.DataFrame()):
    # Appends RSI14 and MACD momentum indicators to a dataframe

    if not isinstance(df, pd.DataFrame):
        raise TypeError('Pandas DataFrame required')
    if not 'close' in df.columns:
        raise AttributeError("Pandas DataFrame 'close' column required")
    if not 'ema12' in df.columns:
        raise AttributeError("Pandas DataFrame 'ema12' column required")
    if not 'ema26' in df.columns:
        raise AttributeError("Pandas DataFrame 'ema26' column required")
    if not df['close'].dtype == 'float64' and not df['close'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'close' column not int64 or float64")
    if not df['ema12'].dtype == 'float64' and not df['ema12'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'ema12' column not int64 or float64")
    if not df['ema26'].dtype == 'float64' and not df['ema26'].dtype == 'int64':
        raise AttributeError("Pandas DataFrame 'ema26' column not int64 or float64")

    #calculatae relative strength index

    df['rsi14'] = calculateRelativeStrengthIndex(df['close'], 14)
    df['rsi14'] = df['rsi14'].fillna(50) #default to midway-50 for first entries

    #calculate moving average convergence divergence
    df['macd'] = df['ema12'] - df['ema26']
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    return df

def saveHistoricRates(filename='cbpGetHistoricRates.csv', df=pd.DataFrame()):
    #save the dataFrame to an uncompressed CSV

    p = re.compile(r"^[\w\-. ]+$")
    if not p.match(filename):
        raise TypeError("Filename required")
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Pandas DataFrame required')
    try:
        df.to_csv(filename)
    except OSError:
        print('Unable to save: ', filename)

    return df

def addHistoricRatesToFile(filename='cbpGetHistoricRates.csv', df=pd.DataFrame()):
    #add more data to csv file
    p = re.compile(r"^[\w\-. ]+$")
    if not p.match(filename):
        raise TypeError("Filename required")
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Pandas DataFrame required')
    try:
        df.to_csv(filename, mode='a', header=False)
    except OSError:
        print('Unable to save: ', filename)

    return df
