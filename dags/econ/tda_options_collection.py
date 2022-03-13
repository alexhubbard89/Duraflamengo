import pandas as pd
import numpy as np
import datetime as dt
import pytz
import string
import requests
import time
import glob
import shutil
import os

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

## Local code
import common.scripts.utils as utils

sm_data_lake_dir = '/Users/alexanderhubbard/stock-market/data'
SINGLE_WRITE_BUFFER = sm_data_lake_dir+'/buffer/tda-options/single/'
ANALYTICAL_WRITE_BUFFER = sm_data_lake_dir+'/buffer/tda-options/analytical/'
DL_WRITE_DIR = sm_data_lake_dir+'/{subdir}/{date}/'

CHAIN = 'https://api.tdameritrade.com/v1/marketdata/chains?apikey={api}&symbol={ticker}&contractType=ALL&range=ALL&strategy={strategy}'


## Make type for writes
DATA_TYPES = {
    'putCall': str,
    'symbol': str,
    'description': str,
    'exchangeName': str,
    'bid': float,
    'ask': float,
    'last': float,
    'mark': float,
    'bidSize': int,
    'askSize': int,
    'bidAskSize': str,
    'lastSize': int,
    'highPrice': float,
    'lowPrice': float,
    'openPrice': float,
    'closePrice': float,
    'totalVolume': int,
    'tradeDate': pd.Timestamp,
    'tradeTimeInLong': int,
    'quoteTimeInLong': int,
    'netChange': float,
    'volatility': float,
    'delta': float,
    'gamma': float,
    'theta': float,
    'vega': float,
    'rho': float,
    'openInterest': int,
    'timeValue': float,
    'theoreticalOptionValue': float,
    'theoreticalVolatility': float,
    'optionDeliverablesList': str,
    'strikePrice': float,
    'expirationDate': int,
    'daysToExpiration': int,
    'expirationType': str,
    'lastTradingDay': int,
    'multiplier': float,
    'settlementType': str,
    'deliverableNote': str,
    'isIndexOption': str,
    'percentChange': float,
    'markChange': float,
    'markPercentChange': float,
    'intrinsicValue': float,
    'inTheMoney': bool,
    'pennyPilot': bool,
    'nonStandard': bool,
    'mini': bool,
    'expDate': dt.date,
    'ticker': str,
    'strategy': str,
    'interval': float,
    'isDelayed': bool,
    'interestRate': float,
    'underlyingPrice': float,
    'daysExp': int,
    'collection_date': dt.date
}

###################
##### Methods #####
###################

def format_data(df):
    for col in DATA_TYPES:
        if DATA_TYPES[col] == pd.Timestamp:
            df[col] = pd.to_datetime(df[col])
        elif DATA_TYPES[col] == dt.date:
            df[col] = pd.to_datetime(df[col]).apply(lambda r: r.date())
        else:
            df[col] = df[col].astype(DATA_TYPES[col])
    return df

def clean_general(df):
    ## get expiration and days until
    a = df['expDate'].apply(lambda r: r.split(':'))
    df['expDate'] = a.apply(lambda r: pd.to_datetime(r[0]).date())
    df['daysExp'] = a.apply(lambda r: r[1])
    df = df.rename(columns={'symbol': 'ticker'})
    
    cols = [
        'expDate', 
        'ticker', 
        'strategy', 
        'interval',
        'isDelayed', 
        'interestRate', 
        'underlyingPrice', 
        'daysExp' ## duplicate but ok for now
    ]

    full_chain_df = pd.DataFrame()
    for i in df.index[:2]:
        r_calls = df.loc[i, 'callExpDateMap']
        tmp_calls = pd.concat([pd.DataFrame(r_calls[x]) for x in r_calls]).reset_index(drop=True)
        r_put = df.loc[i, 'putExpDateMap']
        tmp_puts = pd.concat([pd.DataFrame(r_put[x]) for x in r_put]).reset_index(drop=True)

        for col in cols:
            v = df.loc[i, col]
            tmp_calls[col] = v
            tmp_puts[col] = v

        full_chain_df = (
            full_chain_df
            .append(tmp_calls)
            .append(tmp_puts)
            .reset_index(drop=True)
        )
        
    ## get collection date
    TODAY = (
        dt.datetime
        .now(pytz.timezone('America/New_York'))
        .date() 
    )
    full_chain_df['collection_date'] = TODAY
    ## make tyupes
    typed_data = format_data(full_chain_df)
        
    return typed_data

def get_single(ticker):
    ## request
    url = (
        CHAIN
        .format(ticker=ticker.upper(),
                api=os.environ['TDA_API_KEY'],
                strategy='SINGLE'
               )
    )
    r = requests.get(url=url)
    if r.content == b'':
        ## if no data then send it away
        _ = (
            df
            .to_csv(SINGLE_WRITE_BUFFER+'{}.csv'.format(ticker), 
                    index=False)
        )
        return True
    ## there is data
    _json = r.json()
    if 'error' in _json.keys():
        ## I've hit the rate limit
        return False
    ## unpack
    df = (
        pd.DataFrame(_json)
        .reset_index()
        .rename(columns={'index': 'expDate'})
    )
    if len(df) == 0:
        ## if no data then send it away
        _ = (
            df
            .to_csv(SINGLE_WRITE_BUFFER+'{}.csv'.format(ticker), 
                    index=False)
        )
        return True
    ## clean
    try:
        ## error happening:
        ## TypeError: 'float' object is not iterable
        ## tmp_calls = pd.concat([pd.DataFrame(r_calls[x]) for x in r_calls]).reset_index(drop=True)
        ## File "/Users/alexanderhubbard/projects/Duraflamengo/dags/econ/tda_options_collection.py", line 128
        chain_df = clean_general(df)    
        _ = (
            chain_df
            .to_csv(SINGLE_WRITE_BUFFER+'{}.csv'.format(ticker), 
                    index=False)
        )
        return True
    except:
        return False

def get_analytical(ticker):
    ## request
    url = (
        CHAIN
        .format(ticker=ticker.upper(),
                api=os.environ['TDA_API_KEY'],
                strategy='ANALYTICAL'
               )
    )    
    r = requests.get(url=url)
    if r.content == b'':
        ## if no data then send it away
        _ = (
            df
            .to_csv(SINGLE_WRITE_BUFFER+'{}.csv'.format(ticker), 
                    index=False)
        )
        return True
    ## there is data
    _json = r.json()
    if 'error' in _json.keys():
        ## I've hit the rate limit
        return False
    ## unpack
    df = (
        pd.DataFrame(_json)
        .reset_index()
        .rename(columns={'index': 'expDate'})
    )
    if len(df) == 0:
        ## if no data then send it away
        _ = (
            df
            .to_csv(SINGLE_WRITE_BUFFER+'{}.csv'.format(ticker), 
                    index=False)
        )
        return True
    ## clean
    chain_df = clean_general(df)
    _ = (
        chain_df
        .to_csv(ANALYTICAL_WRITE_BUFFER+'{}.csv'.format(ticker), 
                index=False)
    )
    return True

def pipeline(collect_threshold=.85, loop_collect=240, strategy='single'):
    """
    https://developer.tdameritrade.com/content/authentication-faq
    Q: Are requests to the Post Access Token API throttled?
        All non-order based requests by personal use non-commercial 
        applications are throttled to 120 per minute.
    """
    ## only collect form 8am to 7pm ET.
    now = dt.datetime.now(pytz.timezone('America/New_York'))
    now_time = str(now.time()).split('.')[0]
    if utils.is_between(now_time, ("08:00", "19:00")) == False:
        print('\n\n\nDO NOT COLLECT\n\n\n')
        ## DO NOT COLLECT
        ## Its currently outside of defined collection hours
        return False
    
    ## check what type of pipeline to fire off
    if strategy.lower() == 'single':
        TMP_WRITE_BUFFER = SINGLE_WRITE_BUFFER
        collection_fuction = get_single
    elif strategy.lower() == 'analytical':
        TMP_WRITE_BUFFER = ANALYTICAL_WRITE_BUFFER
        collection_fuction = get_analytical
    else:
        raise ValueError('The strategy you requested is not an option.')
    
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('daily-tda-price-collect') 
        .getOrCreate()
    )
    sc_tda = spark.sparkContext    
    
    ## set collection variables
    ticker_file = sm_data_lake_dir + '/seed-data/nasdaq_screener_1628807233734.csv'
    ticker_df = pd.read_csv(ticker_file)
    ## remove tickers with forward slash. They break stuff
    ticker_df = (
        ticker_df
        .loc[ticker_df['Symbol'].str.contains('/') == False]
    )
    all_ticker_list = ticker_df['Symbol'].tolist()
    collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(TMP_WRITE_BUFFER+'*')]
    tickers_left = (
        list( 
            set(all_ticker_list) 
            - 
            set(collected_list)
        )
    )
    if len(tickers_left) < loop_collect:
        ticker_list = tickers_left ## prevents exception
    else:
        ticker_list = list(np.random.choice(tickers_left, loop_collect, replace=False))
    collect_percent = len(collected_list)/len(all_ticker_list) 
    collect_percent_og = collect_percent
    
    count = 0
    while collect_percent < collect_threshold:
        start_loop = dt.datetime.now()
        _str = """
        COLLECTING OPTIONS DATA – STRATEGY: {} 
        \tLoop number: {}
        \tTickers left to collect: {}
        \tAttemptint to collect: {}
        \tTickers collected: {}
        \tPercent collected: {}
        \tSample tickers: {}
        """
        print(_str.format(strategy, count, len(tickers_left), len(ticker_list),
              len(collected_list), collect_percent, ticker_list[:5]))

        ## make requests
        option_chain_collection = (
            sc_tda
            .parallelize(ticker_list)
            .map(lambda t: collection_fuction(t))
            .collect()
        )

        ## calculate percent collected
        collected_list = glob.glob(TMP_WRITE_BUFFER+'*')
        tickers_left = (
            list( 
                set(all_ticker_list) 
                - 
                set(collected_list)
            )
        )
        if len(tickers_left) < loop_collect:
            ticker_list = tickers_left ## prevents exception
        else:
            ticker_list = list(np.random.choice(tickers_left, loop_collect, replace=False))
        collect_percent = len(collected_list)/len(all_ticker_list) 
        collect_percent_og = collect_percent
        if collect_percent >= collect_threshold:
            continue ## no need to sleep
        ## iterat the counter and exit if too many
        count += 1
        if count > 200:
            collect_percent = 1
            continue ## no need to sleep
        ## Avoid throttle and sleep if needed
        loop_time = (dt.datetime.now() - start_loop).seconds
        sleep_time = (60 - loop_time)
        if sleep_time > 0:
            time.sleep(sleep_time)

    ## Exit the loop and write
    _str = """
    FINAL COLLECTION STATS – {} Loops
    \tTickers collected: {}
    \tPercent collected: {}
    """
    print(_str.format(count, len(collected_list), collect_percent_og))    
    
def migrate(strategy='single'):
    ## Only migrate during collection times
    now = dt.datetime.now(pytz.timezone('America/New_York'))
    now_time = str(now.time()).split('.')[0]
    if utils.is_between(now_time, ("08:00", "19:00")) == False:
        print('\n\n\nDO NOT COLLECT\n\n\n')
        ## DO NOT COLLECT
        ## Its currently outside of defined collection hours
        return False

    ## check what type of data to migrate
    if strategy.lower() == 'single':
        subdir = 'tda-options-single'
        TMP_BUFFER = SINGLE_WRITE_BUFFER
    elif strategy.lower() == 'analytical':
        subdir = 'tda-options-analytical'
        TMP_BUFFER = ANALYTICAL_WRITE_BUFFER
    else:
        raise ValueError('The strategy you requested is not an option.')
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('migrate-tda-options') 
        .getOrCreate()
    )
    ## get data to migrate
    collected_options_df = (
        spark.read.format('csv')
        .options(header='true')
        .load(TMP_BUFFER + '*')
    )
    today = (
        dt.datetime
        .now(pytz.timezone('America/New_York'))
        .date()
    )
    _ = (
        utils
        .write_spark(spark, collected_options_df, 
                     subdir, today)
    )
    return True

def clear_buffer(strategy='single'):
    """"
    Custom buffer clear.
    Check if it is currently withing defined collection time.
    If it is, do not clear.
    If it is outside of defined hours, clear buffer.
    """
    ## check what type of data to migrate
    if strategy.lower() == 'single':
        subdir = 'tda-options/single'
    elif strategy.lower() == 'analytical':
        subdir = 'tda-options/analytical'
    else:
        raise ValueError('The strategy you requested is not an option.')
    
    now = dt.datetime.now(pytz.timezone('America/New_York'))
    now_time = str(now.time()).split('.')[0]
    if utils.is_between(now_time, ("08:00", "19:00")) == True:
        print('\n\n\nDO NOT CLEAR BUFFER\n\n\n')
        ## DO NOT CLEAR BUFFER
        ## Bc its currently market hours
        return False
    utils.clear_buffer(subdir=subdir)
    return True