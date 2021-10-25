import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import string
import pytz
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
from common.scripts.spoof import Ip_Spoofer
import common.scripts.utils as utils

## get global vars
from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER_TARGET_DIR = sm_data_lake_dir+'/buffer/target-price-benzinga/'
DL_TARGET_DIR = sm_data_lake_dir+'/target-price-benzinga/{date}/'
UO_BENGINZA_URL = """https://api.benzinga.com/api/v1/signal/option_activity
?token={BENGINZA_TOKEN}
&parameters[date_from]={START}
&parameters[date_to]={END}
&pagesize=1000"""
UNUSUAL_OPTIONS_BUFFER = sm_data_lake_dir+'/buffer/unusual-options-benzinga/'

## functions
def clean(text):
    return (
        text
        .replace("'",'')
        .replace(',','')
        .replace('  ', ' ')
        .replace('&','and')
        .replace('__', '_')
        .replace('\n', '')
    )

def flip_sign(text):
    return '-'+text.strip('(').strip(')') if '(' in text else text

def percent(text):
    return float(text.strip('%'))/100 if '%' in text else text

def int_extend(column):
    int_text = ['K', 'M', 'B', 'T']
    int_scale = [1000, 1000000, 1000000000, 1000000000]
    for t, s in zip(int_text, int_scale):
        column = column.apply(lambda row: flip_sign(str(row)))
        column = column.apply(lambda row: int(float(str(row).replace(t, ''))*s) if t in str(row) else row)
        column = column.apply(lambda row: percent(str(row)))
        column = column.apply(lambda row: np.nan if row == '-' else row)
    return column

def clean_col(column):
    return (
        column
        .replace('/', '')
        .replace(' ', '_')
        .replace('(', '')
        .replace(')', '')
        .replace("'",'')
        .replace(',','')
        .replace('&','and')
        .replace('__', '_')
        .lower()
    )

def benzinga_target_price(ticker, print_url=False):
    try:
        url = 'https://www.benzinga.com/stock/{}/ratings'.format(ticker)
        spoofer_obj = Ip_Spoofer()
        page = spoofer_obj.request_page(url, print_url=print_url)
        table = page.find('table')
        cols = [clean_col(x.text) for x in table.find('thead').findAll('th')]
        df = pd.DataFrame()
        for tr in table.find('tbody').findAll('tr'):
            data = pd.DataFrame([[x.text for x in tr.findAll('td')]], 
                                columns=cols)
            df = df.append(data).reset_index(drop=True)    
        df = df.rename(columns={'date': 'analyst_rating_date'})
        df['analyst_rating_date'] = pd.to_datetime(df['analyst_rating_date'])
        df['pt'] = df['pt'].apply(lambda row: np.nan if row == '' else float(row))
        df['ticker'] = ticker
        df['date_collected'] = datetime.now().date()
        df.to_csv(BUFFER_TARGET_DIR+ticker+'.csv', index=False)
        return True
    except:
        return False ## no data
    
def price_target_collection(collect_threshold=.50):
    """
    Collect price targets from beninga.com.
    - Cycle through all tickers
    - Make requests until threshold is met
    - Store to local buffer
    - Coalesce to spark data frame
    - Partition by rating date
    """
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('benzing-price-target-collection') 
        .getOrCreate()
    )
    sc_tda = spark.sparkContext    
    ## set collection variables
    ticker_file = sm_data_lake_dir + '/seed-data/nasdaq_screener_1628807233734.csv'
    ticker_df = pd.read_csv(ticker_file)
    all_ticker_list = ticker_df['Symbol'].tolist()
    collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(BUFFER_TARGET_DIR+'*')]
    ticker_list = (
        list( 
            set(all_ticker_list) 
            - 
            set(collected_list)
        )
    )
    collect_percent = len(collected_list)/len(all_ticker_list) 

    count = 0
    while collect_percent < collect_threshold:
        _str = """
        COLLECTING PRICE DATA - LOOP {}
        \tTickers left to collect: {}
        \tTickers collected: {}
        \tPercent collected: {}
        \tSample tickers: {}
        """
        print(_str.format(count, len(ticker_list),
              len(collected_list), collect_percent, ticker_list[:5]))

        ## make requests
        yesterday_price_df_list = (
            sc_tda
            .parallelize(ticker_list)
            .map(benzinga_target_price)
            .collect()
        )

        ## calculate percent collected
        collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(BUFFER_TARGET_DIR+'*')]
        ticker_list = (
            list( 
                set(all_ticker_list) 
                - 
                set(collected_list)
            )
        )
        collect_percent = len(collected_list)/len(all_ticker_list) 
        collect_percent_og = collect_percent
        if collect_percent >= collect_threshold:
            continue ## no need to sleep
        ## iterat the counter and exit if too many
        count += 1
        if count > 25:
            collect_percent = 1
            continue ## no need to sleep

    ## Exit the loop and write
    _str = """
    FINAL COLLECTION STATS – {} Loops
    \tTickers collected: {}
    \tPercent collected: {}
    """
    print(_str.format(count-1, len(collected_list), collect_percent))    
    return True
    
def migrate_target_buffer():
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('benzing-migrate-ratings') 
        .getOrCreate()
    )
    ## get everything in buffer
    collected_df = (
        spark.read.format('csv')
        .options(header='true')
        .load(BUFFER_TARGET_DIR+'*')
        .toPandas()
    )
    ## format
    collected_df['analyst_rating_date'] = (
        pd.to_datetime(collected_df['analyst_rating_date'])
        .apply(lambda r: r.date())
    )
    collected_df['pt'] = collected_df['pt'].astype(float)
    for c in ['research_firm', 'action', 'current', 'ticker']:
        collected_df[c] = collected_df[c].astype(str)
    ## get previously collected
    dl_df = (
        spark.read.format('orc')
        .options(header='true')
        .load(DL_TARGET_DIR.format(date='*'))
        .toPandas()
    )
    ## format
    dl_df['analyst_rating_date'] = (
        pd.to_datetime(dl_df['analyst_rating_date'])
        .apply(lambda r: r.date())
    )
    dl_df['pt'] = dl_df['pt'].astype(float)
    for c in ['research_firm', 'action', 'current', 'ticker']:
        dl_df[c] = dl_df[c].astype(str)

    join_cols = list(dl_df.drop('date_collected', 1).columns)
    dl_df['previously_collect'] = True
    ## find new data
    collected_df = (
        collected_df
        .merge(dl_df.drop('date_collected', 1), 
               how='left', on=join_cols)
        .fillna(False)
    )
    collected_df = (
        collected_df
        .loc[collected_df['previously_collect'] == False]
        .drop('previously_collect', 1)
    )
    date_list = sorted(collected_df['analyst_rating_date'].unique())
    dl_df_subset = (
        dl_df
        .loc[dl_df['analyst_rating_date'].isin(date_list)]
    )
    del dl_df
    for date in date_list:
        tmp_dl_df = (
            dl_df_subset
            .loc[dl_df_subset['analyst_rating_date'] == date]
            .drop('previously_collect', 1)
        )
        tmp_df = (
            tmp_dl_df
            .append(
                collected_df
                .loc[collected_df['analyst_rating_date'] == date]
            )
            .reset_index(drop=True)
        )
        ## format bc of append
        tmp_df['analyst_rating_date'] = (
            pd.to_datetime(tmp_df['analyst_rating_date'])
            .apply(lambda r: r.date())
        )
        tmp_df['pt'] = tmp_df['pt'].astype(float)
        for c in ['research_firm', 'action', 'current', 'ticker']:
            tmp_df[c] = tmp_df[c].astype(str)
        _ = utils.write_spark(spark, tmp_df, 'target-price-benzinga',date)
    return True 

def get_unusual_options(date):
    """
    Use benzinga's API to get unusual options data
    Arg
        date: Collection date.
    Return
        df: Dataframe
    """
    ## convert datetime string and localize
    dt = (
        pd.to_datetime(date)
        .tz_convert('America/New_York')
    )
    start = dt.date()
    ## prevent running during odd hours
    collect = True
    if dt.day_of_week > 4:
        collect = False ## do not collect weekend
    if dt.hour < 9:
        collect = False ## do not collect too early
    if dt.hour > 18:
        collect = False ## do not collect too late
    ## write empty file
    if collect == False:
        _ = (
            pd.DataFrame()
            .to_csv(UNUSUAL_OPTIONS_BUFFER+'{}.csv'.format(start), index=False)
        )
    ## format url
    end = start + timedelta(days=1)
    url = (
        UO_BENGINZA_URL
        .format(BENGINZA_TOKEN=Variable.get('BENGINZA_TOKEN'),
                START=start, END=end)
        .replace('\n', '')
    )
    ## make request
    spoof = Ip_Spoofer()
    page = Ip_Spoofer.request_page(spoof, url)
    ## unpack data
    items_list = page.findAll('item')
    result_list = []
    for item in items_list:
        item_content = item.contents[1::2]
        data = dict()
        _ = [data.update({x.name: x.text}) for x in item_content]
        _ = result_list.append(data)
    ## save to buffer for spark to migrate
    _ = (
        pd.DataFrame(result_list)
        .to_csv(UNUSUAL_OPTIONS_BUFFER+'{}.csv'.format(start), index=False)
    )
    return True

def migrate_options_buffer():
    ## get date from buffer file
    date = (
        glob
        .glob(UNUSUAL_OPTIONS_BUFFER+'*')[0]
        .split('/')[-1]
        .split('.csv')[0]
    )
    ## get data
    file = glob.glob(UNUSUAL_OPTIONS_BUFFER+'*')[0]
    try:
        collected_df = pd.read_csv(file)
    except ValueError as e: 
        if e == 'No columns to parse from file':
            return False
        else:
            return e
    if len(collected_df) == 0:
        return False
    ## little formatting
    date_cols = ['date_expiration', 'date']
    for col in date_cols:
        collected_df[col] = (
            pd.to_datetime(collected_df[col])
            .apply(lambda r: r.date())
        )

    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('benzing-migrate-unusual-options') 
        .getOrCreate()
    )    
    ## write to data lake
    _ = utils.write_spark(spark, collected_df, 'unusual-options-benzinga', date)
    return True