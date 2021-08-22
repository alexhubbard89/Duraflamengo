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

## get global vars
from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER_DIR = sm_data_lake_dir+'/buffer/target-price-benzinga/'
DL_DIR = sm_data_lake_dir+'/target-price-benzinga/{date}/'

def write_spark(spark, df, date):
    try:
        df_sp = spark.createDataFrame(df)
    except:
        print('except')
        df_sp = df ## already in spark
    file_path = DL_DIR.format(date=str(date))
    _ = (
        df_sp
        .write
        .format("orc")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(file_path)
    )
    return True

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
        df.to_csv(BUFFER_DIR+ticker+'.csv', index=False)
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
    collected_list = glob.glob(BUFFER_DIR+'*')
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
        collected_list = glob.glob(BUFFER_DIR+'*')
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
    
def migrate_buffer():
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('benzing-migrate') 
        .getOrCreate()
    )
    ## get everything in buffer
    collected_df = (
        spark.read.format('csv')
        .options(header='true')
        .load(BUFFER_DIR+'*')
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
        .load(DL_DIR.format(date='*'))
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
        _ = write_spark(spark, tmp_df, date)
    return True 

def clear_buffer():
    print('clear buffer')
    shutil.rmtree(BUFFER_DIR)
    os.mkdir(BUFFER_DIR)
    return True