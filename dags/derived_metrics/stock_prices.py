import pandas as pd
import numpy as np
import datetime as dt
import string
import pytz
import glob
import shutil, os

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

## Local code
import common.scripts.utils as utils

## locations
from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
PRICE_DIR = sm_data_lake_dir+'/tda-daily-price/date'
STOCK_AVG_BUFFER = sm_data_lake_dir+'/buffer/der-m-avg-price'

def prepare_pv() -> bool:
    """
    Use spark to read and filter needed data.
    Write to buffer.
    
    Since the data lake is so large I'm hitting 
    memory issues. This process finds the dates
    necessary for analysis, copies the contents
    to a buffer, prepares the data, then clears
    the migrated buffer.
    
    Inputs:
        - Date is the last day of the analysis
        - Days is the lookback window
    """
    ## load and migrate all data
    spark = (
        SparkSession
        .builder 
        .appName('prepare-price-vol-avg') 
        .getOrCreate()
    )
    schema = (
        T.StructType(
            [
                T.StructField('open', T.FloatType(), True),
                T.StructField('high', T.FloatType(), True),
                T.StructField('low', T.FloatType(), True),
                T.StructField('close', T.FloatType(), True),
                T.StructField('volume', T.IntegerType(), True),
                T.StructField('date', T.DateType(), True),
                T.StructField('ticker', T.StringType(), True)
            ]
        )
    )
    price_df = (
        spark
        .read
        .format("orc")
        .option("path", STOCK_AVG_BUFFER+'/raw/*')
        .schema(schema)
        .load()
        .orderBy(['ticker', 'date'])
        .repartition(1) ## do not partition
        .write
        .format("csv")
        .mode("overwrite")
        .save(STOCK_AVG_BUFFER + '/prepared/', header=True)
    )
    _ = spark.stop()
    return True

def make_pv() -> bool:
    """
    Make and write dialy price and volume
    averages to the data lake.
    Lookback window is hardcoded to reflect
    typical averaging.
    
    """
    ## load data
    price_df = utils.read_many_csv(STOCK_AVG_BUFFER + '/prepared/')
    price_df['date'] = (
        pd.to_datetime(price_df['date'])
        .apply(lambda r: r.date())
    )
    ## fill missing close prices
    price_df['close'] = price_df['close'].fillna(method='ffill')
    ## make avg price
    price_avg_5 = (
        price_df
        .groupby('ticker')
        ['close']
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'close': 'avg_price_5'})
        .set_index('index')
        .drop('ticker', 1)
    )
    price_avg_10 = (
        price_df
        .groupby('ticker')
        ['close']
        .rolling(10)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'close': 'avg_price_10'})
        .set_index('index')
        .drop('ticker', 1)
    )
    price_avg_50 = (
        price_df
        .groupby('ticker')
        ['close']
        .rolling(50)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'close': 'avg_price_50'})
        .set_index('index')
        .drop('ticker', 1)
    )
    price_avg_200 = (
        price_df
        .groupby('ticker')
        ['close']
        .rolling(200, min_periods=1)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'close': 'avg_price_200'})
        .set_index('index')
        .drop('ticker', 1)
    )
    ## make avg vol
    volume_avg_5 = (
        price_df
        .groupby('ticker')
        ['volume']
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'volume': 'avg_volume_5'})
        .set_index('index')
        .drop('ticker', 1)
    )
    volume_avg_10 = (
        price_df
        .groupby('ticker')
        ['volume']
        .rolling(10)
        .mean()
        .reset_index()
        .rename(columns={'level_1': 'index', 'volume': 'avg_volume_10'})
        .set_index('index')
        .drop('ticker', 1)
    )
    ## join and subset columns
    price_df_full = (
        price_df
        .join(price_avg_5)
        .join(price_avg_10)
        .join(price_avg_50)
        .join(price_avg_200)
        .join(volume_avg_5)
        .join(volume_avg_10)
        [['ticker', 'date', 'close', 
          'avg_price_5', 'avg_price_10', 'avg_price_50',
          'avg_price_200', 'avg_volume_5', 'avg_volume_10']]
    )
    ## subset to analysis date
    # get date
    fn = (
        glob
        .glob(STOCK_AVG_BUFFER + '/prepared/*.csv')[0]
        .split('/')[-1]
        .split('T')[0]
        .split('.csv')[0]
        .split('-')
    )
    date = (
        dt.date(int(fn[0]), int(fn[1]), int(fn[2]))
        - dt.timedelta(1) ## bc collect yesterday prices
        )
    # filter
    price_df_subset = (
        price_df_full
        .loc[price_df_full['date'] == date]
    )
    if len(price_df_subset) > 0:
        _ = (
            price_df_subset
            .to_csv(STOCK_AVG_BUFFER + '/finished/{}.csv'.format(date), index=False)
        )
    return True

def migrate_pv():
    """
    Migrate analyst target price data.
    Use file name as partition.
    """
    ## subset
    file_list = glob.glob(STOCK_AVG_BUFFER + '/finished/' + '*.csv')
    if len(file_list) == 0:
        return False
    price_df = utils.read_many_csv(STOCK_AVG_BUFFER + '/finished/')
    date = (
        pd.to_datetime(
            glob.glob(STOCK_AVG_BUFFER + '/finished/*.csv')[0]
            .split('/')[-1]
            .split('T')[0]
            .split('.csv')[0]
        ).date()
    )
    date_cols = ['date']
    for col in date_cols:
        price_df[col] = (
            pd.to_datetime(price_df[col])
            .apply(lambda r: r.date())
        )
    ## migrate
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('migrate-price-vol-avg') 
        .getOrCreate()
    )
    _ = utils.write_spark(spark, price_df, 
                          'derived-measurements/avg-price-vol', date)
    _ = spark.stop()
    return True