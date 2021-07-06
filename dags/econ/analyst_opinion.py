import pandas as pd
import numpy as np
from datetime import datetime
import string

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

## Local code
from common.scripts.spoof import Ip_Spoofer

## database stuff
import psycopg2
import urllib.parse as urlparse
import os
from sqlalchemy import event, create_engine ## fast writes
import psycopg2.extras
import psycopg2.errorcodes
urlparse.uses_netloc.append("postgres")
def open_connection():
    url = urlparse.urlparse(os.environ["DATABASE_URL"])
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection
engine = create_engine(os.environ["DATABASE_URL"], use_batch_mode=True)


def get_marketwatch(ticker):
    ## request page
    url = 'https://www.marketwatch.com/investing/stock/{}/analystestimates'.format(ticker.lower())
    spoofer_obj = Ip_Spoofer()
    page = spoofer_obj.request_page(url, print_url=False)

    try:
        ## isolatge page
        page_ar = page.find('div', {'class': 'element element--analyst analyst-ratings'})
        del page
        
        ## collect columns
        # columns should remain the same
        # cols = [x.text for x in page_ar.find('thead').findAll('th')]
        # cols[0] = 'rating'
        cols = ['rating', 'three_m_ago', 'one_m_ago', 'current']

        ## collect data
        data = []
        for tr in page_ar.find('tbody').findAll('tr')[:-1]:
            data.append([x.text.replace('\n', '') for x in tr.findAll('td')])

        ## structure data
        df = pd.DataFrame(data, columns=cols)
        for col in cols:
            df[col] = df[col].apply(lambda r: None if r == 'N/A' else r)
        

        ## add ticker
        df['ticker'] = ticker
        df['collection_date'] = datetime.now().date()
        df['collected'] = True
        return df.to_dict()
    
    except:
        ## If error log and move on.
        ## The most common is that the analyst rating does not exist, 
        ## for multiple reasons.
        ## The exception cased should be narrowed down after a few collection cycles
        
        return (
            pd.DataFrame([[ticker, datetime.now().date(), False]], 
                         columns=['ticker', 'collection_date', 'collected'])
            .to_dict()
        )
        
def prep_marketwatch(df_list):
    ## unpack
    df = (
        pd.concat([pd.DataFrame(x) for x in df_list])
        .reset_index(drop=True)
    )
    
    ## for now not always
    df = (
        df
        .rename(columns={'3M Ago': 'three_m_ago', 
                         '1M Ago':'one_m_ago', 
                         'Current': 'current'})
    )
    
    ## force types
    str_cols = ['rating', 'ticker']
    date_cols = ['collection_date']
    bool_cols = ['collected']
    int_cols = ['three_m_ago', 'one_m_ago', 'current']
    for col in str_cols:
        df[col] = df[col].apply(str)
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    for col in bool_cols:
        df[col] = df[col].astype(bool)
    for col in int_cols:
        df[col] = df[col].astype(float)
    return df
        
def write_spark(spark, df, data_dir, subdir, date):
    df_sp = spark.createDataFrame(df)
    file_path = '{}/{}/{}'.format(data_dir, subdir, str(date))
    (df_sp
     .write
     .format("orc")
     .mode("overwrite")
     .option("compression", "snappy")
     .save(file_path)
    )
    return True

def pipeline(data_dir):
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('analyst-estimates') 
        .getOrCreate()
    )
    sc = spark.sparkContext


    ##### Get tickers to collect
    ## this can point to local directory and query via spark
    sql_ = """
    SELECT DISTINCT ticker
    FROM stock_market.historical_price
    """
    ticker_df = pd.read_sql_query(sql_, open_connection())
    ticker_df = (
        ticker_df
        .loc[ticker_df['ticker'].apply(lambda r: '.' not in r)]
    )
    ticker_list = ticker_df['ticker'].tolist()

    ## make requests
    analyst_estimate_df_list = (
        sc
        .parallelize(ticker_list)
        .map(get_marketwatch)
        .collect()
    )

    ## unpack the data
    analyst_estimate_df = prep_marketwatch(analyst_estimate_df_list)

    ## write data
    subdir = 'analyst-opinion/marketwatch'
    d = str(analyst_estimate_df.loc[0, 'collection_date'].date())
    write_spark(spark, analyst_estimate_df, data_dir, subdir, d)

    ## done
    return True

if __name__ == "__main__":
    pipeline(data_dir='/Users/alexanderhubbard/stock-market/data')