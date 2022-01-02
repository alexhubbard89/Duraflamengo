import pandas as pd
import numpy as np
import datetime as dt
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

from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER = sm_data_lake_dir+'/buffer/marketwatch-financials/{}/{}.csv'
WRITE_PATH = sm_data_lake_dir + '/marketwatch-financials/{}/{}.csv'

## Methods to get and clean data
def format_data(df):
    for col in df.columns:
        if col == 'ticker':
            df[col] = df[col].astype(str)
        elif col == 'date':
            df[col] = pd.to_datetime(df[col]).apply(lambda r: r.date())
        else:
            df[col] = df[col].astype(float)
            
    df['collection_date'] = (
        dt.datetime.now(pytz.timezone('America/New_York'))
        .date()
    )
    return df

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

def get_financials(ticker: str, fin_type: str , freq: str) -> bool:
    """
    Find the balance-sheet, income, or cash-flow
    for a given ticker symbol.
    
    fin_types: 'balance-sheet', 'cash-flow', 'income'
    freqs: 'quarter', 'year'    
    
    Inputs:
           ticker - The stock ticker symbol
           fin_type - The type of financials
           freq - Annual or quartely report
    Return: True for collection success
    """
    if '/' in ticker:
        return False
    all_data = pd.DataFrame()
    if freq == 'quarter':
        url = 'https://www.marketwatch.com/investing/stock/{}/financials/{}/quarter'.format(ticker.lower(), fin_type)
        index_col = 'date'
    else:
        url = 'https://www.marketwatch.com/investing/stock/{}/financials/{}'.format(ticker.lower(), fin_type)
        index_col = 'year'
    spoofer_obj = Ip_Spoofer()
    page = spoofer_obj.request_page(url)
    
    if page == None:
        return all_data
    
    head_list = page.findAll('thead', {'class': 'table__header'})
    body_list = page.findAll('tbody', {'class': 'table__body row-hover'})
    if len(body_list) == 0:
        return all_data
    
    for i in range(len(head_list)):
        thead = head_list[i]
        tbody = body_list[i]

        cols = [clean(x.text) for x in thead.find('tr').findAll('th')[:-1]]
        data = []
        for tr in tbody.findAll('tr'):
            _ = data.append([clean(x.text) for x in tr.findAll('td')[:-1]])

        df = pd.DataFrame(data, columns=cols)
        for col in cols[1:]:
            df[col] = utils.int_extend(df[col])

        all_data = all_data.append(df).reset_index(drop=True)

    all_data = all_data.set_index(all_data.columns[0]).transpose().reset_index().rename(columns={'index': index_col})
    all_data.columns.name = None
    all_data.columns = [clean_col(col) for col in all_data.columns]
    cols = list(set(all_data.columns) - set([index_col]))
    for col in cols:
        try:
            index_ = all_data.loc[all_data[col].notnull()].index
        except ValueError as e:  
            if str(e) == 'Cannot index with multidimensional key':
                all_data = all_data.drop([col], 1)
                all_data.loc[:, col] = np.nan
                index_ = [0]
            
        if len(index_) == 0:
            continue
        all_data[col] = (
            all_data
            .loc[index_, col]
            .apply(lambda r: np.nan if r in ['None', 'N/A'] else r)
            .astype(float)
        )
    all_data[index_col] = pd.to_datetime(all_data[index_col])
    all_data['ticker'] = ticker
    
    ## format for writes
    formatted_data = format_data(all_data)
    formatted_data.to_csv(BUFFER.format(fin_type, ticker), index=False)
    return True

def migrate_financials(ticker: str, fin_type: str) -> bool:
    """
    Migrate the balance-sheet, income, or cash-flow
    for a given ticker symbol from the buffer to the datalake.
    
    fin_types: 'balance-sheet', 'cash-flow', 'income'
    freqs: All collections are quarterly for now.
    
    Inputs:
           ticker - The stock ticker symbol
           fin_type - The type of financials
           freq - Annual or quartely report
    Return: True for migration success.
    """
    buffer_df = pd.read_csv(BUFFER.format(fin_type, ticker))
    dl_df = pd.read_csv(WRITE_PATH.format(fin_type, ticker))
    ## format date for sort
    date_cols = ['collection_date', 'date']
    for col in date_cols:
        buffer_df[col] = (
            pd.to_datetime(buffer_df[col])
            .apply(lambda r: r.date())
        )
        dl_df[col] = (
            pd.to_datetime(dl_df[col])
            .apply(lambda r: r.date())
        )
    ## full dataset
    full_df = (
        dl_df
        .append(buffer_df)
        .sort_values('collection_date', ascending=False)
        .drop_duplicates('date')
    )
    full_df = full_df.loc[full_df['date'].notnull()]
    ## replace data
    _ = (
        full_df
        .to_csv(WRITE_PATH.format(fin_type, ticker), index=False)
    )
    ## remove from buffer
    _ = os.remove(BUFFER.format(fin_type, ticker))
    return True

## Pipeline methods to distribute 
def distributed_collection(collect_threshold: float, 
                           fin_type: str, 
                           loop_collect: int) -> bool:
    """
    Collect all financials data per collection type.
    
    fin_types: 'balance-sheet', 'cash-flow', 'income'
    freqs: All collections are quarterly for now.
    
    Inputs:
           collect_threshold - Collect enough data before stopping
           fin_type - The type of financials
           loop_collect - Number of tickers to collect per loop
    Return: True for migration success.
    """
    print('start session\n\n\n')
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('marketwatch-financials-collection') 
        .getOrCreate()
    )
    sc = spark.sparkContext

    ## Set buffer based on collection type
    TMP_BUFFER = BUFFER.format(fin_type, 'split').split('split')[0]

    ##### Get tickers to collect
    ue_df = pd.read_csv(BUFFER+'/to-collect/data.csv')
    all_ticker_list = ue_df['ticker'].tolist()
    collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(TMP_BUFFER+'*')]
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
    collect_percent_og = collect_percent ## prevents error
    collected_analyst_estimate_df = pd.DataFrame()
    
    count = 0
    while collect_percent < collect_threshold:
        _str = """
        COLLECTING PRICE DATA - LOOP {}
        \tTickers Left to Collect: {}
        \tTickers collected: {}
        \tPercent collected: {}
        """
        print(_str.format(count, len(ticker_list), len(collected_list), 
                          collect_percent, collect_threshold, 
                          collect_percent > collect_threshold))
        ## make requests
        analyst_estimate_df_list = (
            sc
            .parallelize(ticker_list)
            .map(lambda t: get_financials(t, fin_type, 'quarter'))
            .collect()
        )
        ## calculate percent collected
        collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(TMP_BUFFER+'*')]
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
        if count > 50:
            collect_percent = 1
        time.sleep(60) ## try to prevent black listing
    ## Exite the loop and write
    _str = """
    FINAL COLLECTION STATS – {} Loops
    \tTickers collected: {}
    \tPercent collected: {}
    """
    print(_str.format(count, len(collected_list), collect_percent_og))

    ## done
    return True

def distributed_migration(fin_type: str) -> bool:
    """
    Migrate all collected financials.
    
    Input: The type of financials to migrate.
    Return: Bool.
    """
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('marketwatch-financials-migration')
        .getOrCreate()
    )
    sc = spark.sparkContext
    ## Get collected tickers
    ## Set buffer based on collection type
    TMP_BUFFER = BUFFER.format(fin_type, 'split').split('split')[0]
    collected_list = [x.split('/')[-1].split('.csv')[0] for x in glob.glob(TMP_BUFFER+'*')]
    ## Migrate and remove file from buffer
    _ = (
        sc
        .parallelize(collected_list)
        .map(lambda t: migrate_financials(t, fin_type))
        .collect()
    )
    return True
