import pandas as pd
import numpy as np
import datetime as dt
import pytz
import glob
import shutil
import os

## make database connection
from sqlalchemy import event, create_engine

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

## Local code
import common.scripts.utils as utils
from common.scripts.spoof import Ip_Spoofer

from airflow.models import Variable
sm_data_lake_dir = Variable.get("sm_data_lake_dir")
BUFFER = sm_data_lake_dir+'/buffer/marketbeat-earnings'
WRITE_PATH_ALL_EARNINGS = sm_data_lake_dir + '/marketbeat-earnings'
WRITE_PATH_NOW_EARNINGS = sm_data_lake_dir + '/marketbeat-earnings-upcoming'
MB_BASE_URL = 'https://www.marketbeat.com/stocks/{EXCHANGE}/{TICKER}/earnings/'


###################
##### Methods #####
###################
def check_date(date_text):
    try:
        pd.to_datetime(date_text)
        return True
    except ValueError:
        return False

def format_earnings(df):
    ## remove bad data
    df_cp = (
        df
        .loc[df['date'].apply(check_date)]
        .copy()
        .reset_index(drop=True)
    )
    
    float_cols = [
        'consensus_estimate',
        'reported_eps',
        'beat_miss',
        'gaap_eps',
        'revenue_estimate',
        'actual_revenue'
    ]
    for col in df_cp.columns:
        if col in ['ticker', 'quarter']:
            df_cp[col] = (
                df_cp
                [col]
                .apply(lambda r: None if r == '' else r)
                .astype(str)
            )
        elif col == 'date':
            ## remove extra str
            df_cp[col] = (
                df_cp[col]
                .apply(lambda r: r.split('(')[0])
            )
            ## convert
            df_cp[col] = (
                pd.to_datetime(df_cp[col])
                .apply(lambda r: r.date())
            )
        elif col in float_cols:
            df_cp[col] = (
                df_cp
                [col]
                .apply(lambda r: np.nan if r == '' else r)
                .astype(float)
            )
            
    df_cp['collection_date'] = (
        dt.datetime.now(pytz.timezone('America/New_York'))
        .date()
    )
    df_clean = (
        df_cp
        [['ticker', 'date', 'quarter', 'collection_date']
         + float_cols]
    )
    
    return df_clean

def request_earnings(ticker: str) -> bool:
    """
    Request financials for a given ticker.
    Write data to buffer.
    
    Input: Ticker of company to request.
    Return: Was the request successful?
    """
    for exchange in ['NASDAQ', 'NYSE']:
        ## request
        url = MB_BASE_URL.format(EXCHANGE=exchange, TICKER=ticker)
        spoofer_obj = Ip_Spoofer()
        page = spoofer_obj.request_page(url)
        ## located data
        page_table = page.find('table', {'id': 'earnings-history'})
        del page
        ## try to locate data
        try:
            ## get columns
            columns = []
            thead = (
                page_table
                .find('thead')
                .find('tr')
            )
            for th in thead.findAll('th'):
                col_val = (
                    th
                    .text
                    .lower()
                    .replace('/', '_')
                    .replace(' ', '_')
                )
                _ = columns.append(col_val)
            ## get data
            data = []
            tbody = (
                page_table
                .find('tbody')
            )
            for tr in tbody.findAll('tr'):
                r_data = []
                for th in tr.findAll('td'):
                    data_val = (
                        th
                        .text
                        .lower()
                        .replace(u'\xa0', u' ')
                        .replace('(estimated)', '')
                        .replace('($', '-')
                        .replace('+$', '')
                        .replace('$', '')
                        .replace(')', '')
                        .replace(',', '')
                        .strip(' ')
                    )
                    ## remove text
                    scalar_check_m = data_val.split(' million')
                    scalar_check_b = data_val.split(' billion')
                    if len(scalar_check_m) > 1:
                        data_val = int(float(scalar_check_m[0]) * 1000000)
                    elif len(scalar_check_b) > 1:
                        data_val = int(float(scalar_check_b[0]) * 1000000000)
                    del scalar_check_m, scalar_check_b

                    _ = r_data.append(data_val)
                _ = (
                    data
                    .append(r_data)
                )
        except:
            ## no table with data
            ## try other exchange
            continue
        ## format data
        df = pd.DataFrame(data, columns=columns)
        if len(df) > 0:
            df['ticker'] = ticker
            formated_df = format_earnings(df)
            ## write to buffer
            _ = formated_df.to_csv(BUFFER + '/collected/{}.csv'.format(ticker), index=False)
            ## if data exists exit
            return True
    ## exit loop.
    ## Must collect data bc previous 
    ## validation of earnings.
    ## If not found something is wrong.
    ## Try until it phases out, or the
    ## website takes requets again.
    return False

def find_upcoming(date: dt.date) -> bool:
    """
    From already collected earning
    reports, determine the lsit of
    tickers that will have earnings
    announcments in the near future.
    I will start scraping for updates
    as early as 75 days after the
    last report. Additionally, if there
    is a projected earnings report date,
    I will start requesting the information
    10 days before the projected date 
    (in case of any chagnes).
    
    Input: None.
    Return: List of tickers to request earnigns.
    """
    date = pd.to_datetime(date).date()
    mb_earnings_df = utils.read_many_csv(WRITE_PATH_ALL_EARNINGS+'/')
    mb_earnings_df['date'] = (
        pd.to_datetime(mb_earnings_df['date'])
        .apply(lambda r: r.date())
    )
    ## had reported earnings in the last 6 months
    recently_reported_ticker_df = (
        mb_earnings_df
        .loc[((mb_earnings_df['reported_eps'].notnull()) &
              (mb_earnings_df['date'] >= (date - dt.timedelta(180)))
             )]
        .sort_values(['ticker', 'date'], ascending=[True, False])
        .drop_duplicates('ticker')
    )
    recently_reported_ticker_list = (
        recently_reported_ticker_df
        ['ticker']
        .tolist()
    )
    #########################
    ##### Find upcoming #####
    #########################
    has_projected_date_df = (
        mb_earnings_df
        .loc[((mb_earnings_df['ticker'].isin(recently_reported_ticker_list)) &
              (mb_earnings_df['reported_eps'].isnull())
             )]
        .copy()
        .drop_duplicates('ticker')
        .reset_index(drop=True)
    )
    ## find tickers with no projected date
    missing_projected_date = (
        list(
            set(recently_reported_ticker_list) 
            - set(has_projected_date_df['ticker'])
        )
    )
    missing_projected_date_df = (
        recently_reported_ticker_df
        .loc[((recently_reported_ticker_df['ticker'].isin(missing_projected_date)) &
              (recently_reported_ticker_df['date'] >= dt.datetime.now().date() - dt.timedelta(60))
             )]
        .copy()
    )
    ## find tickers with upcoming projected date
    ## and combine with missing upcoming
    fd = dt.datetime.now().date() + dt.timedelta(30)
    upcoming_earnings_df = (
        has_projected_date_df
        .loc[has_projected_date_df['date'] <= fd]
        .append(missing_projected_date_df)
        .reset_index(drop=True)
        [['ticker', 'date']]
    )
    ## write to buffer
    ## this lets spark know what to distribute
    _ = upcoming_earnings_df.to_csv(BUFFER + '/to-collect/data.csv', index=False)
    return True

def distributed_collection() -> bool:
    """
    Pick up collection list
    from buffer and distribute
    collection.
    """
    ue_df = pd.read_csv(BUFFER+'/to-collect/data.csv')
    
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('marketbeat-financials-collection') 
        .getOrCreate()
    )
    sc = spark.sparkContext
    #################################
    ##### distribute collection #####
    #################################
    ## prep ticker list
    ticker_list = ue_df['ticker'].tolist()
    collected_list = [
        x.split('/')[-1].split('.csv')[0]
        for x in glob.glob(BUFFER+'/collected/*')
    ]
    to_collect_list = (
        list(
            set(ticker_list) 
            - set(collected_list)
        )
    )
    ## collect
    migration_results_list = (
        sc
        .parallelize(to_collect_list)
        .map(lambda t: request_earnings(t))
        .collect()
    )
    return True

def migrate_mb_fins(ticker: str) -> bool:
    """
    Migrate data for tickers with financials.
    Keep previously collected data.
    """
    ## file names
    fn_dl = WRITE_PATH_ALL_EARNINGS+'/{}.csv'.format(ticker)
    fn_b = BUFFER + '/collected/{}.csv'.format(ticker)
    ## read
    og_df_raw = pd.read_csv(fn_dl)
    og_df = (
        og_df_raw
        .loc[og_df_raw['reported_eps'].notnull()]
        ## remove future earnings rows
    )
    new_df = pd.read_csv(fn_b)
    ## bring together
    dist_cols = list(set(og_df.columns) - set('collection_date'))
    migrate_df = (
        og_df
        .append(new_df)
        .drop_duplicates(dist_cols)
        .reset_index(drop=True)
    )
    _ = migrate_df.to_csv(WRITE_PATH_ALL_EARNINGS+'/{}.csv'.format(ticker), index=False)
    del migrate_df
    return True

def migrate_daily() -> bool:
    """
    Distribute the migration process
    of daily earnings collected files.
    """
    ## start spark session
    spark = (
        SparkSession
        .builder 
        .appName('marketbeat-financials-migration') 
        .getOrCreate()
    )
    sc = spark.sparkContext
    collected_list = [
        x.split('/')[-1].split('.csv')[0]
        for x in glob.glob(BUFFER+'/collected/*')
    ]
    ## collect
    migration_results_list = (
        sc
        .parallelize(collected_list)
        .map(lambda t: migrate_mb_fins(t))
        .collect()
    )
    return True

def make_upcoming_table() -> bool:
    """
    Subset collect list to display
    tickers with antipated earnings
    for the next two weeks.
    Write and replace csv for FE.
    """
    ue_df = pd.read_csv(BUFFER+'/to-collect/data.csv')
    ue_df['date'] = (
        pd.to_datetime(ue_df['date'])
        .apply(lambda r: r.date())
    )
    ue_df_subset = (
        ue_df
        .loc[ue_df['date'] <= dt.datetime.today().date() + dt.timedelta(14)]
        .copy()
        .sort_values('date')
        .reset_index(drop=True)
    )
    _ = ue_df_subset.to_parquet(WRITE_PATH_NOW_EARNINGS+'/data.parquet', index=False)
    ## make avilable for webapp
    engine = create_engine(os.environ['DATABASE_URL'], use_batch_mode=True)
    _ = (
        ue_df_subset
        .to_sql('upcoming_earnings', schema='stomas', 
                con=engine, if_exists='replace', index=False)
    )
    return True
