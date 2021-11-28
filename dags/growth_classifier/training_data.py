import pandas as pd
import numpy as np
import datetime as dt
import string
import pytz
import glob
import shutil, os
from scipy import stats

import pandas_market_calendars as mcal

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
## derived data locations
# orc 
AVG_PV_DIR = sm_data_lake_dir+'/derived-measurements/avg-price-vol/*'
# below are the datasets that 
# require forward fill
AO_SCORE_DIR = sm_data_lake_dir+'/derived-measurements/ao-score/*'
AO_PT_DIR = sm_data_lake_dir+'/derived-measurements/ao-price-target/*'
INCOME_TRENDS_DIR = sm_data_lake_dir+'/derived-measurements/income-trends/*'
BS_TRENDS_DIR = sm_data_lake_dir+'/derived-measurements/balance-sheet-trends/*'
CF_TRENDS_DIR = sm_data_lake_dir+'/derived-measurements/cash-flow-trends/*'

dir_full_list = [
    AVG_PV_DIR
]
dir_sparse_list = [
    AO_SCORE_DIR,
    AO_PT_DIR
]
fin_trends_list = [
    INCOME_TRENDS_DIR,
    BS_TRENDS_DIR,
    CF_TRENDS_DIR
]
## buffer
TRAINING_BUFFER = sm_data_lake_dir+'/buffer/training-data'
## output
TRAINING_DIR = sm_data_lake_dir+'/training-data/growth-stock-selector'

###################
##### Methods #####
###################

def set_date(date: dt.date) -> bool:
    """
    Make empty file with date of training
    data set. 
    This is done because I don't know how
    to use an input with sparks functions.
    Since the date is a varying argument,
    I will use the python operator
    to communicate with spark.
    """
    df = pd.DataFrame([date], columns=['date'])
    date_str = str(date)[:10]
    _ = df.to_csv(TRAINING_BUFFER+'/{}.csv'.format(date_str))
    return True

def explode_ticker(ticker, base_dates_df):
    df = base_dates_df.copy()
    df['ticker'] = ticker
    return df.sort_values('date')

def make_training_data(growth_threshold=1.1,
                       window=120
                      ) -> bool:
    """
    This is a work in progress. The
    function needs to be broken down.
    This is first iteration to get the
    infrastructure set up.

    This function locates all data 
    needed for the model and joins it
    together.
    
    Fitting future data takes additional
    logic. Since 90 days in the future 
    can be a non-market day, not all
    future dates will have a past counterpart.
    I need to have the date be a little flexible.
    
    """
    ## gete date
    fn = (
        glob
        .glob(TRAINING_BUFFER + '/*.csv')[0]
        .split('/')[-1].split('.csv')[0]
        .split('T')[0]
        .split('-')
    )
    date = dt.date(int(fn[0]), int(fn[1]), int(fn[2]))

    ## start session
    spark = (
        SparkSession
        .builder 
        .appName('building-growth-training') 
        .getOrCreate()
    )
    
    ## make date vars
    date_max = date - dt.timedelta(days=90)
    date_min = date - dt.timedelta(days=window+90)
    date_min_extra = date - dt.timedelta(days=365+90)
    training_date_range = [x.date() for x in pd.date_range(date_min, date_max)]
    ## get price data
    training_data = (
        spark
        .read
        .format("orc")
        .option("path", AVG_PV_DIR)
        .load()
        .filter((
            ## training window
            ((F.col('date') >= date_min) & (F.col('date') <= date_max)) |
            ## test date
            (F.col('date') == date)
        ))
        .toPandas()
    )
    training_data['date'] = (
        pd.to_datetime(training_data['date'])
        .apply(lambda r: r.date())
    )
    training_data['week_num'] = (
        training_data['date']
        .apply(lambda r: r.isocalendar()[1])
    )
    ## is there data for the day?
    if (training_data['date'] == date).sum() == 0:
        return False
    ## get future price data
    ## load and migrate all data
    future_price = (
        spark
        .read
        .format("orc")
        .option("path", AVG_PV_DIR)
        .load()
        ## add buffer time to max date
        .filter(((F.col('date') >= date_min) & 
                 (F.col('date') <= date+dt.timedelta(90+10))))
        .select('ticker', 'date', 'close', 'avg_price')
        .toPandas()
        .rename(columns={
            'date': 'future_date', 
            'close': 'future_close',
            'avg_price': 'future_avg_close'
        })
    )
    if len(future_price) > 0:
        ## not all training data will have a future price
        ## fill missing close
        for col in ['future_close', 'future_avg_close']:
            future_price[col] = (
                future_price
                .groupby('ticker')
                [col]
                .fillna(method='ffill')
            )
        ## date and week num to join backwards
        future_price['date'] = (
            future_price['future_date'] 
            - dt.timedelta(90)
        )
        future_price['week_num'] = (
            future_price['date']
            .apply(lambda r: r.isocalendar()[1])
        )
        ## remove week duplicates for join
        future_price_distinct = (
            future_price
            .sort_values(['ticker', 'week_num', 'date'])
            .drop('date', 1)
            .drop_duplicates(['ticker', 'week_num'])
        )
        ## join future data
        training_data = (
            training_data
            .merge(future_price_distinct, how='left', on=['ticker', 'week_num'])
        )
        ## clear space
        del future_price_distinct
        ## add growth bool
        training_data['growth'] = (
            (
                training_data['future_close'] 
                / training_data['avg_price'] 
            )
            >= growth_threshold
        )
    ## make df for days market is open.
    ## Join to fill missing data and 
    ## perform smoothing.
    ## Leave NaNs blank.
    ## Fill later if needed.
    nyse = mcal.get_calendar('NYSE')
    nyse_cal = nyse.schedule(start_date=date_min_extra, end_date=date)
    nyse_date_list = [x for x in nyse_cal['market_open'].apply(lambda r: r.date())]
    BASE_DF = pd.DataFrame(nyse_date_list, columns=['date'])
    ## explode once
    ## explode full date list
    date_ticker_explode_df = (
        pd.concat([explode_ticker(t, BASE_DF) for t in training_data['ticker'].unique()])
        .reset_index(drop=True)
    )
    # Use extended window to fill missing data using ffill.
    # Left join on price ensures the correct dates.
    # Do not fill NA for sparse. This can be done later.
    # Being NA is more helpful.
    for _dir in dir_sparse_list:
        print(_dir)
        tmp_df = (
            spark
            .read
            .format("orc")
            .option("path", _dir)
            .load()
            .filter((
                ## training window
                ((F.col('date') >= date_min_extra) & 
                 (F.col('date') <= date))
            ))
            .orderBy(['ticker', 'date'])
            .toPandas()
        )
        tmp_df['date'] = (
            pd.to_datetime(tmp_df['date'])
            .apply(lambda r: r.date())
        )
        ## fill missing
        full_tmp_df = (
            date_ticker_explode_df.copy()
            .merge(tmp_df, how='left', on=['date', 'ticker'])
        )
        ## Scores lose magnitude as days since last scored
        ## moves farther in the past, up to 180 days.
        if _dir == AO_SCORE_DIR:
            full_tmp_df['weight'] = (
                (
                    180
                    - full_tmp_df['n_days_last_rating']
                )
                /180
            )
            ## weight the change
            full_tmp_df['score_normalized_weighted'] = (
                full_tmp_df['weight'] 
                * full_tmp_df['score_normalized']
            )
            ## remove unneeded columns
            full_tmp_df = (
                full_tmp_df
                .drop(['weight', 'score_normalized'], 1)
            )
        training_data = (
            training_data
            .merge(full_tmp_df, how='left', on=['ticker', 'date'])
        )
        del full_tmp_df, tmp_df
    ## make growth target from analyist opinions
    training_data['growth_target'] = (
        training_data['pt_avg'] / training_data['avg_price']
    )
    ## Use extended window to fill missing data using ffill.
    ## Left join on price ensures the correct dates.
    ## Fill NA bc financials come out once a quarter
    ## and are used until new financials come out.
    fin_types = ['income', 'balance-sheet', 'cash-flow']
    for fin_type in fin_types:
        print(fin_type)
        data_location = sm_data_lake_dir+'/derived-measurements/{}-trends'.format(fin_type)
        tmp_df = pd.DataFrame()
        print(date_min, date_max, date)
        for d in training_date_range + [date]:
            data_loc = data_location + '/' + str(d)[:10]
            if not os.path.isdir(data_loc):
                continue
            ## prepare data
            ## loda data
            tmp_day_df = (
                spark
                .read
                .format("orc")
                .option("path", data_loc)
                .load()
                .orderBy(['ticker', 'date'])
                .toPandas()
            )
            tmp_day_df['date'] = (
                pd.to_datetime(tmp_day_df['date'])
                .apply(lambda r: r.date())
            )
            tmp_df = (
                tmp_df
                .append(tmp_day_df)
                .reset_index(drop=True)
            )
        ## Join financials to training set
        ## prevent dupe cols
        if (
            (len([x for x in training_data.columns if 'preferred_dividends' in x]) == 0) &
            (len([x for x in tmp_df.columns if 'preferred_dividends' in x]) > 0)
        ):
            tmp_df = tmp_df.drop(columns=['preferred_dividends'])
        if (
            (len([x for x in training_data.columns if 'release_date' in x]) == 0) &
            (len([x for x in tmp_df.columns if 'release_date' in x]) > 0)
        ):
            tmp_df = tmp_df.drop(columns=['release_date'])
        training_data = (
            training_data
            .merge(tmp_df, how='left', on=['ticker', 'date'])
        )
        del tmp_df
    ## do not need to query over many days.
    ## csv will do just fine.
    _ = training_data.to_csv(TRAINING_DIR+'/{}.csv'.format(date), index=False)
    _ = spark.stop()
    return True