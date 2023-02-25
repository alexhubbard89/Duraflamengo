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
from common.spoof import Ip_Spoofer
import common.utils as utils

## get global vars
from airflow.models import Variable

sm_data_lake_dir = Variable.get("sm_data_lake_dir")
TDA = "https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory?apikey={api}&periodType={periodType}&period={period}&frequencyType={frequencyType}&frequency={frequency}&needExtendedHoursData=True"
DAILY_WRITE_BUFFER = sm_data_lake_dir + "/buffer/tda-daily-price/"
MINUTE_WRITE_BUFFER = sm_data_lake_dir + "/buffer/tda-minute-price/"
MINUTE_WATCHLIST_WRITE_BUFFER = sm_data_lake_dir + "/buffer/tda-minute-price-watchlist/"
PRICE_TICKER_DIR = sm_data_lake_dir + "/tda-daily-price/ticker"

## functions
def make_minute_buffer():
    print("clear buffer")
    shutil.rmtree(MINUTE_WRITE_BUFFER)
    os.mkdir(MINUTE_WRITE_BUFFER)
    ## make dir to track collection
    os.mkdir(MINUTE_WRITE_BUFFER + "collected/")
    ## leave extra space to ensure all dirs are ready
    end_date = datetime.now(pytz.timezone("America/New_York")).date()
    start_date = end_date - timedelta(days=30)
    for d in pd.date_range(start_date, end_date):
        date = str(d.date())
        os.mkdir(MINUTE_WRITE_BUFFER + date)
    return True


def get_historical_minute_price(ticker, buffer=MINUTE_WRITE_BUFFER):
    ## request
    url = TDA.format(
        ticker=ticker.upper(),
        api=os.environ["TDA_API_KEY"],
        periodType="day",
        period=10,
        frequencyType="minute",
        frequency=1,
    )
    r = requests.get(url=url)
    try:
        td_historical = pd.DataFrame(r.json()["candles"])
        if len(td_historical) == 0:
            return False

        ## format
        td_historical["datetime"] = pd.to_datetime(td_historical["datetime"], unit="ms")
        td_historical["date"] = td_historical["datetime"].apply(lambda r: r.date())

        td_historical["ticker"] = ticker

        ## write out individual dates
        for d in td_historical["date"].unique().tolist():
            date = str(d)
            df_write = td_historical.loc[td_historical["date"] == d]
            write_path = MINUTE_WRITE_BUFFER + date + "/{}.csv".format(ticker)
            df_write.to_csv(write_path, index=False)
        ## make file to signify collection
        _ = pd.DataFrame().to_csv(buffer + "collected/{}.csv".format(ticker))
        return True
    except:
        return False


def get_minute_collected(buffer: str = MINUTE_WRITE_BUFFER):
    return [x.split("/")[-1].split(".")[0] for x in glob.glob(buffer + "collected/*")]


def minute_price_pipeline(collect_threshold=0.85, loop_collect=240, watchlist=False):
    """
    https://developer.tdameritrade.com/content/authentication-faq
    Q: Are requests to the Post Access Token API throttled?
        All non-order based requests by personal use non-commercial
        applications are throttled to 120 per minute.
    """
    ## start spark session
    spark = SparkSession.builder.appName("minute-tda-price-collect").getOrCreate()
    sc_tda = spark.sparkContext

    ## set collection variables
    if watchlist == False:
        ticker_file = sm_data_lake_dir + "/seed-data/nasdaq_screener_1628807233734.csv"
        ticker_df = pd.read_csv(ticker_file)
        all_ticker_list = ticker_df["Symbol"].tolist()
        buffer = MINUTE_WRITE_BUFFER
    elif watchlist == True:
        all_ticker_list = utils.get_watchlist()
        buffer = MINUTE_WATCHLIST_WRITE_BUFFER
    collected_list = get_minute_collected(buffer)
    tickers_left = list(set(all_ticker_list) - set(collected_list))
    ## delete later
    if len(tickers_left) < loop_collect:
        ticker_list = tickers_left  ## prevents exception
    else:
        ticker_list = list(np.random.choice(tickers_left, loop_collect, replace=False))
    collect_percent = len(collected_list) / len(all_ticker_list)
    collect_percent_og = collect_percent

    count = 0
    while collect_percent < collect_threshold:
        start_loop = datetime.now()
        _str = """
        COLLECTING MINUTE BY MINUTE PRICE DATA - LOOP {}
        \tTickers left to collect: {}
        \tAttemptint to collect: {}
        \tTickers collected: {}
        \tPercent collected: {}
        \tSample tickers: {}
        """
        print(
            _str.format(
                count,
                len(tickers_left),
                len(ticker_list),
                len(collected_list),
                collect_percent,
                ticker_list[:5],
            )
        )

        ## make requests
        yesterday_price_df_list = (
            sc_tda.parallelize(ticker_list)
            .map(lambda t: get_historical_minute_price(t, buffer=buffer))
            .collect()
        )

        ## calculate percent collected
        collected_list = get_minute_collected(buffer)
        tickers_left = list(set(all_ticker_list) - set(collected_list))
        if len(tickers_left) < loop_collect:
            ticker_list = tickers_left  ## prevents exception
        else:
            ticker_list = list(
                np.random.choice(tickers_left, loop_collect, replace=False)
            )
        collect_percent = len(collected_list) / len(all_ticker_list)
        collect_percent_og = collect_percent
        if collect_percent >= collect_threshold:
            collect_percent = 1
            continue  ## no need to sleep
        ## iterat the counter and exit if too many
        count += 1
        if count > 200:
            collect_percent = 1
            continue  ## no need to sleep
        ## Avoid throttle and sleep if needed
        loop_time = (datetime.now() - start_loop).seconds
        print("Sleep time...")
        sleep_time = 60 - loop_time
        if sleep_time > 0:
            time.sleep(sleep_time)

    ## Exit the loop and write
    _str = """
    FINAL MINUTE BY MINUTE COLLECTION STATS – {} Loops
    \tTickers collected: {}
    \tPercent collected: {}
    """
    print(_str.format(count - 1, len(collected_list), collect_percent_og))


def migrate_tda_minute():
    ## start spark session
    spark = SparkSession.builder.appName("migrate-tda-minute").getOrCreate()
    ## get directories to migrate
    dir_list = [x[0].split("/")[-1] for x in os.walk(MINUTE_WRITE_BUFFER)]
    date_list = [x for x in dir_list if x not in ["", "collected"]]
    for date in date_list:
        try:
            collected_tickers_df = (
                spark.read.format("csv")
                .options(header="true")
                .load(MINUTE_WRITE_BUFFER + date + "/*")
            )
            print("write data")
            _ = utils.write_spark(spark, collected_tickers_df, "tda-minute-price", date)
        except:
            pass  ## empty dirs done purposefully
    return True


def get_historical_daily_price(ticker, date):
    ## request
    url = TDA.format(
        ticker=ticker.upper(),
        api=Variable.get("TDA_API_KEY"),
        periodType="year",
        period=10,
        frequencyType="daily",
        frequency=1,
    )
    r = requests.get(url=url)
    try:
        td_historical = pd.DataFrame(r.json()["candles"]).rename(
            columns={"datetime": "date"}
        )
        if len(td_historical) == 0:
            return False
        ## format
        td_historical["ticker"] = ticker
        td_historical["date"] = pd.to_datetime(td_historical["date"], unit="ms").apply(
            lambda r: r.date()
        )
        ## slice
        td_historical_subset = td_historical.loc[
            td_historical["date"] == date
        ].reset_index(drop=True)
        if not os.path.exists(f"{PRICE_TICKER_DIR}/{ticker}"):
            os.makedirs(f"{PRICE_TICKER_DIR}/{ticker}")
        td_historical.to_csv(f"{PRICE_TICKER_DIR}/{ticker}/data.csv", index=False)
        td_historical_subset.to_csv(f"{DAILY_WRITE_BUFFER}{ticker}.csv", index=False)
        return True
    except:
        return False


def daily_price_pipeline(collect_threshold=0.85, loop_collect=240):
    """
    https://developer.tdameritrade.com/content/authentication-faq
    Q: Are requests to the Post Access Token API throttled?
        All non-order based requests by personal use non-commercial
        applications are throttled to 120 per minute.
    """
    ## start spark session
    spark = SparkSession.builder.appName("daily-tda-price-collect").getOrCreate()
    sc_tda = spark.sparkContext

    ## get collection date
    YESTERDAY = datetime.now(pytz.timezone("America/New_York")).date() - timedelta(
        days=1
    )

    ## set collection variables
    ticker_file = sm_data_lake_dir + "/seed-data/nasdaq_screener_1628807233734.csv"
    ticker_df = pd.read_csv(ticker_file)
    all_ticker_list = ticker_df["Symbol"].tolist()
    collected_list = [
        x.split("/")[-1].split(".csv")[0] for x in glob.glob(DAILY_WRITE_BUFFER + "*")
    ]
    tickers_left = list(set(all_ticker_list) - set(collected_list))
    if len(tickers_left) < loop_collect:
        ticker_list = tickers_left  ## prevents exception
    else:
        ticker_list = list(np.random.choice(tickers_left, loop_collect, replace=False))
    collect_percent = len(collected_list) / len(all_ticker_list)
    collect_percent_og = collect_percent

    count = 0
    while collect_percent < collect_threshold:
        start_loop = datetime.now()
        _str = """
        COLLECTING PRICE DATA - LOOP {}
        \tTickers left to collect: {}
        \tAttemptint to collect: {}
        \tTickers collected: {}
        \tPercent collected: {}
        \tDate: {}
        \tSample tickers: {}
        """
        print(
            _str.format(
                count,
                len(tickers_left),
                len(ticker_list),
                len(collected_list),
                collect_percent,
                YESTERDAY,
                ticker_list[:5],
            )
        )

        ## make requests
        yesterday_price_df_list = (
            sc_tda.parallelize(ticker_list)
            .map(lambda t: get_historical_daily_price(t, YESTERDAY))
            .collect()
        )

        ## calculate percent collected
        collected_list = [
            x.split("/")[-1].split(".csv")[0]
            for x in glob.glob(DAILY_WRITE_BUFFER + "*")
        ]
        tickers_left = list(set(all_ticker_list) - set(collected_list))
        if len(tickers_left) < loop_collect:
            ticker_list = tickers_left  ## prevents exception
        else:
            ticker_list = list(
                np.random.choice(tickers_left, loop_collect, replace=False)
            )
        collect_percent = len(collected_list) / len(all_ticker_list)
        collect_percent_og = collect_percent
        if collect_percent >= collect_threshold:
            continue  ## no need to sleep
        ## iterat the counter and exit if too many
        count += 1
        if count > 200:
            collect_percent = 1
            continue  ## no need to sleep
        ## Avoid throttle and sleep if needed
        loop_time = (datetime.now() - start_loop).seconds
        sleep_time = 60 - loop_time
        if sleep_time > 0:
            time.sleep(sleep_time)

    ## Exit the loop and write
    _str = """
    FINAL COLLECTION STATS – {} Loops
    \tTickers collected: {}
    \tPercent collected: {}
    \tDate: {}
    """
    print(_str.format(count - 1, len(collected_list), collect_percent_og, YESTERDAY))

    ###### Move everything below to its own task in DAG
    ## get everything in buffer
    collected_tickers_df = (
        spark.read.format("csv").options(header="true").load(DAILY_WRITE_BUFFER + "*")
    )
    print("write data")
    _ = utils.write_spark(
        spark, collected_tickers_df, "tda-daily-price/date", YESTERDAY
    )

    ## clear buffer directory
    print("clear buffer")
    shutil.rmtree(DAILY_WRITE_BUFFER)
    os.mkdir(DAILY_WRITE_BUFFER)
    return True


if __name__ == "__main__":
    _ = daily_price_pipeline()
