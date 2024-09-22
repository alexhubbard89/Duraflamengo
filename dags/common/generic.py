## spark
from matplotlib.pyplot import get
from pyspark.sql import SparkSession

from typing import Callable
import pandas as pd
import datetime as dt
import requests
import os
import common.utils as utils
import tda.collect as tda
from collections import deque
from time import sleep

FMP_KEY = os.environ["FMP_KEY"]


def collect_generic_daily(
    ds: dt.date, url: str, dl_dir: str, dtypes: dict, data_return: bool = False
):
    """
    General FMP collection for dialy metrics.

    Inputs:
        - ds: Date
        - url: Formatted url
        - dl_dir: Data-lake location
        - dtypes: Data types
        - data_return: Return the data set
    """
    r = requests.get(url)
    data = r.json()
    if len(data) == 0:
        return False
    df = pd.DataFrame(data)
    df_typed = utils.format_data(df, dtypes)
    if data_return:
        return df_typed
    df_typed.to_parquet(dl_dir + f"/{ds}.parquet", index=False)
    df_typed.to_parquet(dl_dir + f"/latest.parquet", index=False)
    return True


# def collect_generic_ticker(
#     ticker: str,
#     add_ticker: bool,
#     url: str,
#     dl_ticker_dir: str,
#     dtypes: dict,
#     year: float = None,
# ):
#     """
#     General FMP collection for tickers.

#     Inputs:
#         - ds: Date
#         - ticker: ticker
#         - add_ticker: Is ticker missing from response
#         - url: Base url
#         - dl_ticker_dir: Ticker oriented data-lake location
#         - dtypes: Data types
#     """
#     if year == None:
#         r = requests.get(url.format(TICKER=ticker, API=FMP_KEY))
#     elif year != None:
#         r = requests.get(url.format(TICKER=ticker, YEAR=year, API=FMP_KEY))
#     if r.status_code == 429:
#         return ticker
#     data = r.json()
#     if len(data) == 0:
#         return False
#     df = pd.DataFrame(pd.DataFrame(data))
#     if add_ticker:
#         df["symbol"] = ticker
#     df_typed = utils.format_data(df, dtypes)
#     fn = dl_ticker_dir + f"/{ticker}.parquet"
#     if os.path.isfile(fn):
#         ## not all data come back fill full history
#         old_df = pd.read_parquet(fn)
#         df_typed = df_typed.append(old_df).drop_duplicates(ignore_index=True)
#     df_typed.to_parquet(fn, index=False)
#     return True


def collect_generic_page(
    ticker: str,
    add_ticker: bool,
    url: str,
    dl_ticker_dir: str,
    dtypes: dict,
):
    """Iterage page count until no more data returns."""
    dfs = []
    collect = True
    page = 0
    while collect:
        url_ = url.format(TICKER=ticker, API=FMP_KEY, PAGE=page)
        print(url_)
        r = requests.get(url_)
        if r.status_code == 429:
            sleep(5)
            continue
        data = r.json()
        if len(data) == 0:
            collect = False
            continue
        dfs.append(pd.DataFrame(pd.DataFrame(data)))
        page += 1
    if len(dfs) == 0:
        return False
    df = pd.concat(dfs)
    if add_ticker:
        df["symbol"] = ticker
    df_typed = utils.format_data(df, dtypes)
    fn = dl_ticker_dir + f"/{ticker}.parquet"
    if os.path.isfile(fn):
        ## not all data come back fill full history
        old_df = pd.read_parquet(fn)
        df_typed = df_typed.append(old_df).drop_duplicates(ignore_index=True)
    df_typed.to_parquet(fn, index=False)
    return True


# def collect_generic_distributed(
#     distribute_through: Callable,
#     spark_app: str,
#     **kwargs,
# ):
#     """
#     General FMP collection distribute through spark.

#     Inputs:
#         - distribute_through: Function to to distribute.
#         - spark_app: Name of the spark app.
#         - kwargs: Arguments for distribution function.
#     """
#     ## collect all
#     collection_list = utils.get_watchlist(extend=True)

#     print(f"Collect extended watch list {len(collection_list)}")
#     while len(collection_list) > 0:
#         distribution_list = [
#             utils.make_input("ticker", t, kwargs) for t in collection_list
#         ]
#         print("PRINT THIS THING!!\n\n\n\n")
#         print(distribution_list[:5])
#         print("PRINT THIS THING!!\n\n\n\n")
#         spark = SparkSession.builder.appName(f"collect-{spark_app}").getOrCreate()
#         sc = spark.sparkContext
#         return_list = (
#             sc.parallelize(distribution_list)
#             .map(lambda r: distribute_through(**r))
#             .collect()
#         )
#         return_df = pd.DataFrame(return_list, columns=["return"])
#         bad_response_df = return_df.loc[~return_df["return"].isin([True, False])]
#         collection_list = bad_response_df["return"].tolist()
#         sc.stop()
#         spark.stop()


def collect_generic_ticker(
    ticker: str,
    add_ticker: bool,
    url: str,
    dtypes: dict,
    year: float = None,
    end_date: str = None,
):
    """
    General FMP collection for tickers.

    Inputs:
        - ticker: ticker symbol
        - add_ticker: Indicates if ticker is missing from response
        - url: Base URL for API
        - dl_ticker_dir: Data lake directory for ticker data
        - dtypes: Data types for formatting DataFrame
        - year: Specific year for data collection, if applicable
    """
    if year:
        final_url = url.format(TICKER=ticker, YEAR=year, API=FMP_KEY)
    elif end_date:
        start_date = end_date - dt.timedelta(2)
        final_url = url.format(
            TICKER=ticker, START_DATE=start_date, END_DATE=end_date, API=FMP_KEY
        )
    else:
        final_url = url.format(TICKER=ticker, API=FMP_KEY)

    response = requests.get(final_url)
    if response.status_code == 429:
        return "429"  # Throttled
    data = response.json()
    if not data:
        return False

    df = pd.DataFrame(data)
    if add_ticker:
        df["symbol"] = ticker
    df_typed = utils.format_data(df, dtypes)
    return df_typed


def collect_generic_distributed(
    distribute_through: Callable,
    spark_app: str,
    dl_ticker_dir: str,
    batch_size: int = 25,
    **kwargs,
):
    """
    General FMP collection distribute through Spark, managing batch processing and optimized file handling.

    Inputs:
        - distribute_through: Function to distribute collection tasks
        - spark_app: Name of the Spark app
        - dl_ticker_dir: Directory to save the collected data
        - kwargs: Additional keyword arguments for the distribution function
    """
    if spark_app == "mxm-collection":
        ## only collect options data to keep this low on compute for now
        collection_list = deque(utils.get_distinct_watchlist_symbols('Options Watchlist'))
    else:
        collection_list = deque(utils.get_distinct_tickers())

    # Initialize Spark session and context once
    spark = (
        SparkSession.builder.appName(f"collect-{spark_app}")
        .config("spark.driver.host", os.environ["SPARK_LOCAL_IP"])
        .config("spark.ui.port", "4050")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    sc = spark.sparkContext

    while collection_list:
        current_batch = [
            collection_list.popleft()
            for _ in range(min(batch_size, len(collection_list)))
        ]
        distribution_list = [
            utils.make_input("ticker", t, kwargs) for t in current_batch
        ]
        return_list = (
            sc.parallelize(distribution_list)
            .map(lambda r: distribute_through(**r))
            .collect()
        )

        # Process responses and write data
        to_retry = []
        for result in return_list:
            if isinstance(result, pd.DataFrame):
                ticker = result["symbol"].iloc[0]
                file_path = os.path.join(dl_ticker_dir, f"{ticker}.parquet")
                write_data_to_parquet(result, file_path)
            elif result == "429":
                index = return_list.index(result)
                to_retry.append(current_batch[index])

        # Re-queue the tickers that need retry
        collection_list.extend(to_retry)

    # Clean up Spark session
    sc.stop()
    spark.stop()


def write_data_to_parquet(df, filepath):
    """
    Writes DataFrame to a Parquet file, appending and deduplicating if file exists.

    Args:
        df (DataFrame): Data to write.
        filepath (str): File path to write the data to.
    """
    if os.path.isfile(filepath):
        old_df = pd.read_parquet(filepath)
        df = pd.concat([df, old_df]).drop_duplicates(ignore_index=True)
    df.to_parquet(filepath, index=False)


def distribute_function(method: Callable, spark_app: str):
    distribution_list = utils.get_watchlist(extend=True)

    spark = SparkSession.builder.appName(f"parallelize-{spark_app}").getOrCreate()
    sc = spark.sparkContext
    return_list = sc.parallelize(distribution_list).map(lambda r: method(r)).collect()
