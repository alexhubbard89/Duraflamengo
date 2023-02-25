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
import glob
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


def collect_generic_ticker(
    ds: dt.date,
    ticker: str,
    add_ticker: bool,
    url: str,
    buffer_dir: str,
    dl_ticker_dir: str,
    dtypes: dict,
    date_col: str = "date",
    has_date: bool = True,
):
    """
    General FMP collection for tickers.

    Inputs:
        - ds: Date
        - ticker: ticker
        - add_ticker: Is ticker missing from response
        - url: Base url
        - buffer_dir: Buffer location
        - dl_ticker_dir: Ticker oriented data-lake location
        - dtypes: Data types
        - date_col: Name of date column
    """
    r = requests.get(url.format(TICKER=ticker, API=FMP_KEY))
    if r.status_code == 429:
        return ticker
    data = r.json()
    if len(data) == 0:
        return False
    df = pd.DataFrame(pd.DataFrame(data))
    if add_ticker:
        df["symbol"] = ticker
    df_typed = utils.format_data(df, dtypes)
    if has_date:
        df_typed_end = df_typed.loc[
            pd.to_datetime(df_typed[date_col]).apply(lambda r: r.date()) == ds
        ].copy()
    else:
        df_typed_end = df_typed.copy()
    ## save
    if len(df_typed_end) > 0:
        ## slice for grades results in no data sometimes
        df_typed_end.to_parquet(buffer_dir + f"/{ds}/{ticker}.parquet", index=False)
    df_typed.to_parquet(dl_ticker_dir + f"/{ticker}.parquet", index=False)
    return True


def collect_generic_page(
    ds: dt.date,
    ticker: str,
    add_ticker: bool,
    url: str,
    buffer_dir: str,
    dl_ticker_dir: str,
    dtypes: dict,
    date_col: str = "date",
):
    """Iterage page count until no more data returns."""
    dfs = []
    collect = True
    page = 0
    while collect:
        url_ = url.format(TICKER=ticker, API=FMP_KEY, PAGE=page)
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
    df_typed_end = df_typed.loc[
        pd.to_datetime(df_typed[date_col]).apply(lambda r: r.date()) == ds
    ].copy()
    if len(df_typed_end) > 0:
        df_typed_end.to_parquet(buffer_dir + f"/{ds}/{ticker}.parquet", index=False)
    fn = dl_ticker_dir + f"/{ticker}.parquet"
    if os.path.isfile(fn):
        ## not all data come back fill full history
        old_df = pd.read_parquet(fn)
        df_typed = df_typed.append(old_df).drop_duplicates()
    df_typed.to_parquet(fn, index=False)
    return True


def collect_generic_distributed(
    get_distribution_list: Callable,
    dl_loc: str,
    buffer_loc: str,
    distribute_through: Callable,
    spark_app: str,
    **kwargs,
):
    """
    General FMP collection distribute through spark.

    Inputs:
        - get_distribution_list: Function to get list to distribute.
        - dl_loc: Final output location.
        - buffer_loc: Buffer to get list to distribute.
        - distribute_through: Function to to distribute.
        - spark_app: Name of the spark app.
        - kwargs: Arguments for distribution function.
    """
    ## collect all
    ds = pd.to_datetime([x for x in os.walk(buffer_loc + "/")][0][1][0]).date()
    kwargs["ds"] = ds
    if get_distribution_list == utils.get_to_collect:
        collection_list = get_distribution_list(buffer_loc)
    elif get_distribution_list == tda.get_option_collection_list:
        collection_list = get_distribution_list(ds)
    elif get_distribution_list == utils.get_watchlist:
        collection_list = get_distribution_list(ds)
    while len(collection_list) > 0:
        distribution_list = [
            utils.make_input("ticker", t, kwargs) for t in collection_list
        ]
        spark = SparkSession.builder.appName(f"collect-{spark_app}").getOrCreate()
        sc = spark.sparkContext
        return_list = (
            sc.parallelize(distribution_list)
            .map(lambda r: distribute_through(**r))
            .collect()
        )
        return_df = pd.DataFrame(return_list, columns=["return"])
        bad_response_df = return_df.loc[~return_df["return"].isin([True, False])]
        collection_list = bad_response_df["return"].tolist()
        sc.stop()
        spark.stop()

    if len(glob.glob(buffer_loc + f"/{ds}/*.parquet")) > 0:
        ## migrate from buffer to data-lake
        spark = SparkSession.builder.appName(f"coalesce-{spark_app}").getOrCreate()
        (
            spark.read.format("parquet")
            .option("path", buffer_loc + f"/{ds}/*.parquet")
            .load()
            .write.format("parquet")
            .mode("overwrite")
            .save(dl_loc + f"/{ds}", header=True)
        )
        spark.stop()
    utils.clear_buffer(buffer_loc.split("/data/buffer")[1])
