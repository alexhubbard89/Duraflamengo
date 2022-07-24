import pandas as pd
import numpy as np
import datetime as dt
import requests
from pyspark.sql import SparkSession
import common.utils as utils
import tda.settings as tda_s
import analysis.settings as analysis_s
import os


def options(ticker: str):
    """
    Collect current option contracts for a given ticker,
    separate into call and puts, and write to DL.
    If the request returns a 200 then return the
    ticker symbol to remove from collection list.

    Inputs: ticker to collect.
    Returs:
        - Bad response: False
        - Good repsonse: Ticker
    """
    dts = dt.datetime.now()
    ds = dts.date()
    ## request data
    r = requests.get(tda_s.OPTIONS_URL.format(API=tda_s.client_id, ticker=ticker))
    if r.status_code != 200:
        return False
    if not (
        ("callExpDateMap" in r.json().keys()) & ("putExpDateMap" in r.json().keys())
    ):
        return ticker
    ## extract calls
    calls_dfs = []
    call_dict = r.json()["callExpDateMap"]
    for option_date in call_dict.keys():
        calls_dfs.append(
            pd.concat(
                [
                    pd.DataFrame(call_dict[option_date][x])
                    for x in call_dict[option_date].keys()
                ]
            )
        )
    calls_df = pd.concat(calls_dfs, ignore_index=True)
    calls_df["collected"] = dts
    ## extract puts
    puts_dfs = []
    put_dict = r.json()["putExpDateMap"]
    for option_date in put_dict.keys():
        puts_dfs.append(
            pd.concat(
                [
                    pd.DataFrame(put_dict[option_date][x])
                    for x in put_dict[option_date].keys()
                ]
            )
        )
    puts_df = pd.concat(puts_dfs, ignore_index=True)
    puts_df["collected"] = dts
    ## append files
    calls_fn = f"{tda_s.OPTIONS}/{ds}/{ticker}_calls.parquet"
    puts_fn = f"{tda_s.OPTIONS}/{ds}/{ticker}_puts.parquet"
    calls_df.append(utils.read_protect_parquet(calls_fn))
    puts_df.append(utils.read_protect_parquet(puts_fn))
    ## format data
    calls_df = utils.format_data(calls_df, tda_s.options_types)
    puts_df = utils.format_data(puts_df, tda_s.options_types)
    ## write data
    calls_df.to_parquet(calls_fn, index=False)
    puts_df.to_parquet(puts_fn, index=False)
    return ticker


def get_option_collection_list(ds: dt.date = None) -> list:
    """
    Get list of tickers to collect for a given date
    of interest. The date refers to the asset metrics
    because options collection does not offer history.

    If no date exists, the grab the most recent date that does.

    I grab two dates for intraday and full metrics.

    Inputs:
        - ds: Date of discory list.
    Returns:
        - List to collect.
    """
    if ds == None:
        ds = dt.datetime.now().date()
    while not os.path.isfile(analysis_s.calls_discovery_dir + f"/{ds}.parquet"):
        ds = ds - dt.timedelta(1)
    yesterday = ds - dt.timedelta(1)
    while not os.path.isfile(analysis_s.calls_discovery_dir + f"/{yesterday}.parquet"):
        yesterday = yesterday - dt.timedelta(1)
    calls_df = pd.read_parquet(analysis_s.calls_discovery_dir + f"/{ds}.parquet")
    puts_df = pd.read_parquet(analysis_s.puts_discovery_dir + f"/{ds}.parquet")
    y_calls_df = pd.read_parquet(
        analysis_s.calls_discovery_dir + f"/{yesterday}.parquet"
    )
    y_puts_df = pd.read_parquet(analysis_s.puts_discovery_dir + f"/{yesterday}.parquet")
    return list(
        set(
            calls_df["symbol"].tolist()
            + puts_df["symbol"].tolist()
            + y_calls_df["symbol"].tolist()
            + y_puts_df["symbol"].tolist()
        )
    )


def distribute_options(ds: dt.date):
    """
    Get options collection list for a given day.
    Collect current options in spark.

    Inputs:
        - ds: Date of discory list.
    """

    path = f"{tda_s.OPTIONS}/{ds}"
    if not os.path.isdir(path):
        os.mkdir(path)
    collection_list = get_option_collection_list(ds)
    while len(collection_list) > 0:
        print("List size {}".format(len(collection_list)))
        spark = SparkSession.builder.appName(f"tda-collect-options").getOrCreate()
        sc = spark.sparkContext
        return_list = (
            sc.parallelize(collection_list[:250]).map(lambda r: options(r)).collect()
        )
        sc.stop()
        spark.stop()
        return_df = pd.DataFrame(return_list, columns=["return"])
        good_response_df = return_df.loc[~return_df["return"].isin([True, False])]
        collection_success = good_response_df["return"].tolist()
        collection_list = list(set(collection_list) - set(collection_success))
