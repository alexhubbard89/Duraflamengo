import pandas as pd
import datetime as dt
import requests
from pyspark.sql import SparkSession
import common.utils as utils
import tda.settings as tda_s
import os
from time import sleep


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
    r = requests.get(tda_s.ANALYTICAL_OPTIONS_URL.format(ticker=ticker))
    if r.status_code != 200:
        return False
    if not (
        ("callExpDateMap" in r.json().keys()) & ("putExpDateMap" in r.json().keys())
    ):
        return ticker
    ## extract calls
    calls_dfs = []
    json_data = r.json()
    call_dict = json_data["callExpDateMap"]
    for option_date in call_dict.keys():
        calls_dfs.append(
            pd.concat(
                [
                    pd.DataFrame(call_dict[option_date][x])
                    for x in call_dict[option_date].keys()
                ]
            )
        )
    if len(calls_dfs) > 0:
        ## unpack
        calls_df = pd.concat(calls_dfs, ignore_index=True)
        calls_df["collected"] = dts
        calls_df["interestRate"] = json_data["interestRate"]
        calls_df["underlyingPrice"] = json_data["underlyingPrice"]
        calls_df["top_level_volatility"] = json_data["volatility"]
        ## append files
        calls_fn = f"{tda_s.OPTIONS_ANALYTICAL_NEW}/{ds}/{ticker}_calls.parquet"
        calls_df.append(utils.read_protect_parquet(calls_fn))
        ## format data
        calls_df = utils.format_data(calls_df, tda_s.options_types)
        ## write data
        if os.path.isfile(calls_fn):
            ## put the dataset together
            old_df = pd.read_parquet(calls_fn)
            calls_df = calls_df.append(old_df).drop_duplicates()
        calls_df.to_parquet(calls_fn, index=False)
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
    if len(puts_dfs) > 0:
        ## unpack
        puts_df = pd.concat(puts_dfs, ignore_index=True)
        puts_df["collected"] = dts
        puts_df["interestRate"] = json_data["interestRate"]
        puts_df["underlyingPrice"] = json_data["underlyingPrice"]
        puts_df["top_level_volatility"] = json_data["volatility"]
        ## append files
        puts_fn = f"{tda_s.OPTIONS_ANALYTICAL_NEW}/{ds}/{ticker}_puts.parquet"
        puts_df.append(utils.read_protect_parquet(puts_fn))
        ## format data
        puts_df = utils.format_data(puts_df, tda_s.options_types)
        ## write data
        if os.path.isfile(puts_fn):
            ## put the dataset together
            old_df = pd.read_parquet(puts_fn)
            puts_df = puts_df.append(old_df).drop_duplicates()

        puts_df.to_parquet(puts_fn, index=False)
    return ticker


def distribute_options(ds: dt.date):
    """
    Get options collection list for a given day.
    Collect current options in spark.

    Inputs:
        - ds: Date of discovery list.
    """
    ds = pd.to_datetime(ds).date()
    ## make directory, it not exist
    path = f"{tda_s.OPTIONS_ANALYTICAL_NEW}/{ds}"
    if not os.path.isdir(path):
        os.mkdir(path)

    collection_list = utils.get_watchlist(extend=True)
    count = 0
    while len(collection_list) > 0:
        now = dt.datetime.now()
        print("List size {}".format(len(collection_list)))
        # return_list = [options(ticker) for ticker in collection_list[:250]]

        ## no spark for now
        spark = SparkSession.builder.appName(
            f"tda-analytical-option-collect"
        ).getOrCreate()
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
        count += 1
        if count > 10:
            collection_list = []
        # if len(collection_list) > 0:
        #     sleep_time = 60 - (dt.datetime.now() - now).seconds
        #     if sleep_time > 0:
        #         sleep(sleep_time)
