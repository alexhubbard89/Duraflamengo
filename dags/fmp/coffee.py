## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

import glob
import pandas as pd
import numpy as np
import datetime as dt
import requests
import os
import common.scripts.settings as s
import common.scripts.utils as utils
from io import StringIO

FMP_KEY = os.environ["FMP_KEY"]


def fmp_collector(start: dt.date, end: dt.date, url: str, dl_path: str):
    """
    Collect data from the Financial Modeling Prep API
    and write as parquet to data-lake.
    """
    r = requests.get(url.format(DSS=start, DSE=end, API=FMP_KEY))
    df = pd.DataFrame(r.json())
    if len(df) == 0:
        return False
    df.to_parquet(dl_path + f"/{start}.parquet")
    df.to_parquet(dl_path + f"/latest.parquet")
    return True


def collect_calendar_data(ds, ds_delta):
    """
    Collect all data from the group of calendar APIs.
    Date is converted for airflow.
    """
    ds = pd.to_datetime(ds).date()
    ds_end = ds + dt.timedelta(ds_delta)
    return [fmp_collector(ds, ds_end, r[0], r[1]) for r in s.calendar_collection_list]


def collect_delisted():
    """
    This is everything. Use the single file and filter
    if you need a point in time, downstream.
    """
    collect = True
    page_num = 0
    dfs = []
    while collect:
        url = s.DELISTED_COMPANIES.format(PAGE=page_num, API=FMP_KEY)
        r = requests.get(url)
        data = r.json()
        if len(data) == 0:
            collect = False
        else:
            page_num += 1
            dfs.append(pd.DataFrame(data))
    df = pd.concat(dfs, ignore_index=True)
    df["ipoDate"] = pd.to_datetime(df["ipoDate"]).apply(lambda r: r.date())
    df["delistedDate"] = pd.to_datetime(df["delistedDate"]).apply(lambda r: r.date())
    df.to_parquet(s.delisted_companies)


def get_full_ip():
    """
    Concat all IPOs to one dataframe.
    """
    spark = SparkSession.builder.appName("get-all-ipo").getOrCreate()
    df = (
        spark.read.format("parquet")
        .option("path", s.ipo_calendar_confirmed + "/*.parquet")
        .load()
        .toPandas()
        .drop_duplicates()
    )
    spark.stop()
    return df


def make_collection_list(ds: dt.date):
    """
    Find tickers to collect data for a given date.
    Use the base NYSE dataset, add IPOs, remove
    companies that have delisted.
    Date is converted for airflow.

    Inputs: Date to collect
    """
    ds = pd.to_datetime(ds).date()
    ## load
    ipo_df = get_full_ip()[["symbol"]].rename(columns={"symbol": "Symbol"})
    ticker_df = pd.read_csv(s.BASE_TICKER_FN)[["Symbol"]]
    delisted_df = pd.read_parquet(s.delisted_companies)
    to_collect_df = pd.concat([ticker_df, ipo_df])

    ## subset
    delisted_tickers = delisted_df.loc[delisted_df["delistedDate"] <= ds]
    to_collect_df = to_collect_df.loc[~to_collect_df["Symbol"].isin(delisted_tickers)]

    ## clean
    to_collect_df = to_collect_df.loc[
        ~to_collect_df["Symbol"]
        .apply(lambda r: r.replace(".", "__").replace("^", "__"))
        .str.contains("__")
    ]
    to_collect_df.to_parquet(s.to_collect + f"/{ds}.parquet")
