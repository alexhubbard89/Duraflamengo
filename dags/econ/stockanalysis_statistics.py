import pandas as pd
import numpy as np
import datetime as dt
import pytz
import glob
import os

## spark
from pyspark.sql import SparkSession
from pyspark import SparkContext

## Local code
import common.utils as utils
from common.spoof import Ip_Spoofer

from airflow.models import Variable

sm_data_lake_dir = Variable.get("sm_data_lake_dir")
MB_BUFFER = sm_data_lake_dir + "/buffer/marketbeat-earnings"
BUFFER = sm_data_lake_dir + "/buffer/stock-analysis"
WRITE_PATH = sm_data_lake_dir + "/stock-analysis"
SA_URL = "https://stockanalysis.com/stocks/{TICKER}/statistics/"

""""
Get company financial data from stockanalysis.com
"""


def get_profile(ticker: str) -> bool:
    """
    Collect the measurements from market
    watch's company profile page.
    Save data to buffer.
    """
    fn = BUFFER + "/profile/{}.csv".format(ticker)
    url = SA_URL.format(TICKER=ticker)
    spoofer_obj = Ip_Spoofer()
    page = spoofer_obj.request_page(url)
    try:
        ## find and grab data
        tables = page.findAll("tbody")
        data_dict = dict()
        for tbody in tables:
            for tr in tbody.findAll("tr"):
                td = tr.findAll("td")
                col = (
                    td[0]
                    .text.lower()
                    .replace("-", "_")
                    .replace(" / ", "_")
                    .replace("%", "percent")
                    .replace(" ", "_")
                    .strip(" ")
                )
                _ = data_dict.update({col: td[1].text})
        df = pd.DataFrame([data_dict])
        df["ticker"] = ticker
        _ = df.to_csv(fn, index=False)
        return True
    except:
        df = pd.DataFrame([ticker], columns=["ticker"])
        _ = df.to_csv(fn, index=False)
        return False


def distribut_collection() -> bool:
    """
    Use spark to distribute the data collection.
    Data to collect is found from tickers that have
    upcoming earnings reports, according to
    market beat.
    """
    ##### Get tickers to collect
    ue_df = pd.read_csv(MB_BUFFER + "/to-collect/data.csv")
    ticker_list = ue_df["ticker"].tolist()
    buffer_tmp = BUFFER + "/profile/*"
    collected_list = [x.split("/")[-1].split(".csv")[0] for x in glob.glob(buffer_tmp)]
    tickers_left = list(set(ticker_list) - set(collected_list))
    ## start spark session
    spark = SparkSession.builder.appName(
        "stockanalysis-profile-collection"
    ).getOrCreate()
    sc = spark.sparkContext
    ## distribute requests
    collected_results_list = (
        sc.parallelize(tickers_left).map(lambda t: get_profile(t)).collect()
    )
    sc.stop()
    spark.stop()
    return True


def migrate_profile(ticker: str) -> bool:
    """
    Migrate company profile metrics from buffer
    to the data-lake. If I have collected
    data, then append. If not, write
    the new buffer data.

    Input: Ticker.
    Return: Success status.
    """
    b_fn = BUFFER + "/profile/{}.csv".format(ticker)
    dl_fn = WRITE_PATH + "/profile/{}.csv".format(ticker)
    if os.path.isfile(dl_fn):
        df = (
            pd.read_csv(b_fn)
            .append(pd.read_csv(dl_fn))
            .drop_duplicates()
            .reset_index(drop=True)
        )
    else:
        df = pd.read_csv(b_fn)
    _ = df.to_csv(dl_fn, index=False)
    return True


def distribute_migration() -> bool:
    """
    Distribute the migration process.
    """
    ##### Get tickers to migrate
    ue_df = pd.read_csv(MB_BUFFER + "/to-collect/data.csv")
    ticker_list = ue_df["ticker"].tolist()
    migrated_list = [
        x.split("/")[-1].split(".csv")[0] for x in glob.glob(WRITE_PATH + "/profile/*")
    ]
    tickers_left = list(set(ticker_list) - set(migrated_list))
    ## start spark session
    spark = SparkSession.builder.appName(
        "stockanalysis-profile-migration"
    ).getOrCreate()
    sc = spark.sparkContext
    ## distribute migration
    collected_results_list = (
        sc.parallelize(tickers_left).map(lambda t: migrate_profile(t)).collect()
    )
    sc.stop()
    spark.stop()
    return True


def get_SA_financials(ticker: str, fin_type: str) -> bool:
    """
    Use Stockanalysis.com to get company financials.
    Both quarterly and annual data will be captured.
    Acceptable financial types:
        - income: NA
        - balance sheet: balance-sheet
        - cash flow statement: cash-flow-statement
        - ratios: ratios

    I've wrapped a try except for this early stage.
    There are times when the arrays have different
    shapes. Since I don't know why it's happening
    and it's only one instance, I am punting.

    Inputs:
        - Ticker
        - Type of financial
    Return
        - Bool to signify success/fail.
    """
    if fin_type == "income_statement":
        url = "https://stockanalysis.com/stocks/{}/financials/".format(ticker)
        fin_dir = "income"
    elif fin_type == "balance_sheet":
        url = "https://stockanalysis.com/stocks/{}/financials/balance-sheet/".format(
            ticker
        )
        fin_dir = "balance-sheet"
    elif fin_type == "cash_flow_statement":
        url = "https://stockanalysis.com/stocks/{}/financials/cash-flow-statement/".format(
            ticker
        )
        fin_dir = "cash-flow"
    elif fin_type == "ratios":
        url = "https://stockanalysis.com/stocks/{}/financials/ratios/".format(ticker)
        fin_dir = "ratios"

    for fin_range in ["quarterly", "annual"]:
        spoofer_obj = Ip_Spoofer()
        page = spoofer_obj.request_page(url + "&range=" + fin_range)
        if page == "Status: 404":
            continue
        page_table = page.find("table")
        if page_table == None:
            continue
        cols = [th.text for th in page_table.find("thead").findAll("th")]
        data = []
        for tr in page_table.find("tbody").findAll("tr"):
            _ = data.append([td.text for td in tr.findAll("td")])
        df = pd.DataFrame(data, columns=cols)

        if len(df) > 0:
            df["ticker"] = ticker
            _ = df.to_csv(
                BUFFER + "/{}/{}/{}.csv".format(fin_range, fin_dir, ticker), index=False
            )
    return True


def migrate_financials(ticker: str) -> bool:
    """
    Migrate company financial metrics from buffer
    to the data-lake. If I have collected
    data, then append. If not, write
    the new buffer data.

    Input: Ticker.
    Return: Success status.
    """
    for fin_dir in ["income", "balance-sheet", "cash-flow", "ratios"]:
        for report in ["quarterly", "annual"]:
            b_fn = BUFFER + "/{}/{}/{}.csv".format(report, fin_dir, ticker)
            dl_fn = WRITE_PATH + "/{}/{}/{}.csv".format(report, fin_dir, ticker)
            try:
                if (os.path.isfile(dl_fn)) & (os.path.isfile(b_fn)):
                    df = (
                        pd.read_csv(b_fn)
                        .append(pd.read_csv(dl_fn))
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                elif os.path.isfile(b_fn):
                    df = pd.read_csv(b_fn)
                else:
                    return False  ## no data
                _ = df.to_csv(dl_fn, index=False)
            except:
                return False
    return True


def distribute_financials_migration() -> bool:
    """
    Distribute the migration process.
    """
    ##### Get tickers to migrate
    collected_list = []
    for fin_dir in ["income", "balance-sheet", "cash-flow", "ratios"]:
        for report in ["quarterly", "annual"]:
            ##### Get tickers to collect
            buffer_tmp = BUFFER + "/{}/{}/*".format(report, fin_dir)
            collected_list_tmp = [
                x.split("/")[-1].split(".csv")[0] for x in glob.glob(buffer_tmp)
            ]
            _ = collected_list.extend(collected_list_tmp)
            collected_list = list(set(collected_list))
    ## start spark session
    spark = SparkSession.builder.appName(
        "stockanalysis-financials-migration"
    ).getOrCreate()
    sc = spark.sparkContext
    ## distribute migration
    ## make requests
    migrateded_results_list = (
        sc.parallelize(collected_list).map(lambda t: migrate_financials(t)).collect()
    )
    sc.stop()
    spark.stop()
    return True
