import pandas as pd
import numpy as np
import datetime as dt
import string
import pytz
import glob
from scipy import stats
import pandas_market_calendars as mcal

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf

## Local code
import common.utils as utils

## locations
from airflow.models import Variable

sm_data_lake_dir = Variable.get("sm_data_lake_dir")
MW_FIN_TRENDS_PATH = (
    sm_data_lake_dir + "/marketwatch-financials/{FIN_TYPE}/{TICKER}.csv"
)
INCOME_BUFFER = sm_data_lake_dir + "/buffer/marketwatch-financials-trends/income/"
BALANCE_SHEET_BUFFER = (
    sm_data_lake_dir + "/buffer/marketwatch-financials-trends/balance-sheet/"
)
CASH_FLOW_BUFFER = sm_data_lake_dir + "/buffer/marketwatch-financials-trends/cash-flow/"


def make_financial_trends(ticker, fin_type):
    """
    Make revenue stats and save to buffer.
    """
    if fin_type == "income":
        buffer = INCOME_BUFFER
    elif fin_type == "balance-sheet":
        buffer = BALANCE_SHEET_BUFFER
    elif fin_type == "cash-flow":
        buffer = CASH_FLOW_BUFFER
    ## get data
    f_path = MW_FIN_TRENDS_PATH.format(FIN_TYPE=fin_type, TICKER=ticker)
    try:
        df_raw = pd.read_csv(f_path)
        ## clean
        drop_cols = [x for x in df_raw.columns if "growth" in x] + [
            "ticker",
            "date",
            "collection_date",
        ]
        cols = df_raw.drop(drop_cols, 1).columns
        clean_cols_dict = dict()
        for col in cols:
            length = len(col)
            col_cleaned = col[: int(length / 2)]
            clean_cols_dict[col] = col_cleaned.replace("-", "_").replace(".", "")
        ## subset
        df = df_raw.rename(columns=clean_cols_dict)[
            list(clean_cols_dict.values()) + ["ticker", "date", "collection_date"]
        ]
        del df_raw
    except:
        ## no data for ticker
        return False

    if len(df) < 3:
        return False
    ## type and sort
    df["date"] = pd.to_datetime(df["date"]).apply(lambda r: r.date())
    df["release_date"] = pd.to_datetime(df["date"]).apply(lambda r: r.date())
    df = df.sort_values("date", ascending=True).reset_index(drop=True)
    ## add slope
    income_metric_cols = df.drop(
        ["ticker", "date", "release_date", "collection_date"], 1
    ).columns
    for metric in income_metric_cols:
        x = [_ for _ in df.index]
        y = df[metric]
        for i in range(3, len(x) + 1):
            x_sub = x[i - 3 : i]
            y_sub = y[i - 3 : i]
            starting_value = y_sub.tolist()[0]
            y_sub_norm = y_sub / starting_value
            slope = stats.linregress(x_sub, y_sub_norm).slope
            df.loc[i - 1, "{}_slope".format(metric)] = slope
        ## growth rate
        df["{}_slope_dx".format(metric)] = df["{}_slope".format(metric)].diff()

    ## Doing the forward fill up front is better.
    ## It is used many times downstream.
    ## Doing it once at the root is the quickest for
    ## overall processing.
    ## forward fill based on market open days
    nyse = mcal.get_calendar("NYSE")
    nyse_cal = nyse.schedule(start_date=df["date"].min(), end_date=dt.datetime.today())
    nyse_date_list = [x.date() for x in pd.to_datetime(nyse_cal["market_open"])]
    NYSE_DATE_BASE = pd.DataFrame(nyse_date_list, columns=["date"])

    ## financials could come out on a weekend
    full_date_list = [
        x.date() for x in pd.date_range(df["date"].min(), dt.datetime.today())
    ]
    FULL_DATE_BASE = pd.DataFrame(full_date_list, columns=["date"])

    # join full, fill na, subset to market days
    df_full = (
        FULL_DATE_BASE.merge(df, how="left", on="date")
        .fillna(method="ffill")
        .merge(NYSE_DATE_BASE, how="right", on="date")
    )

    if len(df) > 0:
        _ = df_full.to_csv(buffer + "{}.csv".format(ticker), index=False)

    return True


def prepare_all_financials(fin_type):
    """
    Make income trends for all tickers.
    Use spark to distribute the work.
    """
    ## clear buffer
    utils.clear_buffer("marketwatch-financials-trends/{}".format(fin_type))

    buffer_path = MW_FIN_TRENDS_PATH.format(FIN_TYPE=fin_type, TICKER="split").split(
        "split"
    )[0]
    all_ticker_list = [
        x.split("/")[-1].split(".csv")[0] for x in glob.glob(buffer_path + "*")
    ]

    ## start spark session
    spark = SparkSession.builder.appName("{}-trends".format(fin_type)).getOrCreate()
    sc = spark.sparkContext

    ## distribute work
    return_list = (
        sc.parallelize(all_ticker_list)
        .map(lambda ticker: make_financial_trends(ticker, fin_type))
        .collect()
    )

    ## stop
    _ = spark.stop()
    _ = sc.stop()
    return True


def migrate_financials(fin_type):
    """
    Migrate financial trends.
    """
    if fin_type == "income":
        buffer = INCOME_BUFFER
    elif fin_type == "balance-sheet":
        buffer = BALANCE_SHEET_BUFFER
    elif fin_type == "cash-flow":
        buffer = CASH_FLOW_BUFFER

    ## determine the columns to use
    file_list = glob.glob(buffer + "*")
    all_cols = [list(pd.read_csv(x, nrows=1).columns) for x in file_list]
    flat_list = [item for sublist in all_cols for item in sublist]
    column_appear_df = pd.DataFrame(
        pd.Series(flat_list).value_counts(), columns=["count"]
    )
    column_appear_df["percent"] = column_appear_df["count"] / len(all_cols)
    use_columns = column_appear_df.loc[column_appear_df["percent"] >= 0.9].index
    ## get data
    all_data = utils.read_many_csv(buffer)[use_columns]

    ## type data
    all_data["ticker"] = all_data["ticker"].astype(str)
    for col in ["date", "collection_date", "release_date"]:
        all_data[col] = pd.to_datetime(all_data[col]).apply(lambda r: r.date())
    float_cols = list(
        set(use_columns) - set(["ticker", "date", "collection_date", "release_date"])
    )
    for col in float_cols:
        all_data[col] = all_data[col].astype(float)

    ## subset all data
    ## I only need back to the start of 2019 for
    ## daily stuff
    all_data = (
        all_data.loc[all_data["date"] >= dt.date(2019, 1, 1)]
        .copy()
        .reset_index(drop=True)
    )

    ## start spark session
    spark = SparkSession.builder.appName(
        "migrate-{}-financials".format(fin_type)
    ).getOrCreate()
    ## write each date
    for date in all_data["date"].unique():
        print(date)
        write_df = all_data.loc[all_data["date"] == date].reset_index(drop=True).copy()
        ## write to spark
        _ = utils.write_spark(
            spark, write_df, "/derived-measurements/{}-trends".format(fin_type), date
        )
        del write_df

    _ = spark.stop()

    ## clear buffer
    utils.clear_buffer("marketwatch-financials-trends/{}".format(fin_type))

    return True
