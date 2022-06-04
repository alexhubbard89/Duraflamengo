import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
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

# sm_data_lake_dir = Variable.get("sm_data_lake_dir")
sm_data_lake_dir = "/Users/alexanderhubbard/stock-market/data"
TRENDING_WRITE_BUFFER = sm_data_lake_dir + "/buffer/yahoo-trending/"
DL_WRITE_DIR = sm_data_lake_dir + "/{subdir}/{date}/"

## functions
def get_trending_collected():
    return [
        x.split("/")[-1].split(".")[0] for x in glob.glob(TRENDING_WRITE_BUFFER + "*")
    ]


def get_yahoo_trending():
    now = datetime.now(pytz.timezone("America/New_York"))
    now_time = str(now.time()).split(".")[0]
    if utils.is_between(now_time, ("09:30", "16:30")) == False:
        print("\n\n\nDO NOT COLLECT\n\n\n")
        ## DO NOT COLLECT
        ## Bc its currently outside of market hours
        return False

    url = "https://finance.yahoo.com/trending-tickers/"
    spoof = Ip_Spoofer()
    page = Ip_Spoofer.request_page(spoof, url)
    cols = [
        x.text.replace("(", "")
        .replace(")", "")
        .replace("%", "percent")
        .replace(" ", "_")
        .replace("/", "_")
        .lower()
        for x in page.find("table").find("tr")
    ]
    df = pd.DataFrame(columns=cols)
    for tr in page.find("tbody").findAll("tr"):
        df = df.append(
            pd.DataFrame(
                [[x.text.replace("%", "") for x in tr.findAll("td")]], columns=cols
            )
        ).reset_index(drop=True)

    df["created_at"] = now
    df["date"] = now.date()

    ## write to buffer
    in_buffer = len(get_trending_collected())
    _ = df.to_csv(TRENDING_WRITE_BUFFER + "file_{}.csv".format(in_buffer), index=False)
    return True


def migrate_trending():
    now = datetime.now(pytz.timezone("America/New_York"))
    now_time = str(now.time()).split(".")[0]
    if utils.is_between(now_time, ("09:30", "16:30")) == False:
        print("\n\n\nDO NOT MIGRATE\n\n\n")
        ## DO NOT MIGRATE
        ## Bc its currently outside of market hours
        return False

    ## start spark session
    spark = SparkSession.builder.appName("migrate-yahoo-trending").getOrCreate()
    try:
        ## get data to migrate
        collected_trending_df = (
            spark.read.format("csv")
            .options(header="true")
            .load(TRENDING_WRITE_BUFFER + "*")
        )
        today = datetime.now(pytz.timezone("America/New_York")).date()
        _ = utils.write_spark(spark, collected_trending_df, "yahoo-trending", today)
    except:
        pass  ## bc empty - dirs done purposefully
    return True


def clear_yahoo_trending_buffer():
    """ "
    Custom buffer clear.
    Check if it is currently trading time.
    If it is, do not clear.
    If it is outside of trading hours, clear buffer.
    """
    now = datetime.now(pytz.timezone("America/New_York"))
    now_time = str(now.time()).split(".")[0]
    if utils.is_between(now_time, ("09:30", "16:30")) == True:
        print("\n\n\nDO NOT CLEAR BUFFER\n\n\n")
        ## DO NOT CLEAR BUFFER
        ## Bc its currently market hours
        return False
    utils.clear_buffer(subdir="yahoo-trending")
    return True
