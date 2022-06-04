import pandas as pd
import datetime as dt
import fmp.settings as s
import common.utils as utils
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def make_daily_stream(ds: dt.date, ticker_dir: str, daily_dir: str, params: dict):
    """
    Read all ticker files, slice for a given date
    and turn into a daily stream.
    This will be used to backfill each stream.
    """
    df = utils.distribute_read_many_parquet(ds, ticker_dir, params)
    if len(df) == 0:
        return False
    spark = SparkSession.builder.appName("write-file").getOrCreate()
    daily_sub_dir = daily_dir.split("/data/")[1]
    utils.write_spark(spark, df, daily_sub_dir, ds, "parquet")
    spark.stop()
    return True
