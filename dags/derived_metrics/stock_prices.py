import pandas as pd
import datetime as dt
import glob

## spark
from pyspark.sql import SparkSession
import pyspark.sql.types as T

## Local code
import common.utils as utils
import derived_metrics.settings as s


def prepare_pv() -> bool:
    """
    Use spark to read and filter needed data.
    Write to buffer.

    Since the data lake is so large I'm hitting
    memory issues. This process finds the dates
    necessary for analysis, copies the contents
    to a buffer, prepares the data, then clears
    the migrated buffer.

    Inputs:
        - Date is the last day of the analysis
        - Days is the lookback window
    """
    ## load and migrate all data
    spark = SparkSession.builder.appName("prepare-price-vol-avg").getOrCreate()
    schema = T.StructType(
        [
            T.StructField("open", T.FloatType(), True),
            T.StructField("high", T.FloatType(), True),
            T.StructField("low", T.FloatType(), True),
            T.StructField("close", T.FloatType(), True),
            T.StructField("volume", T.IntegerType(), True),
            T.StructField("date", T.DateType(), True),
            T.StructField("ticker", T.StringType(), True),
        ]
    )
    price_df = (
        spark.read.format("orc")
        .option("path", s.stock_avg_buffer + "/raw/*")
        .schema(schema)
        .load()
        .orderBy(["ticker", "date"])
        .repartition(1)  ## do not partition
        .write.format("csv")
        .mode("overwrite")
        .save(s.stock_avg_buffer + "/prepared/", header=True)
    )
    _ = spark.stop()
    return True


def make_pv(ds: dt.date, yesterday: bool = True) -> bool:
    """
    Make and write dialy price and volume
    averages to the data lake.
    Lookback window is hardcoded to reflect
    typical averaging.

    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    ## move the files to the buffer
    op_kwargs = {
        "data_loc": s.price_dir,
        "date": ds,
        "days": 400,
        "buffer_loc": s.stock_avg_buffer + "/raw",
    }
    utils.move_files(**op_kwargs)
    ## bulk load all parquet from buffer and clear buffer
    ## reading all at once causes an error, hence the loop.
    dfs = []
    for fn in glob.glob(s.stock_avg_buffer + "/raw/*"):
        dfs.append(pd.read_parquet(fn))
    price_df = pd.concat(dfs, ignore_index=True)
    utils.clear_buffer((s.stock_avg_buffer + "/raw").split("data/buffer/")[1])
    date_base = pd.DataFrame(
        [
            x.date()
            for x in pd.date_range(price_df["date"].min(), price_df["date"].max())
        ],
        columns=["date"],
    )
    date_base = date_base.loc[date_base["date"].apply(lambda r: r.weekday() < 5)]
    price_df = date_base.merge(price_df, how="left", on="date")
    price_df["close"] = price_df["close"].fillna(method="ffill")
    ## make avg price
    price_avg_5 = (
        price_df.groupby("symbol")["close"]
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "close": "avg_price_5"})
        .set_index("index")
        .drop("symbol", 1)
    )
    price_avg_10 = (
        price_df.groupby("symbol")["close"]
        .rolling(10)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "close": "avg_price_10"})
        .set_index("index")
        .drop("symbol", 1)
    )
    price_avg_50 = (
        price_df.groupby("symbol")["close"]
        .rolling(50)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "close": "avg_price_50"})
        .set_index("index")
        .drop("symbol", 1)
    )
    price_avg_200 = (
        price_df.groupby("symbol")["close"]
        .rolling(200, min_periods=1)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "close": "avg_price_200"})
        .set_index("index")
        .drop("symbol", 1)
    )
    ## make avg vol
    volume_avg_5 = (
        price_df.groupby("symbol")["volume"]
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "volume": "avg_volume_5"})
        .set_index("index")
        .drop("symbol", 1)
    )
    volume_avg_10 = (
        price_df.groupby("symbol")["volume"]
        .rolling(10)
        .mean()
        .reset_index()
        .rename(columns={"level_1": "index", "volume": "avg_volume_10"})
        .set_index("index")
        .drop("symbol", 1)
    )
    ## join and subset columns
    price_df_full = (
        price_df.join(price_avg_5)
        .join(price_avg_10)
        .join(price_avg_50)
        .join(price_avg_200)
        .join(volume_avg_5)
        .join(volume_avg_10)[
            [
                "symbol",
                "date",
                "close",
                "avg_price_5",
                "avg_price_10",
                "avg_price_50",
                "avg_price_200",
                "avg_volume_5",
                "avg_volume_10",
            ]
        ]
    )
    ## subset to analysis date
    price_df_subset = price_df_full.loc[price_df_full["date"] == ds]
    if len(price_df_subset) > 0:
        fn = f"{s.avg_price}/{ds}.parquet"
        price_df_subset.to_parquet(fn, index=False)
    return True


def migrate_pv():
    """
    Migrate analyst target price data.
    Use file name as partition.
    """
    ## subset
    file_list = glob.glob(s.stock_avg_buffer + "/finished/" + "*.csv")
    if len(file_list) == 0:
        return False
    price_df = utils.read_many_csv(s.stock_avg_buffer + "/finished/")
    date = pd.to_datetime(
        glob.glob(s.stock_avg_buffer + "/finished/*.csv")[0]
        .split("/")[-1]
        .split("T")[0]
        .split(".csv")[0]
    ).date()
    date_cols = ["date"]
    for col in date_cols:
        price_df[col] = pd.to_datetime(price_df[col]).apply(lambda r: r.date())
    ## migrate
    ## start spark session
    spark = SparkSession.builder.appName("migrate-price-vol-avg").getOrCreate()
    sub_dir = s.avg_price.split("/data/")[1]
    utils.write_spark(spark, price_df, sub_dir, date)
    spark.stop()
    return True
