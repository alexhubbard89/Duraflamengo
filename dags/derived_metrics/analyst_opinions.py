import pandas as pd
import numpy as np
import datetime as dt
import string
import pytz
import glob

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
AO_RATINGS = "/benzinga-rating-changes"
AO_BUFFER = sm_data_lake_dir + "/buffer/der-m-ao"
AO_SCORES_BUFFER = sm_data_lake_dir + "/buffer/der-m-ao-score"
AO_PT_BUFFER = sm_data_lake_dir + "/buffer/der-m-ao-pt"

## Map
rating_list = [
    "strong_buy",
    "buy",
    "overweight",
    "hold",
    "underweight",
    "sell",
    "strong_sell",
]
rating_map = {
    "buy": "buy",
    "neutral": "hold",
    "outperform": "overweight",
    "overweight": "overweight",
    "hold": "hold",
    "equal-weight": "hold",
    "market perform": "hold",
    "underweight": "underweight",
    "underperform": "underweight",
    "sell": "sell",
    "sector perform": "hold",
    "strong buy": "strong_buy",
    "market outperform": "overweight",
    "nan": None,
    "in-line": "hold",
    "sector weight": "hold",
    "positive": "overweight",
    "perform": "hold",
    "peer perform": "hold",
    "accumulate": "overweight",
    "reduce": "underweight",
    "sector outperform": "buy",
    "speculative buy": "overweight",
    "negative": "underweight",
    "fair value": "hold",
    "mixed": "hold",
    "top pick": "strong_buy",
    "market underperform": "hold",
    "long-term buy": "overweight",
    "outperformer": "overweight",
    "sector underperform": "underweight",
    "conviction buy": "overweight",
    "underperformer": "underweight",
    "market weight": "hold",
    "strong sell": "strong_sell",
    "not rated": None,
    "sector performer": "hold",
    "cautious": "hold",
    "trim": "sell",
    "add": "overweight",
    "below average": "underweight",
    "trading buy": "overweight",
    "hold neutral": "hold",
    "tender": "underweight",
    "gradually accumulate": "overweight",
    "above average": "overweight",
    "trading sell": "underweight",
    "equalweight": "hold",
    "sector overweight": "overweight",
}


def prepare_data() -> bool:
    """
    Use spark to read and filter needed data.
    Write to buffer.
    Inputs were removed to put into airflow.
    The input arg are used in the step before,
    and migrate raw files to the buffer.

    Inputs:
        - Date is the last day of the analysis
        - Days is the lookback window
    """
    ## start spark session
    spark = SparkSession.builder.appName("prepare-ao").getOrCreate()
    ## Set schema
    schema = T.StructType(
        [
            T.StructField("pt_prior", T.FloatType(), True),
            T.StructField("updated", T.LongType(), True),
            T.StructField("url_news", T.StringType(), True),
            T.StructField("analyst", T.StringType(), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("ticker", T.StringType(), True),
            T.StructField("exchange", T.StringType(), True),
            T.StructField("action_pt", T.StringType(), True),
            T.StructField("action_company", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("date", T.DateType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("pt_current", T.FloatType(), True),
            T.StructField("url", T.StringType(), True),
            T.StructField("importance", T.FloatType(), True),
            T.StructField("time", T.StringType(), True),
            T.StructField("notes", T.StringType(), True),
            T.StructField("url_calendar", T.StringType(), True),
            T.StructField("analyst_name", T.StringType(), True),
            T.StructField("rating_current", T.StringType(), True),
            T.StructField("rating_prior", T.StringType(), True),
        ]
    )
    ao_df_sp = (
        spark.read.format("orc")
        .option("path", AO_BUFFER + "/raw/*")
        .schema(schema)
        .load()
        .repartition(1)  ## do not partition
        .write.format("csv")
        .mode("overwrite")
        .save(AO_BUFFER + "/prepared/", header=True)
    )
    _ = spark.stop()
    return True


def make_scores():
    """
    Make and write scores for found tickers.
    """
    ## subset
    ao_subset = utils.read_many_csv(AO_BUFFER + "/prepared/")
    ao_subset["date"] = pd.to_datetime(ao_subset["date"]).apply(lambda r: r.date())

    fn = (
        glob.glob(AO_BUFFER + "/prepared/*.csv")[0]
        .split("/")[-1]
        .split(".csv")[0]
        .split("T")[0]
        .split("-")
    )
    date = dt.date(int(fn[0]), int(fn[1]), int(fn[2]))

    ## clean ratings text
    ao_subset["clean_rating"] = ao_subset["rating_current"].str.lower().map(rating_map)
    ## count ratings
    rating_count_df = (
        ao_subset.groupby(["ticker", "clean_rating"])
        .agg({"pt_prior": "count", "date": "max"})[["pt_prior", "date"]]
        .reset_index()
        .rename(columns={"pt_prior": "count", "clean_rating": "rating"})
    )
    ## add days since last rating
    rating_count_df["n_days_last_rating"] = (date - rating_count_df["date"]).apply(
        lambda r: r.days
    )
    ## pivot
    rating_pivot_df = rating_count_df.pivot_table(
        index="ticker", columns=["rating"], values="count", aggfunc="sum"
    ).fillna(0)
    ## add missing ratings
    for col in list(set(rating_list) - set(rating_pivot_df.columns)):
        rating_pivot_df[col] = 0

    ## make score
    for col in rating_pivot_df.columns:
        rating_pivot_df[col] = rating_pivot_df[col].astype(int)
    rating_pivot_df["total_ratings"] = rating_pivot_df.sum(1)
    for col in rating_list:
        rating_pivot_df["{}_percent".format(col)] = (
            rating_pivot_df[col] / rating_pivot_df["total_ratings"]
        )
    rating_pivot_df["score"] = (
        rating_pivot_df["strong_buy"] * 4
        + rating_pivot_df["buy"] * 3
        + rating_pivot_df["overweight"] * 2
        + rating_pivot_df["hold"] * 1
        + rating_pivot_df["underweight"] * -2
        + rating_pivot_df["sell"] * -3
        + rating_pivot_df["strong_sell"] * -4
    )
    rating_pivot_df["score_normalized"] = (
        rating_pivot_df["score"] / rating_pivot_df["total_ratings"]
    ).fillna(0)
    ## subset columns
    rating_subset_df = (
        rating_pivot_df[["score", "score_normalized", "total_ratings"]]
        .copy()
        .reset_index(drop=False)
    )
    rating_subset_df.columns.name = None

    ## lots of missing data, do inner join
    ## add last rating back to df
    # ratings
    n_days_df = rating_count_df.sort_values("date", ascending=False).drop_duplicates(
        "ticker"
    )[["ticker", "n_days_last_rating"]]

    full_ratings_df = rating_subset_df.merge(n_days_df, how="outer", on="ticker")
    # bring date
    if len(full_ratings_df) > 0:
        full_ratings_df["date"] = date
        _ = full_ratings_df.to_csv(
            AO_SCORES_BUFFER + "/scored/{}.csv".format(date), index=False
        )

    return True


def migrate_score():
    """
    Migrate scored AO data.
    Use file name as partition.
    """
    ## subset
    ao_scored = utils.read_many_csv(AO_SCORES_BUFFER + "/scored/")
    date = pd.to_datetime(
        glob.glob(AO_SCORES_BUFFER + "/scored/*.csv")[0].split("/")[-1].split(".csv")[0]
    ).date()
    date_cols = ["date"]
    for col in date_cols:
        ao_scored[col] = pd.to_datetime(ao_scored[col]).apply(lambda r: r.date())
    ## migrate
    ## start spark session
    spark = SparkSession.builder.appName("migrate-ao-score").getOrCreate()
    _ = utils.write_spark(spark, ao_scored, "/derived-measurements/ao-score", date)
    return True


def make_pt():
    """
    Make and write analyst price targets for found tickers.
    """
    ## read and clean
    ao_subset = utils.read_many_csv(AO_BUFFER + "/prepared/")
    ao_subset["date"] = pd.to_datetime(ao_subset["date"]).apply(lambda r: r.date())
    fn = (
        glob.glob(AO_BUFFER + "/prepared/*.csv")[0]
        .split("/")[-1]
        .split(".csv")[0]
        .split("T")[0]
        .split("-")
    )
    date = dt.date(int(fn[0]), int(fn[1]), int(fn[2]))

    ao_subset = ao_subset.sort_values("date", ascending=False).drop_duplicates(
        ["ticker", "analyst"]
    )[
        [
            "analyst",
            "ticker",
            "action_pt",
            "action_company",
            "date",
            "name",
            "pt_current",
        ]
    ]
    ao_subset["pt_sigma"] = ao_subset["pt_current"]

    ## aggregate
    ao_subset_agg = (
        ao_subset.groupby("ticker")
        .agg({"pt_current": np.median, "pt_sigma": np.std})
        .rename(columns={"pt_current": "pt_avg"})
        .fillna(0)
        .reset_index()
    )
    ao_subset_agg["date"] = date

    if len(ao_subset_agg) > 0:
        _ = ao_subset_agg.to_csv(
            AO_PT_BUFFER + "/targeted/{}.csv".format(date), index=False
        )
    return True


def migrate_pt():
    """
    Migrate analyst target price data.
    Use file name as partition.
    """
    ## subset
    ao_targeted = utils.read_many_csv(AO_PT_BUFFER + "/targeted/")
    date = pd.to_datetime(
        glob.glob(AO_PT_BUFFER + "/targeted/*.csv")[0].split("/")[-1].split(".csv")[0]
    ).date()
    date_cols = ["date"]
    for col in date_cols:
        ao_targeted[col] = pd.to_datetime(ao_targeted[col]).apply(lambda r: r.date())
    ## migrate
    ## start spark session
    spark = SparkSession.builder.appName("migrate-ao-price-target").getOrCreate()
    _ = utils.write_spark(
        spark, ao_targeted, "derived-measurements/ao-price-target", date
    )
    return True
