import pandas as pd
import numpy as np
import datetime as dt
import common.utils as utils
import fmp.settings as fmp_s
import derived_metrics.settings as s
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf


def make_ratios(ds: dt.date):
    """Make ratios using daily closing price."""
    if not os.path.isdir(fmp_s.historical_daily_price_full + f"/{ds}"):
        return False
    ## load data
    params = {"evaluation": "lt_e", "column": "fillingDate", "slice": ds}
    income_df = (
        utils.distribute_read_many_parquet(ds, fmp_s.fmp_q_i_ticker, params)
        .sort_values("fillingDate", ascending=False)
        .drop_duplicates("symbol")[
            ["symbol", "netIncome", "revenue", "weightedAverageShsOut"]
        ]
    )
    balance_sheet_df = (
        utils.distribute_read_many_parquet(ds, fmp_s.fmp_q_bs_ticker, params)
        .sort_values("fillingDate", ascending=False)
        .drop_duplicates("symbol")[
            ["symbol", "commonStock", "totalAssets", "totalLiabilities"]
        ]
    )
    cash_flow_df = (
        utils.distribute_read_many_parquet(ds, fmp_s.fmp_q_cf_ticker, params)
        .sort_values("fillingDate", ascending=False)
        .drop_duplicates("symbol")[["symbol", "dividendsPaid"]]
    )
    spark = SparkSession.builder.appName("read-data").getOrCreate()
    price_df = (
        spark.read.format("parquet")
        .option("path", fmp_s.historical_daily_price_full + f"/{ds}/*.parquet")
        .load()
        .toPandas()[["symbol", "date", "adjClose"]]
    )
    spark.stop()
    ## join
    ratios_df = (
        price_df.merge(income_df, how="left", on="symbol")
        .merge(balance_sheet_df, how="left", on="symbol")
        .merge(cash_flow_df, how="left", on="symbol")
    )
    ## make ratios
    ratios_df["eps"] = round(
        (ratios_df["netIncome"] - ratios_df["dividendsPaid"])
        / ratios_df["commonStock"],
        4,
    )
    ratios_df["pe"] = round(
        ratios_df["adjClose"]
        / (
            (ratios_df["netIncome"] - ratios_df["dividendsPaid"])
            / ratios_df["commonStock"]
        ),
        4,
    )
    ratios_df["ps"] = round(
        ratios_df["adjClose"]
        / ((ratios_df["revenue"] / ratios_df["weightedAverageShsOut"]) + 0.0001),
        4,
    )
    ratios_df["pb"] = round(
        ratios_df["adjClose"]
        / (
            (
                (ratios_df["totalAssets"] - ratios_df["totalLiabilities"])
                / ratios_df["weightedAverageShsOut"]
            )
            + 0.0001
        ),
        4,
    )
    if len(ratios_df) > 0:
        ## write to data-lake
        ratios_df = utils.format_data(ratios_df, s.ratio_types)
        spark = SparkSession.builder.appName("write-data").getOrCreate()
        daily_sub_dir = s.ratios.split("/data/")[1]
        utils.write_spark(spark, ratios_df, daily_sub_dir, ds, "parquet")
        spark.stop()


def make_is_ratios(ds: dt.date):
    """Make industry, sector aggregate ratios."""
    ## guard
    if not os.path.isdir(s.ratios + f"/{ds}"):
        return False
    ## shape data
    spark = SparkSession.builder.appName(f"read-data").getOrCreate()
    ratios_df = (
        spark.read.format("parquet")
        .option("path", s.ratios + f"/{ds}/*.parquet")
        .load()
        .toPandas()[["symbol", "date", "pe"]]
    )
    spark.stop()
    max_date = utils.find_max_date(fmp_s.company_profile)
    spark = SparkSession.builder.appName(f"read").getOrCreate()
    company_profile_df = (
        spark.read.format("parquet")
        .option("path", fmp_s.company_profile + f"/{max_date}/*.parquet")
        .load()
        .toPandas()[["symbol", "industry", "sector"]]
    )
    spark.stop()
    is_ratios_df = ratios_df.loc[
        ((ratios_df["pe"] != 0) & (abs(ratios_df["pe"]) != np.inf))
    ].merge(company_profile_df, how="inner", on="symbol")
    ## make metrics
    industry_pe_df = is_ratios_df.groupby(["date", "industry"]).agg({"pe": "describe"})
    industry_pe_df.columns = industry_pe_df.columns.droplevel()
    industry_pe_df.columns.name = None
    sector_pe_df = is_ratios_df.groupby(["date", "sector"]).agg({"pe": "describe"})
    sector_pe_df.columns = sector_pe_df.columns.droplevel()
    sector_pe_df.columns.name = None
    ## save
    if len(industry_pe_df) > 0:
        industry_pe_df = utils.format_data(industry_pe_df, s.industry_types)
        industry_pe_df = industry_pe_df.reset_index()
        spark = SparkSession.builder.appName("write-data").getOrCreate()
        daily_sub_dir = s.industry_ratios.split("/data/")[1]
        utils.write_spark(spark, industry_pe_df, daily_sub_dir, ds, "parquet")
        spark.stop()
    if len(sector_pe_df) > 0:
        sector_pe_df = utils.format_data(sector_pe_df, s.sector_types)
        sector_pe_df = sector_pe_df.reset_index()
        spark = SparkSession.builder.appName("write-data").getOrCreate()
        daily_sub_dir = s.sector_ratios.split("/data/")[1]
        utils.write_spark(spark, sector_pe_df, daily_sub_dir, ds, "parquet")
        spark.stop()
