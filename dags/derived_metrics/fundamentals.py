import pandas as pd
import numpy as np
import datetime as dt
import common.utils as utils
import fmp.settings as fmp_s
import derived_metrics.settings as der_s
import os

from pyspark.sql import SparkSession


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
        ratios_df = utils.format_data(ratios_df, der_s.ratio_types)
        ratios_df.to_parquet(f"{der_s.ratios}/{ds}.parquet", index=False)


def make_is_ratios(ds: dt.date):
    """Make industry, sector aggregate ratios."""
    ## guard
    if not os.path.isfile(f"{der_s.ratios}/{ds}.parquet"):
        return False
    ## shape data
    ratios_df = pd.read_parquet(
        f"{der_s.ratios}/{ds}.parquet",
        columns=["symbol", "date", "eps", "pe", "ps", "pb"],
    )
    company_profile_df = pd.read_parquet(
        f"{fmp_s.company_profile_ticker}", columns=["symbol", "sector", "industry"]
    )
    company_profile_df = company_profile_df.loc[
        ~company_profile_df["sector"].isin(["", "N/A"])
    ].drop_duplicates()
    is_ratios_df = ratios_df.loc[
        (
            (ratios_df["pe"] != 0)
            & (abs(ratios_df["pe"]) != np.inf)
            & (ratios_df["eps"] != 0)
            & (abs(ratios_df["eps"]) != np.inf)
        )
    ].merge(company_profile_df, how="inner", on="symbol")
    ## make metrics
    industry_pe_df = (
        is_ratios_df.groupby(["date", "industry"])
        .agg(["mean", "std", "count"])
        .reset_index(drop=False)
    )
    sector_pe_df = (
        is_ratios_df.groupby(["date", "sector"])
        .agg(["mean", "std", "count"])
        .reset_index(drop=False)
    )
    ## save without formatting for now.
    ## Not sure how to format with multi-level columns names
    if len(industry_pe_df) > 0:
        industry_pe_df.to_parquet(f"{der_s.industry_ratios}/{ds}.parquet", index=False)
    if len(sector_pe_df) > 0:
        sector_pe_df.to_parquet(f"{der_s.sector_ratios}/{ds}.parquet", index=False)


def make_sector_ratio_scores(ds: dt.date, yesterday: bool = True):
    """
    Make z-scores for all ratios by sector.
    Save dataset as daily file.

    Input: Date
    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    if not os.path.isfile(f"{der_s.sector_ratios}/{ds}.parquet"):
        return False
    ## Load data
    # Ratios
    agg_ratios_df = pd.read_parquet(f"{der_s.sector_ratios}/{ds}.parquet")
    agg_ratios_df.columns = [
        "_".join([eval(x)[0], eval(x)[1]]) if eval(x)[1] != "" else eval(x)[0]
        for x in agg_ratios_df.columns
    ]
    # Profile with sector
    company_profile_df = pd.read_parquet(
        f"{fmp_s.company_profile_ticker}", columns=["symbol", "sector"]
    )
    company_profile_df = company_profile_df.loc[
        ~company_profile_df["sector"].isin(["", "N/A"])
    ].drop_duplicates()
    # Join ticker ration with sector and sector ratios
    compare_df = (
        pd.read_parquet(
            f"{der_s.ratios}/{ds}.parquet",
            columns=["symbol", "date", "eps", "pe", "ps", "pb"],
        )
        .dropna()
        .merge(company_profile_df, how="inner", on="symbol")
        .merge(agg_ratios_df.drop("date", 1), how="inner", on="sector")
    )
    ## Remove bad data
    compare_df = compare_df.loc[
        (
            (compare_df["pe"] != 0)
            & (abs(compare_df["pe"]) != np.inf)
            & (compare_df["eps"] != 0)
            & (abs(compare_df["eps"]) != np.inf)
        )
    ]
    ## make z-scores
    metrics = ["eps", "pe", "ps", "pb"]
    sector_ratio_scores_df = compare_df[["date", "symbol", "sector"] + metrics].copy()
    for metric in metrics:
        sector_ratio_scores_df[f"{metric}_score"] = (
            compare_df[metric] - compare_df[f"{metric}_mean"]
        ) / compare_df[f"{metric}_std"]

    sector_ratio_scores_df_typed = utils.format_data(
        sector_ratio_scores_df, der_s.sector_ratio_types
    )
    sector_ratio_scores_df_typed.to_parquet(
        f"{der_s.sector_ratio_scores}/{ds}.parquet", index=False
    )


def append_ratio_score(ticker: str, new_data: pd.DataFrame):
    """
    Open historical ticker price file,
    append new data, and deduplicate.

    Input: Ticker to append.
    """
    ticker_fn = f"{der_s.sector_ratio_scores_ticker}/{ticker}.parquet"
    if os.path.exists(ticker_fn):
        new_data = (
            pd.read_parquet(ticker_fn)
            .append(new_data)
            .drop_duplicates(["symbol", "date"])
            .sort_values("date", ascending=False)
        )
    new_data.to_parquet(ticker_fn, index=True)


def distribute_append_price(ds: dt.date, yesterday: bool = True):
    """
    Read daily ratio scores of tickers I'm monitoring.
    Prep the dataframe to distribute through spark.
    Append daily ratio score to full ticker history
    and deduplicate.

    Input: Date to deconstruct stream.
    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    if not os.path.isfile(f"{der_s.sector_ratio_scores}/{ds}.parquet"):
        return False
    ## prep list to distribute through append function
    # add missing columns to record schema evolution
    df_distribute = pd.read_parquet(f"{der_s.sector_ratio_scores}/{ds}.parquet")
    cols = df_distribute.columns.to_list()
    distribute_list = [
        {"ticker": x[0], "new_data": pd.DataFrame([x[1]], columns=cols)}
        for x in zip(
            df_distribute["symbol"].tolist(), df_distribute[cols].values.tolist()
        )
    ]
    ## distribute append
    spark = (
        SparkSession.builder.appName(f"pivot-daily-ratio-score-to-ticker-{ds}")
        .getOrCreate()
        .newSession()
    )
    sc = spark.sparkContext
    sc.parallelize(distribute_list).map(lambda r: append_ratio_score(**r)).collect()
    sc.stop()
    spark.stop()
