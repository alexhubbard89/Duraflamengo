import pandas as pd
import numpy as np
import datetime as dt
from common import utils
import fmp.settings as fmp_s
import derived_metrics.settings as der_s
import os

from pyspark.sql import SparkSession


# Function to classify EPS
def classify_eps(eps):
    # Define your EPS thresholds here
    threshold_high_growth = 5.0  # Replace with your threshold value for 'High Growth'
    # You can define other thresholds if needed

    # Check the value of EPS and classify accordingly
    if eps > threshold_high_growth:
        return 4
    # You can add more conditions for other categories


# Function to classify PE
def classify_pe(pe):
    # Define your PE thresholds here
    threshold_income = 10.0  # Replace with your threshold value for 'Income'
    threshold_value = 20.0  # Replace with your threshold value for 'Value'
    threshold_moderate_growth = (
        30.0  # Replace with your threshold value for 'Moderate Growth'
    )

    # Check the value of PE and classify accordingly
    if pe <= threshold_income:
        return 1
    elif threshold_income < pe <= threshold_value:
        return 2
    elif threshold_value < pe <= threshold_moderate_growth:
        return 3
    else:
        return 4


# Function to classify PS
def classify_ps(ps):
    # Define your PS thresholds here
    threshold_income = 1.0  # Replace with your threshold value for 'Income'
    threshold_value = 2.0  # Replace with your threshold value for 'Value'
    threshold_moderate_growth = (
        3.0  # Replace with your threshold value for 'Moderate Growth'
    )

    # Check the value of PS and classify accordingly
    if ps <= threshold_income:
        return 1
    elif threshold_income < ps <= threshold_value:
        return 2
    elif threshold_value < ps <= threshold_moderate_growth:
        return 3
    else:
        return 4


# Function to classify PB
def classify_pb(pb):
    # Define your PB thresholds here
    threshold_income = 1.0  # Replace with your threshold value for 'Income'
    threshold_value = 2.0  # Replace with your threshold value for 'Value'
    threshold_moderate_growth = (
        3.0  # Replace with your threshold value for 'Moderate Growth'
    )

    # Check the value of PB and classify accordingly
    if pb <= threshold_income:
        return 1
    elif threshold_income < pb <= threshold_value:
        return 2
    elif threshold_value < pb <= threshold_moderate_growth:
        return 3
    else:
        return 4


# Function to classify Dividend Yield
def classify_dividend_yield(dividend_yield):
    # Define your Dividend Yield thresholds here
    threshold_high_growth = 5.0  # Replace with your threshold value for 'High Growth'
    threshold_moderate_growth = (
        2.0  # Replace with your threshold value for 'Moderate Growth'
    )
    threshold_value = 1.0  # Replace with your threshold value for 'Value'

    # Check the value of Dividend Yield and classify accordingly
    if dividend_yield >= threshold_high_growth:
        return 4
    elif threshold_moderate_growth <= dividend_yield < threshold_high_growth:
        return 3
    elif threshold_value <= dividend_yield < threshold_moderate_growth:
        return 2
    else:
        return 1


# Function to classify Net Income Margin
def classify_net_income_margin(net_income_margin):
    # Define your Net Income Margin thresholds here
    threshold_high_growth = 0.2  # Replace with your threshold value for 'High Growth'
    threshold_moderate_growth = (
        0.1  # Replace with your threshold value for 'Moderate Growth'
    )
    threshold_value = 0.05  # Replace with your threshold value for 'Value'

    # Check the value of Net Income Margin and classify accordingly
    if net_income_margin > threshold_high_growth:
        return 4
    elif threshold_moderate_growth <= net_income_margin <= threshold_high_growth:
        return 3
    elif threshold_value <= net_income_margin < threshold_moderate_growth:
        return 2
    else:
        return 1


# Function to classify Debt-to-Equity Ratio
def classify_debt_to_equity_ratio(debt_to_equity_ratio):
    # Define your Debt-to-Equity Ratio thresholds here
    threshold_high_growth = 1.0  # Replace with your threshold value for 'High Growth'
    threshold_moderate_growth = (
        0.5  # Replace with your threshold value for 'Moderate Growth'
    )
    threshold_value = 0.2  # Replace with your threshold value for 'Value'

    # Check the value of Debt-to-Equity Ratio and classify accordingly
    if debt_to_equity_ratio > threshold_high_growth:
        return 4
    elif threshold_moderate_growth <= debt_to_equity_ratio <= threshold_high_growth:
        return 3
    elif threshold_value <= debt_to_equity_ratio < threshold_moderate_growth:
        return 2
    else:
        return 1


def make_ratios(symbol: str):
    iq_fn = f"{fmp_s.fmp_q_i_ticker}/{symbol}.parquet"
    bsq_fn = f"{fmp_s.fmp_q_bs_ticker}/{symbol}.parquet"
    cfq_fn = f"{fmp_s.fmp_q_cf_ticker}/{symbol}.parquet"
    price_fn = f"{fmp_s.historical_ticker_price_full}/{symbol}.parquet"

    try:
        iq_df = (
            pd.read_parquet(iq_fn)
            .sort_values("fillingDate", ascending=False)[
                [
                    "symbol",
                    "fillingDate",
                    "netIncome",
                    "revenue",
                    "weightedAverageShsOut",
                ]
            ]
            .rename(columns={"fillingDate": "date"})
        )
        iq_df["revenue_growth_rate"] = (
            iq_df["revenue"].diff(-1) / iq_df["revenue"].shift(-1)
        ) * 100

        bsq_df = (
            pd.read_parquet(bsq_fn)
            .sort_values("fillingDate", ascending=False)[
                [
                    "symbol",
                    "fillingDate",
                    "commonStock",
                    "totalAssets",
                    "totalLiabilities",
                    "totalDebt",
                    "totalEquity",
                ]
            ]
            .rename(columns={"fillingDate": "date"})
        )
        cfq_df = (
            pd.read_parquet(cfq_fn)
            .sort_values("fillingDate", ascending=False)[
                ["symbol", "fillingDate", "dividendsPaid"]
            ]
            .rename(columns={"fillingDate": "date"})
        )
        price_df = pd.read_parquet(price_fn).sort_values("date", ascending=False)[
            ["symbol", "date", "adjClose"]
        ]
    except FileNotFoundError:
        # Handle missing files gracefully (e.g., print a message or take other appropriate action)
        print(f"File not found for symbol {symbol}. Skipping analysis for this symbol.")
        return None

    ## make ratios
    ratios_df = (
        price_df.merge(iq_df, how="left", on=["symbol", "date"])
        .merge(bsq_df, how="left", on=["symbol", "date"])
        .merge(cfq_df, how="left", on=["symbol", "date"])
        .fillna(method="bfill")
    )

    ratios_df["dps"] = round(
        ratios_df["dividendsPaid"] / (ratios_df["weightedAverageShsOut"] + 0.0001),
        4,
    )

    ratios_df["eps"] = round(
        (ratios_df["netIncome"] - ratios_df["dps"])
        / ratios_df["weightedAverageShsOut"],
        4,
    )
    ratios_df["pe"] = round(
        ratios_df["adjClose"]
        / (
            (ratios_df["netIncome"] - ratios_df["dps"])
            / ratios_df["weightedAverageShsOut"]
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

    ratios_df["dividend_yield"] = (ratios_df["dps"] / ratios_df["adjClose"]) * 100

    # income
    ratios_df["net_income_margin"] = (
        ratios_df["netIncome"] / ratios_df["revenue"]
    ) * 100

    # equity
    ratios_df["debt_to_equity_ratio"] = (
        ratios_df["totalDebt"] / ratios_df["totalEquity"]
    )

    # Apply each classification function to the respective column in your dataset
    ratios_df["eps_category"] = ratios_df["eps"].apply(lambda x: classify_eps(x))
    ratios_df["pe_category"] = ratios_df["pe"].apply(lambda x: classify_pe(x))
    ratios_df["ps_category"] = ratios_df["ps"].apply(lambda x: classify_ps(x))
    ratios_df["pb_category"] = ratios_df["pb"].apply(lambda x: classify_pb(x))
    ratios_df["dividend_yield_category"] = ratios_df["dividend_yield"].apply(
        lambda x: classify_dividend_yield(x)
    )
    ratios_df["net_income_margin_category"] = ratios_df["net_income_margin"].apply(
        lambda x: classify_net_income_margin(x)
    )
    ratios_df["debt_to_equity_ratio_category"] = ratios_df[
        "debt_to_equity_ratio"
    ].apply(lambda x: classify_debt_to_equity_ratio(x))

    cols = [
        "eps_category",
        "pe_category",
        "ps_category",
        "pb_category",
        "dividend_yield_category",
        "net_income_margin_category",
        "debt_to_equity_ratio_category",
    ]
    ratios_df["growth_class"] = ratios_df[cols].mean(axis=1)

    # save all data
    ratios_fn = f"{der_s.ratios}/{symbol}.parquet"
    ratios_df.to_parquet(ratios_fn, index=False)


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
