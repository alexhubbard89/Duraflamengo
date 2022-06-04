import pandas as pd
import numpy as np
import datetime as dt
import pytz
import glob

## Local code
import common.utils as utils

sm_data_lake_dir = "/Users/alexanderhubbard/stock-market/data"
MB_EARNINGS = sm_data_lake_dir + "/marketbeat-earnings"
SA_PROFILE = sm_data_lake_dir + "/stock-analysis/quarterly/profile"
SA_INCOME = sm_data_lake_dir + "/stock-analysis/quarterly/income"
SA_BS = sm_data_lake_dir + "/stock-analysis/quarterly/balance-sheet"
SA_CS = sm_data_lake_dir + "/stock-analysis/quarterly/cash-flow"
SA_RATIOS = sm_data_lake_dir + "/stock-analysis/quarterly/ratios"
BUSINESS_HEALTH = sm_data_lake_dir + "/derived-measurements/business-health"


def income_metrics() -> pd.DataFrame:
    """
    Make business health income metrics.
    """
    income_df = (
        utils.read_many_csv(SA_INCOME + "/")[
            ["datekey", "ticker", "revenue", "gp", "opinc", "netinc"]
        ]
        .dropna()
        .reset_index(drop=True)
    )
    income_df["date"] = pd.to_datetime(income_df["datekey"]).apply(lambda r: r.date())
    income_df = income_df.sort_values(["ticker", "date"]).reset_index(drop=True)
    income_df["gross_profit_margin"] = income_df["gp"] / income_df["revenue"]
    income_df["operating_margin"] = income_df["opinc"] / income_df["revenue"]
    income_df["proft_margin"] = income_df["netinc"] / income_df["revenue"]
    ## variability over 1 year
    income_df["operating_margin_sigma"] = (
        income_df.groupby(["ticker"])["operating_margin"]
        .rolling(4)
        .std()
        .reset_index()
        .set_index("level_1")["operating_margin"]
    )
    income_df["operating_margin_sigma_delta"] = income_df.groupby(["ticker"])[
        "operating_margin_sigma"
    ].diff()
    return income_df


def balance_sheet_metrics() -> pd.DataFrame:
    """
    Make business health income metrics.
    """
    balance_df = (
        utils.read_many_csv(SA_BS + "/")[
            [
                "datekey",
                "ticker",
                "assetsc",
                "inventory",
                "liabilitiesc",
                "debtc",
                "liabilities",
                "assets",
                "equity",
            ]
        ]
        .dropna()
        .reset_index(drop=True)
    )
    balance_df["date"] = pd.to_datetime(balance_df["datekey"]).apply(lambda r: r.date())

    balance_df["acid"] = (balance_df["assetsc"] - balance_df["inventory"]) / balance_df[
        "liabilitiesc"
    ]
    balance_df["solvency"] = balance_df["assetsc"] / balance_df["liabilitiesc"]
    balance_df["debt_ratio"] = balance_df["liabilities"] / balance_df["assets"]
    return balance_df


def ro_metrics(income_df: pd.DataFrame, balance_df: pd.DataFrame) -> pd.DataFrame:
    ro_df = balance_df[["ticker", "date", "assets", "equity"]].merge(
        income_df[["ticker", "date", "netinc"]], how="inner", on=["ticker", "date"]
    )
    ro_df["roe"] = ro_df["netinc"] / ro_df["equity"]
    ro_df["roa"] = ro_df["netinc"] / ro_df["assets"]
    return ro_df


def deplicate(df: pd.DataFrame) -> pd.DataFrame:
    bh_data = (
        pd.read_csv(BUSINESS_HEALTH + "/data.csv")
        .append(df)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return bh_data


def pipeline() -> bool:
    """
    Make business health metrics, deuplicate,
    and write to the datalake.
    """
    ## make metrics
    i_df = income_metrics()
    bs_df = balance_sheet_metrics()
    ro_df = ro_metrics(i_df, bs_df)
    ## join
    health_df = (
        i_df[
            [
                "ticker",
                "date",
                "gross_profit_margin",
                "operating_margin",
                "proft_margin",
                "operating_margin_sigma",
                "operating_margin_sigma_delta",
            ]
        ]
        .merge(
            bs_df[["ticker", "date", "acid", "solvency", "debt_ratio"]],
            how="inner",
            on=["ticker", "date"],
        )
        .merge(
            ro_df[["ticker", "date", "roe", "roa"]], how="inner", on=["ticker", "date"]
        )
        .sort_values(["ticker", "date"], ascending=[True, False])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    ## make stats
    health_df["healthy_profit_margin"] = health_df["gross_profit_margin"] >= 0.6
    health_df["healthy_operating_margin"] = health_df["operating_margin"] < 0.15
    health_df["healthy_proft_margin"] = health_df["proft_margin"] >= 0.2
    health_df["healthy_acid"] = health_df["acid"] >= 1
    health_df["healthy_solvency"] = health_df["solvency"] >= 2
    health_df["healthy_debt_ratio"] = health_df["debt_ratio"] < 0.4
    health_df["healthy_roe"] = health_df["roe"] >= 0.15
    health_df["healthy_roa"] = health_df["roa"] >= 0.05
    ## deduplicate
    health_df_dedupe = deplicate(health_df)
    ## write to datalake
    health_df_dedupe.to_csv(BUSINESS_HEALTH + "/data.csv", index=False)
    return True
