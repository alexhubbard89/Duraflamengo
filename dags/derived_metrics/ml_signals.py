import pandas as pd
import datetime as dt
import os
import common.utils as utils
import derived_metrics.settings as der_s
import fmp.settings as fmp_s


def join_ticker_signals(
    ds: dt.date,
    yesterday: bool = False,
):
    """
    Bring together all signals to describe the ticker
    for a given day.

    Inputs: Date
    """
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    if not os.path.isfile(f"{der_s.sector_ratio_scores}/{ds}.parquet"):
        return False
    ## Get Seasonality
    seasonality_df = pd.read_parquet(der_s.mbg_seasonality)
    ## Get ratios and add SPY
    sector_ratio_scores_df = pd.read_parquet(
        f"{der_s.sector_ratio_scores}/{ds}.parquet"
    )
    index = len(sector_ratio_scores_df)
    sector_ratio_scores_df.loc[index, "date"] = ds
    sector_ratio_scores_df.loc[index, "symbol"] = "SPY"
    sector_ratio_scores_df.loc[index, "sector"] = "SPY"
    sector_ratio_scores_df["month"] = ds.month
    sector_ratio_scores_df = sector_ratio_scores_df.fillna(0)
    ## Earnings
    earnings_date_df = pd.read_parquet(
        f"{fmp_s.earnings_calendar_confirmed}/{ds}.parquet",
        columns=["symbol", "when", "date"],
    )
    earnings_date_df["earnings_delta"] = pd.to_datetime(earnings_date_df["date"]).apply(
        lambda r: (r.date() - ds).days
    )
    earnings_date_df = pd.concat(
        [earnings_date_df, pd.get_dummies(earnings_date_df["when"])], 1
    ).drop(["when", "date"], 1)
    ## Price levels
    cols = [
        "open",
        "high",
        "low",
        "close",
        "symbol",
        "date",
        "close_avg_5",
        "close_avg_13",
        "close_avg_50",
        "close_avg_200",
        "avg_volume",
        "avg_range",
    ]
    params = {"evaluation": "equal", "column": "date", "slice": ds}
    daily_sr_df = utils.distribute_read_many_parquet(ds, der_s.sr_levels, params)[cols]
    if len(daily_sr_df) == 0:
        return False  ## it'll probs break on the column slice
    full_df = (
        sector_ratio_scores_df.merge(seasonality_df, how="left", on=["sector", "month"])
        .drop("month", 1)
        .merge(earnings_date_df, how="left", on="symbol")
        .merge(daily_sr_df, how="left", on=["symbol", "date"])
    )
    full_df["earnings_delta"] = full_df["earnings_delta"].fillna(90)
    full_df = full_df.fillna(0)
    full_df.to_parquet(f"{der_s.ml_ticker_signals}/{ds}.parquet", index=False)
