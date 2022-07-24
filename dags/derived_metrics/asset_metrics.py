import requests
import pandas as pd
import os
import datetime as dt
import common.utils as utils
import derived_metrics.settings as dm_s
from distutils.util import strtobool


def get_price(ds: dt.date, window: int = 20) -> pd.DataFrame:
    """
    Get price for a given day and creating
    average volume and price, make price slope.
    """
    min_date = ds - dt.timedelta(window)
    subdir_list = [str(x.date()) for x in pd.date_range(min_date, ds)]

    params = {
        "read_method": "many_dir",
        "path": "historical_daily_price_full",
        "subdir_list": subdir_list,
        "params": {
            "column_slice": ["date", "symbol", "open", "close", "volume"],
            "evaluation": "not_null",
            "columns": ["symbol", "open", "close", "volume"],
        },
    }

    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    price_df = pd.DataFrame(r.json())
    price_df["date"] = pd.to_datetime(price_df["date"].replace(" GMT", "")).apply(
        lambda r: r.date()
    )
    if not ds in price_df["date"].tolist():
        params = {
            "read_method": "parquet",
            "path": "historical_thirty_minute_price",
            "subdir_list": str(ds),
            "params": {
                "column_slice": ["date", "symbol", "open", "close", "volume"],
                "evaluation": "not_null",
                "columns": ["symbol", "open", "close", "volume"],
            },
        }

        r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
        today_price_df = (
            pd.DataFrame(r.json())
            .sort_values("date", ascending=False)
            .drop_duplicates("symbol")
        )
        today_price_df["date"] = pd.to_datetime(
            today_price_df["date"].replace(" GMT", "")
        ).apply(lambda r: r.date())
        price_df = pd.concat([price_df, today_price_df], ignore_index=True)
    price_df = price_df.sort_values(["symbol", "date"], ignore_index=True)

    ## add derived metrics
    price_df_agg = (
        price_df.groupby("symbol")
        .mean()[["close", "volume"]]
        .rename(columns={"close": "close_avg", "volume": "volume_avg"})
        .reset_index()
    )
    tmp_df = (
        pd.DataFrame(
            price_df.groupby("symbol")["close"].apply(
                lambda r: utils.rolling_regression(r, 10)
            )
        )
        .reset_index(drop=True)
        .rename(columns={"close": "close_slope"})
    )
    tmp_df.index = price_df.index
    price_df = (
        price_df.loc[price_df["date"] == ds]
        .join(tmp_df, how="left")
        .merge(price_df_agg, how="left", on="symbol")
    )
    return price_df


def get_yahoo_instrument(ds: dt.date, window: int = 20) -> pd.DataFrame:
    """
    Get metrics for each asset from Yahoo Finance.
    """
    min_date = ds - dt.timedelta(window)
    subdir_list = [str(x.date()) for x in pd.date_range(min_date, ds)]

    params = {
        "read_method": "many_dir",
        "path": "instrument_info_daily",
        "subdir_list": subdir_list,
        "params": {
            "column_slice": [
                "date",
                "ticker",
                "support",
                "resistance",
                "stop_loss",
                "description",
                "target_price",
                "short_term",
                "mid_term",
                "long_term",
                "discount",
            ]
        },
    }

    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    yahoo_metrics_df = pd.DataFrame(r.json()).rename(columns={"ticker": "symbol"})
    yahoo_metrics_df["date"] = pd.to_datetime(
        yahoo_metrics_df["date"].replace(" GMT", "")
    ).apply(lambda r: r.date())
    yahoo_metrics_df = (
        yahoo_metrics_df.sort_values("date", ascending=False)
        .drop(columns=["date"])
        .drop_duplicates("symbol")
    )
    return yahoo_metrics_df


def get_benzinga_opinions(ds: dt.date, window: int = 90) -> pd.DataFrame:
    """
    Get ratings data and convert to numeric score.
    """
    start_date = ds - dt.timedelta(90)
    params = {
        "path": "benzinga_rating_changes_coalesce",
        "subdir": f"/data.parquet",
        "read_method": "parquet",
    }

    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    ratings_data = pd.DataFrame(r.json())
    ratings_data["date"] = pd.to_datetime(
        ratings_data["date"].apply(lambda r: r.split(" GMT")[0])
    ).apply(lambda r: r.date())
    ratings_data = ratings_data.loc[ratings_data["date"] >= start_date]

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

    rating_value_map = {
        "strong_buy": 4,
        "buy": 3,
        "overweight": 2,
        "hold": 1,
        "underweight": -2,
        "sell": -3,
        "strong_sell": -4,
    }

    ## clean ratings text
    ratings_data["clean_rating"] = (
        ratings_data["rating_current"].str.lower().map(rating_map)
    )
    ratings_data["rating_value"] = (
        ratings_data["clean_rating"].str.lower().map(rating_value_map)
    )

    ratings_agg = (
        ratings_data.groupby("ticker")
        .agg(
            {
                "rating_value": "mean",
                "pt_current": "mean",
                "analyst": "count",
                "date": "max",
            }
        )
        .reset_index()
        .rename(
            columns={
                "ticker": "symbol",
                "rating_value": "avg_rating",
                "pt_current": "avg_pt",
                "analyst": "n_rating",
                "date": "max_date",
            }
        )
    )
    return ratings_agg


def attach_metrics(
    ds: dt.date, window: int = 20, yesterday: bool = False
) -> pd.DataFrame:
    """
    Attach metrics to each ticker symbol using:
    - Financial Modeling Prep
    - Yahoo Finance API
    - Benzinga analyst opinions
        - The lookgback window is longer for this
          because ratings are less frequent.

    This method overwrites yesterdays file when new data
    is available, i.e., with FMP collection.
    It then works intraday, collecting prices every hour on
    the half hour mark, found in the discovery_dag.
    """
    print("Starting")
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    price_df = get_price(ds, window)
    yahoo_metrics_df = get_yahoo_instrument(ds, window)
    ratings_agg = get_benzinga_opinions(ds, window * 2)
    full_df = price_df.merge(yahoo_metrics_df, how="left", on="symbol").merge(
        ratings_agg, how="left", on="symbol"
    )
    full_df["srv_compare"] = [
        (x[0] - x[2]) / (x[1] - x[2])
        for x in full_df[["close", "resistance", "support"]].values
    ]
    full_df["slrv_compare"] = [
        (x[0] - x[2]) / (x[1] - x[2])
        for x in full_df[["close", "resistance", "stop_loss"]].values
    ]
    full_df["growth_rate"] = (full_df["avg_pt"] / full_df["close"]) - 1
    full_df = utils.format_data(full_df, dm_s.asset_metrics_types)
    full_df.to_parquet(f"{dm_s.asset_metrics}/{ds}.parquet", index=False)
    print(full_df)
    print(f"{dm_s.asset_metrics}/{ds}.parquet")
