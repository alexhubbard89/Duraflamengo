import pandas as pd
import numpy as np
import os
import datetime as dt
import glob

import common.utils as utils
import derived_metrics.settings as der_s
import derived_metrics.technicals as technicals
from pyspark.sql import SparkSession

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold


def potential_support(df: pd.DataFrame, index: int, window: int) -> bool:
    try:
        conditions = []
        for i in range(1, window + 1):
            conditions.append(df.loc[index, "low"] <= df.loc[index - i, "low"])
        return (sum(conditions) / len(conditions)) * -1
    except:
        ## not enough data
        return 0


def potential_resistance(df: pd.DataFrame, index: int, window: int) -> bool:
    try:
        conditions = []
        for i in range(1, window + 1):
            conditions.append(df.loc[index, "high"] >= df.loc[index - i, "high"])
        return sum(conditions) / len(conditions)
    except:
        ## not enough data
        return 0


def discovery(
    ticker: str,
    ds: dt.date,
    yesterday: bool,
    potential_lb: int = 3,
    cycle_shift_threshold: float = 0.25,
):
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    fn = f"{der_s.option_swings}/{ds}/{ticker}.parquet"

    df_full = pd.read_parquet(f"{der_s.sr_levels}/{ticker}.parquet")
    df_full.loc[df_full["date"] <= ds]
    df = df_full.tail(365).copy()
    if sum([x in df.columns for x in ["high", "low"]]) < 2:
        return  ## missing data
    ## add average support resistance
    support_index = df.loc[df["support"].notnull()].index
    df.loc[support_index, "avg_support"] = (
        df.loc[support_index, "support"].rolling(min_periods=1, window=3).mean()
    )
    resistance_index = df.loc[df["resistance"].notnull()].index
    df.loc[resistance_index, "avg_resistance"] = (
        df.loc[resistance_index, "resistance"].rolling(min_periods=1, window=3).mean()
    )
    df[["avg_support", "avg_resistance"]] = df[
        ["avg_support", "avg_resistance"]
    ].fillna(method="ffill")

    ## anomalous ranges
    df["hl_delta"] = df["high"] - df["low"]
    hl_delta_rolling = df["hl_delta"].rolling(25)
    df["range_z_score"] = (
        df["hl_delta"] - hl_delta_rolling.mean()
    ) / hl_delta_rolling.std()
    non_anomalous_range = df.loc[abs(df["range_z_score"]) < 1.5, "range_z_score"].index
    df["rolling_range"] = df["hl_delta"].rolling(min_periods=1, window=25).mean()

    ## for date math
    support_index = df.loc[df["support"].notnull()].index
    resistance_index = df.loc[df["resistance"].notnull()].index
    df.loc[support_index, "support_date"] = df.loc[support_index, "date"]
    df.loc[resistance_index, "resistance_date"] = df.loc[resistance_index, "date"]
    df[["support_date", "resistance_date"]] = df[
        ["support_date", "resistance_date"]
    ].fillna(method="ffill")
    df["resistance_cycle_days"] = (df["support_date"] - df["resistance_date"]).apply(
        lambda r: r.days
    )
    df["support_cycle_days"] = (df["resistance_date"] - df["support_date"]).apply(
        lambda r: r.days
    )

    df["derivative"] = df["close_avg_5"].diff()
    df["derivative_shift"] = df["derivative"].shift(-1)

    df["potential_support"] = [potential_support(df, x, potential_lb) for x in df.index]
    df["potential_resistance"] = [
        potential_resistance(df, x, potential_lb) for x in df.index
    ]

    ## min distance from all recent price levels, normalized by price level
    sr_list = (
        df.loc[df["support"].notnull(), "support"].tolist()
        + df.loc[df["resistance"].notnull(), "resistance"].tolist()
    )
    df["price_level_distance"] = df["close"].apply(
        lambda r: min([abs(1 - (r / x)) for x in sr_list])
    )

    ## classify direction
    df["rolling_velocity"] = (
        df[["potential_support", "potential_resistance"]]
        .sum(1)
        .rolling(3)
        .mean()
        .apply(lambda r: round(r, 4))
    )
    df["rolling_velocity_yesterday"] = df["rolling_velocity"].shift(1)

    uptrend_index = [
        x for x in df.loc[df["rolling_velocity"] >= cycle_shift_threshold].index
    ]
    downtrend_index = [
        x for x in df.loc[df["rolling_velocity"] <= -cycle_shift_threshold].index
    ]
    df.loc[uptrend_index, "cycle_direction"] = 1
    df.loc[downtrend_index, "cycle_direction"] = -1
    df["cycle_direction"] = df["cycle_direction"].fillna(method="ffill")

    ## make cycle numbers
    cycle_shift = df.loc[df["cycle_direction"].rolling(2).sum() == 0].index
    df.loc[cycle_shift, "cycle_shift"] = df.loc[cycle_shift, "date"]
    df["cycle_shift"] = df["cycle_shift"].fillna(method="ffill")
    df.loc[cycle_shift, "cycle_number"] = [x + 1 for x in range(len(cycle_shift))]
    df["cycle_number"] = df["cycle_number"].fillna(method="ffill")
    ## add cycle length
    df_cycle_agg = (
        df.loc[non_anomalous_range]
        .groupby("cycle_number")
        .count()[["open"]]
        .rename(columns={"open": "cycle_length"})
        .reset_index(drop=False)
    )
    df_cycle_agg["cycle_length_avg"] = (
        df_cycle_agg["cycle_length"].rolling(min_periods=1, window=3).mean()
    )
    df = df.merge(df_cycle_agg, how="left", on="cycle_number")

    ## cycle rank
    df["intra_cycle_rank"] = (
        df.reset_index()
        .groupby("cycle_number")
        .rank(method="first")[["index"]]
        .rename(columns={"index": "intra_cycle_rank"})
    )

    ## price target delta
    uptrend_index = df.loc[df["cycle_direction"] == 1].index
    downtrend_index = df.loc[df["cycle_direction"] == -1].index
    df.loc[uptrend_index, "pt_delta"] = (
        df.loc[uptrend_index, "avg_resistance"] - df.loc[uptrend_index, "close"]
    )

    ## write input date data
    df_write = df.loc[df["date"] == ds]
    if len(df_write) > 0:
        df_write.to_parquet(fn, index=False)


def distribute_discovery(ds: dt.date, yesterday: bool = False):
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    distribute_list = utils.get_high_volume(ds)
    if not os.path.isdir(f"{der_s.option_swings}/{ds}/"):
        utils.mk_dir(f"{der_s.option_swings}/{ds}/")

    spark = SparkSession.builder.appName(
        "derived-metrics-option-swing-discovery"
    ).getOrCreate()
    sc = spark.sparkContext
    sc.parallelize(distribute_list).map(
        lambda ticker: discovery(ticker, ds, yesterday)
    ).collect()
    sc.stop()
    spark.stop()


def predict_swing(ds, yesterday):
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    if len(glob.glob(f"{der_s.option_swings}/{ds}/*")) == 0:
        return  ## no data
    if not os.path.isdir(f"{der_s.option_swings_ml}/{ds}/"):
        utils.mk_dir(f"{der_s.option_swings_ml}/{ds}/")
    ## Fit model based on all simulations
    all_simulations_df = pd.read_parquet(der_s.option_swings_simulations)
    all_simulations_df = all_simulations_df.dropna()
    feature_cols = [
        "intra_cycle_rank",
        "cycle_direction",
        "hl_delta",
        "avg_price_range",
        "cycle_length_avg",
        "rolling_velocity",
        "rolling_velocity_yesterday",
    ]
    X = all_simulations_df[feature_cols]
    y = all_simulations_df["price_purcahse_percent_to_target"] >= 1
    # define the model
    rf_clf = RandomForestClassifier()
    # fit the model on the whole dataset
    rf_clf.fit(X, y)

    ## fit discovery
    options_discovery = pd.read_parquet(f"{der_s.option_swings}/{ds}/").rename(
        columns={"rolling_range": "avg_price_range"}
    )

    X_predict = options_discovery[feature_cols].dropna()
    ## predict model
    predictions = rf_clf.predict(X_predict)
    predictions_prob = rf_clf.predict_proba(X_predict)
    X_predict["predict_class_bool"] = predictions
    X_predict["predict_prob"] = [x[1] for x in predictions_prob]
    X_predict["symbol"] = options_discovery.loc[X_predict.index, "symbol"]
    X_predict["expiration_target"] = X_predict["cycle_length_avg"].apply(
        lambda r: ds + dt.timedelta(int(r) + 14)
    )
    X_predict["avg_resistance"] = options_discovery.loc[
        X_predict.index, "avg_resistance"
    ]
    X_predict["avg_support"] = options_discovery.loc[X_predict.index, "avg_support"]
    X_predict["close"] = options_discovery.loc[X_predict.index, "close"]
    X_predict["date"] = ds

    for i in X_predict.index:
        cycle_direction = X_predict.loc[i, "cycle_direction"]
        if cycle_direction == 1:
            X_predict.loc[i, "contract_type"] = "CALL"
            X_predict.loc[i, "direction_type"] = "BULL"
            X_predict.loc[i, "target_price"] = X_predict.loc[i, "avg_resistance"]
        elif cycle_direction == -1:
            X_predict.loc[i, "contract_type"] = "PUT"
            X_predict.loc[i, "direction_type"] = "BEAR"
            X_predict.loc[i, "target_price"] = X_predict.loc[i, "avg_support"]

    X_predict.to_parquet(f"{der_s.option_swings_ml}/{ds}/data.parquet", index=False)
