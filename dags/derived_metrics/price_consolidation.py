import pandas as pd
import numpy as np
import datetime as dt
import os
import common.utils as utils

import fmp.settings as fmp_s
import tda.settings as tda_s
import derived_metrics.settings as d_settings


def make_sparse(ticker: str, limit=True):
    """
    Make sparse matrix to show the price range between high and low
    for every day available.

    Inputs:
        - ticker
        - limit with lookback window (I don' need to calculate for all history)
    """
    fn = f"{d_settings.price_consolidation_sparse}/{ticker}.parquet"
    full_df = pd.read_parquet(
        f"{fmp_s.historical_ticker_price_full}/{ticker}.parquet"
    ).sort_values("date")
    if limit:
        ds_limit = dt.date.today() - dt.timedelta(days=(365 * 5))
        full_df = full_df.loc[full_df["date"] >= ds_limit]

    ## make base df
    a = full_df[["high", "low"]]
    data = [x / 100 for x in range(int(a.min().min() * 100), int(a.max().max() * 100))]
    BASE_SPARSE = pd.DataFrame(index=data)

    ## find entire space
    tmp_agg = full_df.groupby("date")[["high", "low"]].apply(
        lambda r: (r.min().min(), r.max().max() + 0.01)
    )

    ## get min values
    tmp_min = pd.DataFrame(tmp_agg.apply(lambda r: round(r[0], 2))).reset_index()
    tmp_min["value"] = 1
    min_pivot = tmp_min.pivot(index=0, columns="date", values="value")

    ## construct sparse df
    sparse_df = BASE_SPARSE.join(min_pivot)

    ## fill max value
    tmp_max = pd.DataFrame(tmp_agg.apply(lambda r: round(r[1], 2))).reset_index()
    for i in tmp_max.index:
        ds = tmp_max.loc[i, "date"]
        v = tmp_max.loc[i, 0]
        sparse_df.loc[v, ds] = 0

    sparse_df = sparse_df.fillna(method="ffill").fillna(0)
    sparse_df.columns = [str(x) for x in sparse_df.columns]
    sparse_df.to_parquet(fn, index=True)


def price_heatmap(ticker: str, window=10):
    """
    Use the sparse matrix to construct a heatmap of
    overlapping prices.

    Inputs:
        - Ticker
        - Lookback window
    """
    fn = f"{d_settings.price_consolidation_heatmap}/{ticker}.parquet"
    sparse_df = pd.read_parquet(
        f"{d_settings.price_consolidation_sparse}/{ticker}.parquet"
    )
    rolling_sparse_df = (
        sparse_df.transpose()
        .rolling(window)
        .sum()
        .fillna(0)
        .transpose()
        .sort_index(ascending=False)
    )
    scaled_rolling_sparse_df = (
        sparse_df.sort_index(ascending=False) * rolling_sparse_df
    ) / window
    scaled_rolling_sparse_df.to_parquet(fn, index=True)
