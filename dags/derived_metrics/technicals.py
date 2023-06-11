from re import I
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession
import common.utils as utils
import derived_metrics.settings as der_s
import fmp.settings as fmp_s
import os
import talib

import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpdates

# determine bullish fractal
def is_support(df: pd.DataFrame, index: int, window: int = 4) -> bool:
    """
    Determine if input price is a support level.

    Inputs:
        - Dataframe with stock price lows
        - Index to check
        - Number of periods to check before and after
    """
    conditions = []
    for i in range(window):
        conditions.append(df.loc[index, "low"] <= df.loc[index - i, "low"])
        conditions.append(df.loc[index, "low"] <= df.loc[index + i, "low"])
    return (sum(conditions) / len(conditions)) == 1


# determine bearish fractal
def is_resistance(df: pd.DataFrame, index: int, window: int = 4) -> bool:
    """
    Determine if input price is a resistance level.

    Inputs:
        - Dataframe with stock price highs
        - Index to check
        - Number of periods to check before and after
    """

    conditions = []
    for i in range(window):
        conditions.append(df.loc[index, "high"] >= df.loc[index + i, "high"])
        conditions.append(df.loc[index, "high"] >= df.loc[index - i, "high"])
    return (sum(conditions) / len(conditions)) == 1


def find_support_resistance(ticker, lookback=90, sr_window=4, level_len=180):
    """
    Make support and resistance level for a given ticker.
    Attach 5, 13, 50, 200 day smooth moving averages (SMA) as close_avg.
    Graph the last 90 days.
    Write dataset and graph to datalake.

    Inputs:
        - Ticker
        - Lookback window to graph
        - level_len: Length to extend support/resistance lines.

    """
    ticker_fn = f"{fmp_s.historical_ticker_price_full}/{ticker}.parquet"
    write_data_fn = f"{der_s.sr_levels}/{ticker}.parquet"
    image_fn = f"{der_s.sr_graphs}/{ticker}.jpg"
    if not os.path.exists(ticker_fn):
        return pd.DataFrame()

    cols = ["date", "open", "high", "low", "close", "volume", "symbol"]
    df = pd.read_parquet(ticker_fn)[cols].sort_values("date", ignore_index=True)
    ## convert time to plot candles
    df["date_og"] = df["date"].copy()
    df["date"] = mpdates.date2num(df["date"])

    # a list to store resistance and support levels
    levels = []
    df["support"] = None
    df["resistance"] = None
    for i in range(sr_window, df.shape[0] - (sr_window + 1)):
        if is_support(df, i, sr_window):
            low = df.loc[i, "low"]
            df.loc[i, "support"] = low
        elif is_resistance(df, i, sr_window):
            high = df.loc[i, "high"]
            df.loc[i, "resistance"] = high

    df["close_avg_5"] = df["close"].rolling(window=5).mean()
    df["close_avg_13"] = df["close"].rolling(window=13).mean()
    df["close_avg_50"] = df["close"].rolling(window=50).mean()
    df["close_avg_200"] = df["close"].rolling(window=200).mean()
    df["avg_volume"] = df["volume"].rolling(window=5).mean()
    df["range"] = abs(df["open"] - df["close"])
    df["avg_range"] = df["range"].rolling(window=25).median()

    ## format and write data
    df_sr = df.rename(columns={"date": "date_int", "date_og": "date"})
    df_sr = utils.format_data(df_sr, der_s.support_resistance_types)
    df_sr.to_parquet(write_data_fn, index=False)

    ## plot
    df_subset = df.tail(lookback).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(16, 9))
    candlestick_ohlc(
        ax, df_subset.values, width=0.6, colorup="green", colordown="red", alpha=0.8
    )
    date_format = mpdates.DateFormatter("%d %b %Y")
    ax.xaxis.set_major_formatter(date_format)

    for min_index_ in df_subset.loc[df_subset["support"].notnull()].index:
        if min_index_ + level_len > len(df_subset):
            max_index_ = len(df_subset) - 1
        else:
            max_index_ = min_index_ + level_len

        price_line = df_subset.loc[min_index_, "support"]
        plt.hlines(
            price_line,
            xmin=df_subset.loc[min_index_, "date"],
            xmax=df_subset.loc[max_index_, "date"],
            colors="black",
            linestyle="--",
        )

    for min_index_ in df_subset.loc[df_subset["resistance"].notnull()].index:
        if min_index_ + level_len > len(df_subset):
            max_index_ = len(df_subset) - 1
        else:
            max_index_ = min_index_ + level_len

        price_line = df_subset.loc[min_index_, "resistance"]
        plt.hlines(
            price_line,
            xmin=df_subset.loc[min_index_, "date"],
            xmax=df_subset.loc[max_index_, "date"],
            colors="black",
            linestyle="--",
        )
    plt.title(f"{ticker}: Support Resistance")
    ax.plot(df_subset["date"], df_subset["close_avg_5"], label="Rolling 5")
    ax.plot(df_subset["date"], df_subset["close_avg_13"], label="Rolling 13")
    ax.plot(df_subset["date"], df_subset["close_avg_50"], label="Rolling 50")
    ax.plot(df_subset["date"], df_subset["close_avg_200"], label="Rolling 200")
    ax.legend()
    ax.grid()
    plt.savefig(image_fn)
    plt.close()


def distribute_sr_creation(
    ds: dt.date,
    graph_lookback=90,
    sr_window=4,
    level_len=180,
    v_threshold: int = 1000000,
    p_threshold: int = 10,
    yesterday: bool = False,
):
    """
    Clear graphs.
    Make support, resistance datasets and graphs for tickers
    that meet volume and price thresholds.
    The support, resistance datasets is for all history
    of each ticker.
    The graphs are the limited to the length of the lookback window.

    Input:
        - Date
        - Lookback window for graphs.
        - Length of convolution window for price levels.
        - Length to extend price levels on the graphs.
        - Volume threshold
        - Price threshold
        - Complete analysis for day prior than input?
    """
    utils.clear_directory(der_s.sr_graphs)
    ds = pd.to_datetime(ds).date()
    if utils.strbool(yesterday):
        ds = ds - dt.timedelta(1)
    distribute_list = utils.get_high_volume(ds, v_threshold, p_threshold)
    spark = SparkSession.builder.appName(
        "derived-metrics-daily-support-resistance"
    ).getOrCreate()
    sc = spark.sparkContext
    sc.parallelize(distribute_list).map(
        lambda ticker: (
            find_support_resistance(ticker, graph_lookback, sr_window, level_len)
        )
    ).collect()
    sc.stop()
    spark.stop()


def candlestick_graph_prep():
    """
    Enrich all tickers in TDA watchlist with:
    - RSI
    - MACD
    - Candlestick pattern names
    - Moving averages
    """

    watch_list = utils.get_watchlist()
    for ticker in watch_list:
        fn = f"{der_s.candlestick_graph_prep}/{ticker}.parquet"

        ## load
        full_df = pd.read_parquet(
            f"{fmp_s.historical_ticker_price_full}/{ticker}.parquet"
        ).sort_values("date")

        ## make stats
        full_df["rsi"] = talib.RSI(full_df["close"])
        full_df["macd"], full_df["macdSignal"], full_df["macdHist"] = talib.MACD(
            full_df["close"]
        )
        for i in [5, 13, 50, 100, 200]:
            full_df[f"close_avg_{i}"] = talib.SMA(full_df["close"], i)

        ## Calculate Stochastic Oscillator
        full_df["slowk"], full_df["slowd"] = talib.STOCH(
            full_df["high"], full_df["low"], full_df["close"]
        )

        ## make patterns
        for candle_name in der_s.candle_names:
            full_df[candle_name] = der_s.ticker_methods[candle_name.upper()](
                full_df["open"], full_df["high"], full_df["low"], full_df["close"]
            )

        patterns_by_day = []
        for i in full_df.index:
            tmp = full_df[der_s.candle_names].loc[i]
            patterns_by_day.append(list(tmp.loc[abs(tmp) > 0].index))
        full_df["pattern_list"] = patterns_by_day

        ## add vix
        vix_df = pd.read_parquet(f"{fmp_s.historical_ticker_price_full}/^VIX.parquet")[
            ["date", "close", "change"]
        ]
        vix_df.columns = ["date", "vix_close", "vix_change"]
        full_df = full_df.merge(vix_df, how="inner", on="date")

        full_df.to_parquet(fn, index=False)
