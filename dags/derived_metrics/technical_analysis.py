import pandas as pd
import datetime as dt
import numpy as np
from sklearn.cluster import DBSCAN
import talib
import os
import derived_metrics.settings as der_s
import fmp.settings as fmp_s
from marty.Watchlist import Watchlist
from marty.DataProcessor import DataProcessor
from derived_metrics.technicals_library.BollingerBands import BollingerBands
from derived_metrics.technicals_library.IchimokuCloud import IchimokuCloud
from derived_metrics.technicals_library.FibonacciRetracementLevels import (
    FibonacciRetracementLevels,
)
from derived_metrics.technicals_library.PriceAvg import PriceAvg
from marty.MarketTrendPredictor import MarketTrendPredictor

from pyspark.sql import SparkSession

import psycopg2
from contextlib import contextmanager
import os


# Context manager for database connection
@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.environ.get("MARTY_DB_NAME"),
        user=os.environ.get("MARTY_DB_USR"),
        password=os.environ.get("MARTY_DB_PW"),
        host=os.environ.get("MARTY_DB_HOST"),
        port=os.environ.get("MARTY_DB_PORT", "5432"),
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def calculate_rsi(df):
    """
    Calculate Relative Strength Index (RSI) for the given dataframe.
    """
    df["rsi"] = talib.RSI(df["close"])
    return df[["symbol", "date", "rsi"]]


def calculate_macd(df):
    """
    Calculate Moving Average Convergence Divergence (MACD) for the given dataframe.
    """
    df["macd"], df["macdSignal"], df["macdHist"] = talib.MACD(df["close"])
    return df[["symbol", "date", "macd", "macdSignal", "macdHist"]]


def calculate_atr(
    data: pd.DataFrame,
    conversion_period: int = 9,
    base_period: int = 26,
    leading_span_b_period: int = 52,
    lagging_span_period: int = 26,
) -> pd.DataFrame:
    """
    Calculates the Average True Range (ATR) for given periods, adds them as new columns to the DataFrame, computes a 30-day rolling average for each ATR, and adds a comparison column for each ATR against its lagging average.

    Parameters:
    - data (pd.DataFrame): The input DataFrame containing the columns 'high', 'low', and 'close'.
    - conversion_period (int, default=9): The time period for the first ATR calculation.
    - base_period (int, default=26): The time period for the second ATR calculation.
    - leading_span_b_period (int, default=52): The time period for the third ATR calculation.
    - lagging_span_period (int, default=26): Included for consistency, but not used in this function.

    Returns:
    - pd.DataFrame: The original DataFrame with additional columns for ATR calculations, their 30-day rolling averages, and comparison columns.
    """
    # Calculate ATRs
    data["atr_9"] = talib.ATR(
        data["high"], data["low"], data["close"], timeperiod=conversion_period
    )
    data["atr_26"] = talib.ATR(
        data["high"], data["low"], data["close"], timeperiod=base_period
    )
    data["atr_52"] = talib.ATR(
        data["high"], data["low"], data["close"], timeperiod=leading_span_b_period
    )

    # Calculate 30-day rolling averages for each ATR
    data["atr_9_rolling_avg"] = data["atr_9"].rolling(window=30).mean()
    data["atr_26_rolling_avg"] = data["atr_26"].rolling(window=30).mean()
    data["atr_52_rolling_avg"] = data["atr_52"].rolling(window=30).mean()

    # Calculate comparison of ATR to its rolling average (ATR / rolling average)
    data["atr_9_comparison"] = data["atr_9"] / data["atr_9_rolling_avg"]
    data["atr_26_comparison"] = data["atr_26"] / data["atr_26_rolling_avg"]
    data["atr_52_comparison"] = data["atr_52"] / data["atr_52_rolling_avg"]

    return data


def recognize_candlestick_patterns(df):
    """
    Recognize candlestick patterns for the given dataframe.
    """
    for candle_name in der_s.candle_names:
        df[candle_name] = getattr(talib, candle_name)(
            df["open"], df["high"], df["low"], df["close"]
        )
    return df[["symbol", "date"] + der_s.candle_names]


# def calculate_trendlines(df, window=30, std_multiplier=2):
#     """
#     Calculate trendlines and channel lines using the given window size and standard deviation multiplier.

#     Args:
#         df: The dataframe containing the price data.
#         window: The window size for the moving average. Default is 30.
#         std_multiplier: The standard deviation multiplier. Default is 2.

#     Returns:
#         The dataframe with the trendlines and channel lines added.
#     """
#     rolling_mean = df["close"].rolling(window=window).mean()
#     rolling_std = df["close"].rolling(window=window).std()

#     # Calculate trendlines and channels based on rolling calculations
#     df["trendline"] = rolling_mean
#     df["upper_channel"] = rolling_mean + std_multiplier * rolling_std
#     df["lower_channel"] = rolling_mean - std_multiplier * rolling_std

#     # Calculate relative positions in a vectorized way
#     for price in ["open", "close", "high", "low"]:
#         for component in ["trendline", "upper_channel", "lower_channel"]:
#             column_name = f"{price}_relative_to_{component}"
#             df[column_name] = (df[price] - df[component]) / df["atr_9"]

#     return df[["symbol", "date"] + column_name]


def calculate_volume_trend(df, window=10, threshold=1.5):
    """
    Calculate volume trend using average volume and high volume activity.

    Args:
        df: The dataframe containing the price data.
        window: The window size for the average volume. Default is 10.
        threshold: The threshold for high volume activity. Default is 1.5.

    Returns:
        The dataframe with the volume trend indicators added.
    """
    df["average_volume"] = df["volume"].rolling(window=window).mean()
    df["volume_ratio"] = df["volume"] / df["average_volume"]
    df["high_volume"] = df["volume_ratio"] > threshold
    df["high_volume_signal"] = df["high_volume"].astype(int)
    return df[
        [
            "symbol",
            "date",
            "average_volume",
            "volume_ratio",
            "high_volume",
            "high_volume_signal",
        ]
    ]


def calculate_price_levels(df, eps=1.2, min_samples=3):
    """
    Calculate price levels using peak and trough analysis and DBSCAN.

    Args:
        df: The dataframe containing the price data with 'close', 'low', and 'high' columns.
        eps: The epsilon value for DBSCAN clustering. Default is 1.0.
        min_samples: The minimum number of samples for DBSCAN clustering. Default is 3.

    Returns:
        A dataframe containing the price levels with their corresponding cluster labels.
    """
    ## subset how far back the price level discovery goes
    max_date = df["date"].max()
    min_date = max_date - dt.timedelta(365 * 4)
    df = df.loc[df["date"] >= min_date].copy()

    # Perform peak and trough analysis
    df["price_level_roc"] = df["close"].pct_change()
    df["price_level_high_roc"] = df["high"].pct_change()
    df["price_level_low_roc"] = df["low"].pct_change()

    df["is_peak"] = np.where(
        (df["price_level_roc"].shift(1) > 0)
        & (df["price_level_roc"].shift(-1) < 0)
        & (df["price_level_roc"] > 0),
        1,
        0,
    )
    df["is_trough"] = np.where(
        (df["price_level_roc"].shift(1) < 0)
        & (df["price_level_roc"].shift(-1) > 0)
        & (df["price_level_roc"] < 0),
        1,
        0,
    )

    df["is_peak_high"] = np.where(
        (df["price_level_high_roc"].shift(1) > 0)
        & (df["price_level_high_roc"].shift(-1) < 0)
        & (df["price_level_high_roc"] > 0),
        1,
        0,
    )
    df["is_trough_low"] = np.where(
        (df["price_level_low_roc"].shift(1) < 0)
        & (df["price_level_low_roc"].shift(-1) > 0)
        & (df["price_level_low_roc"] < 0),
        1,
        0,
    )

    # Look for peak or trough for max date
    # compare_lb = 5
    # today_index_ = df.loc[df["date"] == df["date"].max()].index[0]

    # print(df.loc[today_index_])
    # today_high = df.loc[today_index_, "high"]
    # today_high_results = [
    #     x < today_high
    #     for x in df.loc[
    #         df["date"] >= (df["date"].max() - dt.timedelta(compare_lb + 1)), "high"
    #     ].tolist()[:-1]
    # ]
    # if sum(today_high_results) / len(today_high_results) == 1:
    #     df.loc[today_index_, "is_peak_high"] = 1

    # today_low = df.loc[today_index_, "low"]
    # today_low_results = [
    #     x > today_low
    #     for x in df.loc[
    #         df["date"] >= (df["date"].max() - dt.timedelta(compare_lb + 1)), "low"
    #     ].tolist()[:-1]
    # ]

    # if sum(today_low_results) / len(today_low_results) == 1:
    #     df.loc[today_index_, "is_trough_low"] = 1

    # # Filter for peaks and troughs
    # peaks = df[df[["is_peak", "is_peak_high"]].sum(1) > 0]
    # troughs = df[df[["is_trough", "is_trough_low"]].sum(1) > 0]

    # # Create turning points dataset
    # turning_points = pd.concat([peaks, troughs]).sort_index()

    # # Prepare the data for clustering
    # X = turning_points[["close", "low", "high"]].values

    # # Perform DBSCAN clustering
    # dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    # clusters = dbscan.fit_predict(X)

    # # Check the number of price levels found
    # num_levels = len(np.unique(clusters))

    # # If the number of levels is too low, adjust parameters and re-cluster
    # i = 0
    # while num_levels <= 4:
    #     # Decrease eps by a factor of 0.9 and increase min_samples by 1
    #     eps *= 0.9
    #     min_samples += 1

    #     # Perform DBSCAN clustering with adjusted parameters
    #     dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    #     clusters = dbscan.fit_predict(X)

    #     num_levels = len(np.unique(clusters))
    #     i += 1
    #     ## force a break
    #     if i > 25:
    #         num_levels = 5

    # # Add cluster labels to turning points
    # turning_points["price_level_cluster"] = clusters

    # df_full = df.merge(
    #     turning_points[["date", "price_level_cluster"]], how="left", on="date"
    # )

    return df[
        [
            "symbol",
            "date",
            "price_level_roc",
            "price_level_high_roc",
            "price_level_low_roc",
            "is_peak",
            "is_peak_high",
            "is_trough",
            "is_trough_low",
            # "price_level_cluster",
        ]
    ]


def calculate_strength_values(df):
    df = df.copy()
    # Calculate strength values for each price level
    df["strength"] = 0.0

    # Volume Analysis
    volume_avg = df.groupby("price_level_cluster")["volume"].mean()
    df["strength"] += df["price_level_cluster"].map(volume_avg)

    # Time Period
    time_period_duration = df.groupby("price_level_cluster")["date"].apply(
        lambda x: (x.max() - x.min()).days
    )
    df["strength"] += df["price_level_cluster"].map(time_period_duration)

    # Number of Touches
    num_touches = df.groupby("price_level_cluster").size()
    df["strength"] += df["price_level_cluster"].map(num_touches)

    return df


def find_support_resistance(df_input, symbol):
    """
    Find the closest support and resistance levels for a given close price.

    Args:
        df_input (pandas.DataFrame): DataFrame containing close prices and cluster information.
        symbol (str): Symbol of the stock.

    Returns:
        pandas.DataFrame: DataFrame containing the symbol, date, support, resistance, next_support, next_resistance, strength_resistance, strength_support, next_strength_resistance, and next_strength_support levels.

    Raises:
        warnings: If the df_input DataFrame is empty or if there are no clusters with support and resistance levels.
    """
    # Add close price because of missing dates from price level analysis
    use_cols = ["symbol", "date", "close"]
    df_close = pd.read_parquet(f"{der_s.enriched_technical_analysis}/{symbol}.parquet")[
        use_cols
    ]
    df = df_input.drop(columns=["close"]).copy()
    min_date = df["date"].min()
    df_close = df_close.loc[df_close["date"] >= min_date].copy()
    df = df_close.merge(df, how="left", on=["symbol", "date"])

    # Filter for clusters with support and resistance levels
    clusters_with_levels = df.loc[df["price_level_cluster"] >= 0].copy()

    # If no price levels found, return nulls
    if len(clusters_with_levels) == 0:
        df["resistance"] = np.nan
        df["support"] = np.nan
        df["next_resistance"] = np.nan
        df["next_support"] = np.nan
        df["strength_resistance"] = np.nan
        df["strength_support"] = np.nan
        df["next_strength_resistance"] = np.nan
        df["next_strength_support"] = np.nan
        return df[
            [
                "symbol",
                "date",
                "support",
                "resistance",
                "next_support",
                "next_resistance",
                "strength_resistance",
                "strength_support",
                "next_strength_resistance",
                "next_strength_support",
            ]
        ]

    # Calculate cluster statistics
    price_level_stats = (
        clusters_with_levels.groupby("price_level_cluster")["close"]
        .agg(["mean"])
        .join(df.groupby("price_level_cluster")["date"].agg([max, min]))
        .join(df.groupby("price_level_cluster")["strength"].mean())
    )

    # Find closest support and resistance for each close price
    df["resistance"] = np.nan
    df["support"] = np.nan
    df["next_resistance"] = np.nan
    df["next_support"] = np.nan
    df["strength_resistance"] = np.nan
    df["strength_support"] = np.nan
    df["next_strength_resistance"] = np.nan
    df["next_strength_support"] = np.nan

    for _, row in df.iterrows():
        close_price = row["close"]
        resistance_values = [
            price for price in price_level_stats["mean"] if price > close_price
        ]
        support_values = [
            price for price in price_level_stats["mean"] if price < close_price
        ]

        if resistance_values:
            closest_resistance = min(resistance_values)
            df.loc[_, "resistance"] = closest_resistance
            df.loc[_, "strength_resistance"] = price_level_stats.loc[
                price_level_stats["mean"] == closest_resistance, "strength"
            ].values[0]
            next_resistance_values = [
                price for price in resistance_values if price > closest_resistance
            ]
            if next_resistance_values:
                next_resistance = min(next_resistance_values)
                df.loc[_, "next_resistance"] = next_resistance
                df.loc[_, "next_strength_resistance"] = price_level_stats.loc[
                    price_level_stats["mean"] == next_resistance, "strength"
                ].values[0]

        if support_values:
            closest_support = max(support_values)
            df.loc[_, "support"] = closest_support
            df.loc[_, "strength_support"] = price_level_stats.loc[
                price_level_stats["mean"] == closest_support, "strength"
            ].values[0]
            next_support_values = [
                price for price in support_values if price < closest_support
            ]
            if next_support_values:
                next_support = max(next_support_values)
                df.loc[_, "next_support"] = next_support
                df.loc[_, "next_strength_support"] = price_level_stats.loc[
                    price_level_stats["mean"] == next_support, "strength"
                ].values[0]

    return df[
        [
            "symbol",
            "date",
            "support",
            "resistance",
            "next_support",
            "next_resistance",
            "strength_resistance",
            "strength_support",
            "next_strength_resistance",
            "next_strength_support",
        ]
    ]


def calculate_adx_signal(df: pd.DataFrame, adx_period: int = 14):
    """
    Calculate the ADX signal using the Average Directional Index.

    Args:
        df (pandas.DataFrame): DataFrame containing the historical price data.

    Returns:
        pandas.DataFrame: DataFrame with the ADX signal, trend, and trend direction added as new columns.
    """
    # Store required columns in variables
    high = df["high"]
    low = df["low"]
    close = df["close"]

    return_cols = ["symbol", "date", "adx_signal", "adx_diff"]

    # Calculate the trend based on the ADX values
    adx_signal = talib.ADX(high, low, close, timeperiod=adx_period)
    adx_diff = np.diff(adx_signal)

    df["adx_signal"] = adx_signal
    df["adx_diff"] = np.concatenate([[np.nan], adx_diff])

    # Calculate the slope of the ADX values for different time periods
    for i in [3, 5, 10]:
        slope_col = f"adx_slope_{i}"
        return_cols.append(slope_col)
        df[slope_col] = adx_signal.rolling(window=i).apply(
            lambda x: np.polyfit(np.arange(i), x, 1)[0], raw=False
        )

    return df[return_cols]


def calculate_minus_di_signal(df: pd.DataFrame, adx_period: int = 14) -> pd.DataFrame:
    """
    Calculate the -DI (Negative Directional Indicator) signal for trend analysis.

    This function calculates the -DI values using the talib.MINUS_DI function and
    analyzes the trend of the -DI values by calculating the difference between
    consecutive values.

    Args:
        df (pandas.DataFrame): DataFrame containing the historical price data.
        adx_period (int): The period for calculating the ADX (default: 14).

    Returns:
        pandas.DataFrame: DataFrame with the -DI signal, trend, and trend direction added as new columns.
    """
    # Store required columns in variables
    high = df["high"]
    low = df["low"]
    close = df["close"]

    return_cols = ["symbol", "date", "minus_di_signal", "minus_di_diff"]

    # Calculate the -DI values
    minus_di_signal = talib.MINUS_DI(high, low, close, timeperiod=adx_period)
    minus_di_diff = np.diff(minus_di_signal)

    df["minus_di_signal"] = minus_di_signal
    df["minus_di_diff"] = np.concatenate([[np.nan], minus_di_diff])

    # Calculate the slope of the -DI values for different time periods
    for i in [3, 5, 10]:
        slope_col = f"minus_di_slope_{i}"
        return_cols.append(slope_col)
        df[slope_col] = minus_di_signal.rolling(window=i).apply(
            lambda x: np.polyfit(np.arange(i), x, 1)[0], raw=False
        )

    return df[return_cols]


def add_history_features(df: pd.DataFrame, window_size: int = 7):
    # Calculate moving averages
    df["rsi_ma"] = df["rsi"].rolling(window=window_size).mean()
    df["macd_ma"] = df["macd"].rolling(window=window_size).mean()
    df["macd_signal_ma"] = df["macdSignal"].rolling(window=window_size).mean()
    df["macd_hist_ma"] = df["macdHist"].rolling(window=window_size).mean()

    # Calculate rate of change
    df["rsi_roc"] = df["rsi"].pct_change(periods=window_size)
    df["macd_roc"] = df["macd"].pct_change(periods=window_size)
    df["macd_signal_roc"] = df["macdSignal"].pct_change(periods=window_size)
    df["macd_hist_roc"] = df["macdHist"].pct_change(periods=window_size)

    # Calculate lagged values
    df["rsi_lag"] = df["rsi"].shift(window_size)
    df["macd_lag"] = df["macd"].shift(window_size)
    df["macd_signal_lag"] = df["macdSignal"].shift(window_size)
    df["macd_hist_lag"] = df["macdHist"].shift(window_size)

    # Binning or Discretization
    rsi_bins = [
        0,
        30,
        70,
        100,
    ]  # You can adjust the bin thresholds as per your requirement
    df["rsi_bin"] = pd.cut(
        df["rsi"], bins=rsi_bins, labels=["Oversold", "Neutral", "Overbought"]
    )

    macd_bins = [
        -np.inf,
        0,
        np.inf,
    ]  # You can adjust the bin thresholds as per your requirement
    df["macd_bin"] = pd.cut(df["macd"], bins=macd_bins, labels=["Negative", "Positive"])

    # Convert categorical variables to dummy variables
    df = pd.get_dummies(df, columns=["rsi_bin", "macd_bin"])

    # Define the lookback window for the identification history
    lookback_window = 10
    # Create the historical identification history features
    for pattern in ["CDLHAMMER", "CDLHARAMI", "CDLENGULFING", "CDLMORNINGSTAR"]:
        # Create a shifted version of the pattern column
        df[f"{pattern}_History"] = df[pattern].shift(1)

        # Calculate the cumulative sum of the pattern occurrences within the lookback window
        df[f"{pattern}_History_Count"] = (
            df[pattern].rolling(lookback_window).sum().shift(1).fillna(0)
        )

        # Create a binary indicator if the pattern has been identified at least once within the lookback window
        df[f"{pattern}_History_Binary"] = np.where(
            df[f"{pattern}_History_Count"] > 0, 1, 0
        )
    return df


# def enrich(symbol: str, ds: dt.datetime, lb_years: int = 5, write=False):
#     """
#     Enrich the given symbol's historical price data with technical analysis indicators, candlestick patterns, volume trend,
#     price levels, Fibonacci retracement levels, Bollinger Bands, and Ichimoku Cloud. Store the enriched data in parquet files.

#     Args:
#         symbol: The symbol symbol of the stock.
#         ds: Date to use for fibonacci_retracement
#     Returns:
#         None.
#     """
#     if ds == None:
#         ds = dt.date.today()
#     ds = pd.to_datetime(ds).date()
#     jcols = ["symbol", "date"]
#     fn = f"{der_s.enriched_technical_analysis}/{symbol}.parquet"

#     full_df = pd.read_parquet(
#         f"{fmp_s.historical_ticker_price_full}/{symbol}.parquet"
#     ).sort_values("date")

#     # slice to make more performant
#     min_date = ds - dt.timedelta(365 * lb_years)
#     full_df = full_df.loc[full_df["date"] >= min_date]
#     full_df = calculate_atr(full_df)  # ATR features are used later
#     sma_df = PriceAvg(full_df).data
#     full_df = full_df.merge(sma_df, how="left", on=jcols)  # Avg features are used later

#     _ = calculate_rsi(full_df)
#     _ = calculate_macd(full_df)
#     _ = recognize_candlestick_patterns(full_df)
#     # _ = calculate_trendlines(full_df) #calculating trend via BB slope
#     _ = calculate_volume_trend(full_df)
#     price_levels_df = calculate_price_levels(full_df)
#     # price_levels_with_strength_df = calculate_strength_values(price_levels_df)
#     # support_resistance_df = find_support_resistance(
#     #     price_levels_with_strength_df, symbol
#     # )
#     fibonacci_df = FibonacciRetracementLevels(full_df).data
#     bollinger_bands = BollingerBands(full_df)
#     bollinger_bands_df = bollinger_bands.insights
#     ichimoku_cloud_df = IchimokuCloud(full_df).data
#     _ = calculate_adx_signal(full_df)
#     _ = calculate_minus_di_signal(full_df)

#     # Join all the datasets
#     enriched_df = (
#         full_df.merge(price_levels_df, how="left", on=jcols)
#         .merge(fibonacci_df, how="left", on=jcols)
#         .merge(bollinger_bands_df, how="left", on=jcols)
#         .merge(ichimoku_cloud_df, how="left", on=jcols)
#         .drop_duplicates(["symbol", "date"])
#     )

#     ## add historical measurements
#     enriched_df_w_history = add_history_features(enriched_df)

#     if write:
#         enriched_df_w_history.to_parquet(fn, index=False)
#     else:
#         return enriched_df_w_history


class TechnicalIndicators:
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()

    def calculate_rsi(self, time_period=14):
        """
        Calculate the Relative Strength Index (RSI) using a custom time period.
        Args:
            time_period (int): Number of periods to use for calculation (default: 14).
        """
        self.data["rsi"] = talib.RSI(self.data["close"], timeperiod=time_period)
        return self.data[["symbol", "date", "rsi"]]

    def calculate_macd(self, fast_period=12, slow_period=26, signal_period=9):
        """
        Calculate Moving Average Convergence Divergence (MACD) using custom periods.
        Args:
            fast_period (int): Number of periods for the fast EMA (default: 12).
            slow_period (int): Number of periods for the slow EMA (default: 26).
            signal_period (int): Number of periods for the signal line (default: 9).
        """
        self.data["macd"], self.data["macdSignal"], self.data["macdHist"] = talib.MACD(
            self.data["close"],
            fastperiod=fast_period,
            slowperiod=slow_period,
            signalperiod=signal_period,
        )
        return self.data[["symbol", "date", "macd", "macdSignal", "macdHist"]]

    def calculate_atr(
        self, conversion_period=9, base_period=26, leading_span_b_period=52
    ):
        """
        Calculate the Average True Range (ATR) using specified periods for different trading strategies.
        Args:
            conversion_period (int): Time period for the first ATR calculation.
            base_period (int): Time period for the second ATR calculation.
            leading_span_b_period (int): Time period for the third ATR calculation.
        """
        # Generate column names based on argument names, not values
        atr_columns = {
            "conversion_period": f"atr_conversion_period",
            "base_period": f"atr_base_period",
            "leading_span_b_period": f"atr_leading_span_b_period",
        }

        # Calculate ATR for each period and assign to dynamically named columns
        self.data["atr_conversion_period"] = talib.ATR(
            self.data["high"],
            self.data["low"],
            self.data["close"],
            timeperiod=conversion_period,
        )
        self.data["atr_base_period"] = talib.ATR(
            self.data["high"],
            self.data["low"],
            self.data["close"],
            timeperiod=base_period,
        )
        self.data["atr_leading_span_b_period"] = talib.ATR(
            self.data["high"],
            self.data["low"],
            self.data["close"],
            timeperiod=leading_span_b_period,
        )

        return self.data[
            [
                "symbol",
                "date",
                "atr_conversion_period",
                "atr_base_period",
                "atr_leading_span_b_period",
            ]
        ]


class PatternRecognition:
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()

    def recognize_candlestick_patterns(self):
        for candle_name in der_s.candle_names:
            self.data[candle_name] = getattr(talib, candle_name)(
                self.data["open"],
                self.data["high"],
                self.data["low"],
                self.data["close"],
            )
        return self.data[["symbol", "date"] + der_s.candle_names]


class TrendAnalysis:
    def __init__(self, data: pd.DataFrame):
        self.data = data.copy()

    def calculate_volume_trend(self, window=10, threshold=1.5):
        self.data["average_volume"] = self.data["volume"].rolling(window=window).mean()
        self.data["volume_ratio"] = self.data["volume"] / self.data["average_volume"]
        self.data["high_volume"] = self.data["volume_ratio"] > threshold
        self.data["high_volume_signal"] = self.data["high_volume"].astype(int)
        return self.data[
            [
                "symbol",
                "date",
                "average_volume",
                "volume_ratio",
                "high_volume",
                "high_volume_signal",
            ]
        ]

    def calculate_price_levels(self, eps=1.2, min_samples=3):
        df = self.data.copy()
        max_date = df["date"].max()
        min_date = max_date - dt.timedelta(days=365 * 4)
        df = df[df["date"] >= min_date]

        df["price_level_roc"] = df["close"].pct_change()
        df["price_level_high_roc"] = df["high"].pct_change()
        df["price_level_low_roc"] = df["low"].pct_change()

        df["is_peak"] = np.where(
            (df["price_level_roc"].shift(1) > 0)
            & (df["price_level_roc"].shift(-1) < 0)
            & (df["price_level_roc"] > 0),
            1,
            0,
        )
        df["is_trough"] = np.where(
            (df["price_level_roc"].shift(1) < 0)
            & (df["price_level_roc"].shift(-1) > 0)
            & (df["price_level_roc"] < 0),
            1,
            0,
        )
        return df[
            [
                "symbol",
                "date",
                "price_level_roc",
                "price_level_high_roc",
                "price_level_low_roc",
                "is_peak",
                "is_trough",
            ]
        ]

    def calculate_adx_signal(self, adx_period: int = 14):
        df = self.data.copy()
        high = df["high"]
        low = df["low"]
        close = df["close"]

        adx_signal = talib.ADX(high, low, close, timeperiod=adx_period)
        adx_diff = np.diff(
            adx_signal, prepend=np.nan
        )  # prepend np.nan to maintain array size

        df["adx_signal"] = adx_signal
        df["adx_diff"] = adx_diff

        for i in [3, 5, 10]:
            slope_col = f"adx_slope_{i}"
            df[slope_col] = (
                df["adx_signal"]
                .rolling(window=i)
                .apply(
                    lambda x: (
                        np.polyfit(np.arange(len(x)), x, 1)[0]
                        if len(x) == i
                        else np.nan
                    ),
                    raw=True,
                )
            )
        return df[
            ["symbol", "date"]
            + [f"adx_slope_{i}" for i in [3, 5, 10]]
            + ["adx_signal", "adx_diff"]
        ]

    def calculate_minus_di_signal(self, adx_period: int = 14):
        df = self.data.copy()
        high = df["high"]
        low = df["low"]
        close = df["close"]

        minus_di_signal = talib.MINUS_DI(high, low, close, timeperiod=adx_period)
        minus_di_diff = np.diff(minus_di_signal, prepend=np.nan)

        df["minus_di_signal"] = minus_di_signal
        df["minus_di_diff"] = minus_di_diff

        for i in [3, 5, 10]:
            slope_col = f"minus_di_slope_{i}"
            df[slope_col] = minus_di_signal.rolling(window=i).apply(
                lambda x: (
                    np.polyfit(np.arange(len(x)), x, 1)[0] if len(x) == i else np.nan
                ),
                raw=True,
            )
        return_cols = ["symbol", "date", "minus_di_signal", "minus_di_diff"] + [
            f"minus_di_slope_{i}" for i in [3, 5, 10]
        ]
        return df[return_cols]


def fetch_symbol_data(symbol, ds, conn):
    """Fetch symbol data for the last 5 years from the database."""
    query = """
        SELECT *
        FROM symbol_daily_price
        WHERE symbol = %s AND date >= %s
        ORDER BY date
    """
    # Calculate the start date for 5 years back
    start_date = ds - pd.DateOffset(years=5)

    df = pd.read_sql(query, conn, params=(symbol, start_date.date()))
    return df


def enrich(
    symbol: str,
    ds: dt.datetime = None,
    enrich_lookback_days: int = 180,
    rsi_time_period: int = 14,
    macd_fast_period: int = 12,
    macd_slow_period: int = 26,
    macd_signal_period: int = 9,
    atr_conversion_period: int = 9,
    atr_base_period: int = 26,
    atr_leading_span_b_period: int = 52,
    volume_window: int = 10,
    volume_threshold: float = 1.5,
    price_levels_eps: float = 1.2,
    price_levels_min_samples: int = 3,
    adx_period: int = 14,
    minus_di_adx_period: int = 14,
    bb_lookback: int = 20,
    ichimoku_conversion_period: int = 9,
    ichimoku_base_period: int = 26,
    ichimoku_leading_span_b_period: int = 52,
    ichimoku_lagging_span_period: int = 26,
    fib_lb_window: int = 180,
):
    if ds is None:
        ds = dt.date.today()
    ds = pd.to_datetime(ds).date()
    jcols = ["symbol", "date"]

    # Load and initially slice the data to a 5-year maximum for efficiency
    with get_db_connection() as conn:
        full_df = fetch_symbol_data(symbol, ds, conn)

    # Initialize and calculate indicators
    tech_indicators = TechnicalIndicators(full_df)
    full_df = full_df.merge(
        tech_indicators.calculate_atr(
            atr_conversion_period, atr_base_period, atr_leading_span_b_period
        ),
        on=jcols,
        how="left",
    )
    full_df = full_df.merge(
        tech_indicators.calculate_rsi(rsi_time_period), on=jcols, how="left"
    )
    full_df = full_df.merge(
        tech_indicators.calculate_macd(
            macd_fast_period, macd_slow_period, macd_signal_period
        ),
        on=jcols,
        how="left",
    )

    # Initialize and compute trend and pattern recognition indicators
    trend_analysis = TrendAnalysis(full_df)
    full_df = full_df.merge(
        trend_analysis.calculate_volume_trend(volume_window, volume_threshold),
        on=jcols,
        how="left",
    )
    full_df = full_df.merge(
        trend_analysis.calculate_price_levels(
            price_levels_eps, price_levels_min_samples
        ),
        on=jcols,
        how="left",
    )
    full_df = full_df.merge(
        trend_analysis.calculate_adx_signal(adx_period), on=jcols, how="left"
    )
    full_df = full_df.merge(
        trend_analysis.calculate_minus_di_signal(minus_di_adx_period),
        on=jcols,
        how="left",
    )

    # Bollinger Bands, Ichimoku Cloud, and Fibonacci Levels
    bollinger_bands = BollingerBands(full_df, bb_lookback)
    ichimoku_cloud = IchimokuCloud(
        full_df,
        ichimoku_conversion_period,
        ichimoku_base_period,
        ichimoku_leading_span_b_period,
        ichimoku_lagging_span_period,
    )
    fibonacci_levels = FibonacciRetracementLevels(full_df, fib_lb_window)

    # Merge additional analysis results
    full_df = full_df.merge(bollinger_bands.insights, on=jcols, how="left")
    full_df = full_df.merge(ichimoku_cloud.data, on=jcols, how="left")
    full_df = full_df.merge(fibonacci_levels.data, on=jcols, how="left")

    # Final slice to ensure lookback days are respected for all metrics generation
    final_min_date = ds - dt.timedelta(days=enrich_lookback_days)
    enriched_df = full_df[full_df["date"] >= final_min_date]

    return enriched_df

    # Save data
    # enriched_df_w_history.to_parquet(fn, index=False)

    # # Load and save (Split into other task for DAG later)
    # mtp = MarketTrendPredictor()
    # enriched_df_predictions = mtp.predict(symbol)
    # enriched_df_predictions = enriched_df_predictions.drop(
    #     columns=[
    #         "earnings",
    #         "candle_patterns_found",
    #         "next_earnings_date",
    #         "days_next_earnings",
    #         # remove so it don't interfere with work the DataProcesses does
    #     ]
    # )


def enrich_watchlist(ds: dt.datetime, extended=False, batch_size=25):

    wl = Watchlist()
    wl.refresh_watchlist(extended=extended)
    watch_list = wl.df["symbol"].tolist()
    total_tickers = len(watch_list)
    print(f"Attempting to enrich {total_tickers} signals in batches of {batch_size}")

    # Process in batches
    for i in range(0, total_tickers, batch_size):
        batch_symbols = watch_list[i : i + batch_size]
        spark = SparkSession.builder.appName("Enrichment").getOrCreate()
        sc = spark.sparkContext

        # Run enrich in parallel for the batch
        symbols_rdd = sc.parallelize(batch_symbols)
        results = symbols_rdd.map(lambda symbol: enrich(symbol, ds)).collect()

        # Stop the Spark session after processing each batch
        spark.stop()

        # Write results to disk after each batch processing
        for result in results:
            symbol = result["symbol"].iloc[0]
            fn = f"{der_s.enriched_technical_analysis}/{symbol}.parquet"
            result.to_parquet(fn, index=False)

        print(
            f"Batch {i // batch_size + 1} of {total_tickers // batch_size + 1} processed and saved."
        )

    print("All data enrichment complete and saved to disk.")


# def enrich_watchlist(ds: dt.datetime, extended=False):
#     spark = SparkSession.builder.appName("Enrichment").getOrCreate()

#     wl = Watchlist()
#     wl.refresh_watchlist(extended=extended)
#     watch_list = wl.df["symbol"].tolist()
#     print(f"Attempting to enrich {len(watch_list)} signals")
#     symbols = spark.sparkContext.parallelize(watch_list)

#     # Enrich each symbol in parallel
#     symbols.foreach(lambda symbol: enrich(symbol, ds))

#     # Stop the Spark session
#     spark.stop()
