import pandas as pd
import datetime as dt
import numpy as np
from sklearn.cluster import DBSCAN
import talib
import os
import common.utils as utils
import derived_metrics.settings as der_s
import fmp.settings as fmp_s
from pyspark.sql import SparkSession


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


def calculate_sma(df, window):
    """
    Calculate Simple Moving Average (SMA) with the specified window size for the given dataframe.
    """
    column_name = f"close_avg_{window}"
    df[column_name] = talib.SMA(df["close"], window)
    return df[["symbol", "date", column_name]]


def recognize_candlestick_patterns(df):
    """
    Recognize candlestick patterns for the given dataframe.
    """
    for candle_name in der_s.candle_names:
        df[candle_name] = getattr(talib, candle_name)(
            df["open"], df["high"], df["low"], df["close"]
        )
    return df[["symbol", "date"] + der_s.candle_names]


def calculate_trendlines(df, window=30, std_multiplier=2):
    """
    Calculate trendlines and channel lines using the given window size and standard deviation multiplier.

    Args:
        df: The dataframe containing the price data.
        window: The window size for the moving average. Default is 30.
        std_multiplier: The standard deviation multiplier. Default is 2.

    Returns:
        The dataframe with the trendlines and channel lines added.
    """
    trendline = df["close"].rolling(window=window).mean()
    upper_channel = (
        trendline + std_multiplier * df["close"].rolling(window=window).std()
    )
    lower_channel = (
        trendline - std_multiplier * df["close"].rolling(window=window).std()
    )
    df["trendline"] = trendline
    df["upper_channel"] = upper_channel
    df["lower_channel"] = lower_channel
    return df[["symbol", "date", "trendline", "upper_channel", "lower_channel"]]


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


def calculate_fibonacci_retracement(symbol, ds, lb_window=180):
    """
    Calculate Fibonacci retracement levels for the given dataframe.

    Args:
        symbol: The symbol symbol of the stock.
        ds: Date Stamp.
        lb_window: The look back window to use. Default, approx 6 months.

    Returns:
        None. The Fibonacci retracement levels are written to a parquet file.
    """
    if ds == None:
        ds = dt.date.today()
    ds = pd.to_datetime(ds).date()
    df = pd.read_parquet(
        f"{fmp_s.historical_ticker_price_full}/{symbol}.parquet"
    ).sort_values("date")
    df = df.loc[df["date"] >= (ds - dt.timedelta(lb_window))]

    # Find the highest and lowest prices
    max_price = df["close"].max()
    min_price = df["close"].min()

    # Calculate the Fibonacci levels
    levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]
    retracement_levels = [
        (max_price - min_price) * level + min_price for level in levels
    ]

    # Create a dataframe with the Fibonacci retracement levels
    fibonacci_dict = {
        "symbol": symbol,
        "date": df["date"].max(),
    }
    for i in range(len(levels)):
        fibonacci_dict[f"fibonacci_level_{levels[i]}"] = retracement_levels[i]

    fibonacci_df = pd.DataFrame([fibonacci_dict])

    # Write the Fibonacci retracement levels to a parquet file
    fn = f"{der_s.fibonacci_retracement}/{symbol}.parquet"
    if os.path.exists(fn):
        old_df = pd.read_parquet(fn)
        full_fibonacci_df = pd.concat(
            [fibonacci_df, old_df], ignore_index=True
        ).drop_duplicates(["symbol", "date"])
    else:
        full_fibonacci_df = fibonacci_df
    full_fibonacci_df.to_parquet(fn, index=False)
    return full_fibonacci_df


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
    df["is_peak"] = np.where(
        (df["price_level_roc"].shift(1) > 0) & (df["price_level_roc"].shift(-1) < 0),
        1,
        0,
    )
    df["is_trough"] = np.where(
        (df["price_level_roc"].shift(1) < 0) & (df["price_level_roc"].shift(-1) > 0),
        1,
        0,
    )

    # Filter for peaks and troughs
    peaks = df[df["is_peak"] == 1]
    troughs = df[df["is_trough"] == 1]

    # Create turning points dataset
    turning_points = pd.concat([peaks, troughs]).sort_index()

    # Prepare the data for clustering
    X = turning_points[["close", "low", "high"]].values

    # Perform DBSCAN clustering
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    clusters = dbscan.fit_predict(X)

    # Check the number of price levels found
    num_levels = len(np.unique(clusters))

    # If the number of levels is too low, adjust parameters and re-cluster
    i = 0
    while num_levels <= 4:
        # Decrease eps by a factor of 0.9 and increase min_samples by 1
        eps *= 0.9
        min_samples += 1

        # Perform DBSCAN clustering with adjusted parameters
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        clusters = dbscan.fit_predict(X)

        num_levels = len(np.unique(clusters))
        i += 1
        ## force a break
        if i > 25:
            num_levels = 5

    # Add cluster labels to turning points
    turning_points["price_level_cluster"] = clusters

    return turning_points[
        [
            "symbol",
            "date",
            "close",
            "low",
            "high",
            "volume",
            "price_level_roc",
            "is_peak",
            "is_trough",
            "price_level_cluster",
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


def calculate_bollinger_bands(df, timeperiod=20):
    """
    Calculate Bollinger Bands for the given dataframe.

    Args:
        df: The dataframe containing the price data with 'close' column.
        timeperiod: The time period for calculating the Bollinger Bands. Default is 20.

    Returns:
        DataFrame: The dataframe with Bollinger Bands columns ['middle_band_bb', 'upper_band_bb', 'lower_band_bb'].
    """
    # Calculate the middle band (SMA)
    middle_band = talib.SMA(df["close"], timeperiod=timeperiod)

    # Calculate the upper and lower bands
    upper_band, _, lower_band = talib.BBANDS(df["close"], timeperiod=timeperiod)

    # Add Bollinger Bands columns to the dataframe
    df["middle_band_bb"] = middle_band
    df["upper_band_bb"] = upper_band
    df["lower_band_bb"] = lower_band

    # Return the dataframe with Bollinger Bands
    return df[["symbol", "date", "middle_band_bb", "upper_band_bb", "lower_band_bb"]]


def calculate_ichimoku_cloud(
    df,
    conversion_period=9,
    base_period=26,
    leading_span_b_period=52,
    lagging_span_period=26,
):
    """
    Calculate the components of the Ichimoku Cloud for the given dataframe.

    Args:
        df: The dataframe containing the price data.
        conversion_period: The period for calculating Tenkan-sen (Conversion Line). Default is 9.
        base_period: The period for calculating Kijun-sen (Base Line). Default is 26.
        leading_span_b_period: The period for calculating Senkou Span B (Leading Span B). Default is 52.
        lagging_span_period: The period for calculating Chikou Span (Lagging Span). Default is 26.

    Returns:
        DataFrame: The dataframe with added columns for Ichimoku Cloud components.
    """
    high_converted = df["high"].rolling(window=conversion_period).max()
    low_converted = df["low"].rolling(window=conversion_period).min()
    tenkan_sen = (high_converted + low_converted) / 2

    high_base = df["high"].rolling(window=base_period).max()
    low_base = df["low"].rolling(window=base_period).min()
    kijun_sen = (high_base + low_base) / 2

    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(base_period)

    high_leading_span_b = df["high"].rolling(window=leading_span_b_period).max()
    low_leading_span_b = df["low"].rolling(window=leading_span_b_period).min()
    senkou_span_b = ((high_leading_span_b + low_leading_span_b) / 2).shift(base_period)

    chikou_span = df["close"].shift(-lagging_span_period)

    df["tenkan_sen"] = tenkan_sen
    df["kijun_sen"] = kijun_sen
    df["senkou_span_a"] = senkou_span_a
    df["senkou_span_b"] = senkou_span_b
    df["chikou_span"] = chikou_span

    return df[
        [
            "symbol",
            "date",
            "tenkan_sen",
            "kijun_sen",
            "senkou_span_a",
            "senkou_span_b",
            "chikou_span",
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


def enrich(symbol: str, ds: dt.datetime, lb_years: int = 5):
    """
    Enrich the given symbol's historical price data with technical analysis indicators, candlestick patterns, volume trend,
    price levels, Fibonacci retracement levels, Bollinger Bands, and Ichimoku Cloud. Store the enriched data in parquet files.

    Args:
        symbol: The symbol symbol of the stock.
        ds: Date to use for fibonacci_retracement
    Returns:
        None.
    """
    if ds == None:
        ds = dt.date.today()
    ds = pd.to_datetime(ds).date()
    jcols = ["symbol", "date"]
    fn = f"{der_s.enriched_technical_analysis}/{symbol}.parquet"
    # if no file, then exit
    if not os.path.exists(fn):
        return False

    full_df = pd.read_parquet(
        f"{fmp_s.historical_ticker_price_full}/{symbol}.parquet"
    ).sort_values("date")

    # slice to make more performant
    min_date = ds - dt.timedelta(365 * lb_years)
    full_df = full_df.loc[full_df["date"] >= min_date]

    rsi_df = calculate_rsi(full_df)
    macd_df = calculate_macd(full_df)

    sma_df = full_df[jcols].copy()
    for window in [5, 10, 20, 50, 100, 200]:
        tmp_df = calculate_sma(full_df, window)
        sma_df = sma_df.merge(tmp_df, how="left", on=jcols)

    #### Identify crossover points
    ## 50 vs 200 crossover
    sma_df["golden_cross"] = (
        (sma_df["close_avg_50"] > sma_df["close_avg_200"])
        & (sma_df["close_avg_50"].shift(1) < sma_df["close_avg_200"].shift(1))
    ).astype(int)
    sma_df["death_cross"] = (
        (sma_df["close_avg_50"] < sma_df["close_avg_200"])
        & (sma_df["close_avg_50"].shift(1) > sma_df["close_avg_200"].shift(1))
    ).astype(int)
    sma_df["recent_golden_cross"] = (
        sma_df["golden_cross"].rolling(5).sum() > 0
    ).astype(int)
    sma_df["recent_death_cross"] = (sma_df["death_cross"].rolling(5).sum() > 0).astype(
        int
    )

    ## 20 vs 50 crossover
    sma_df["medium_golden_cross"] = (
        (sma_df["close_avg_20"] > sma_df["close_avg_50"])
        & (sma_df["close_avg_20"].shift(1) < sma_df["close_avg_50"].shift(1))
    ).astype(int)
    sma_df["medium_death_cross"] = (
        (sma_df["close_avg_20"] < sma_df["close_avg_50"])
        & (sma_df["close_avg_20"].shift(1) > sma_df["close_avg_50"].shift(1))
    ).astype(int)
    sma_df["recent_medium_golden_cross"] = (
        sma_df["medium_golden_cross"].rolling(2).sum() > 0
    ).astype(int)
    sma_df["recent_medium_death_cross"] = (
        sma_df["medium_death_cross"].rolling(2).sum() > 0
    ).astype(int)

    ## 10 vs 20 crossover
    sma_df["short_golden_cross"] = (
        (sma_df["close_avg_10"] > sma_df["close_avg_20"])
        & (sma_df["close_avg_10"].shift(1) < sma_df["close_avg_20"].shift(1))
    ).astype(int)
    sma_df["short_death_cross"] = (
        (sma_df["close_avg_10"] < sma_df["close_avg_20"])
        & (sma_df["close_avg_10"].shift(1) > sma_df["close_avg_20"].shift(1))
    ).astype(int)

    candlestick_df = recognize_candlestick_patterns(full_df)
    trendlines_df = calculate_trendlines(full_df)
    volume_trend_df = calculate_volume_trend(full_df)
    price_levels_df = calculate_price_levels(full_df)
    price_levels_with_strength_df = calculate_strength_values(price_levels_df)
    support_resistance_df = find_support_resistance(
        price_levels_with_strength_df, symbol
    )
    fibonacci_df = calculate_fibonacci_retracement(symbol, ds)
    bollinger_bands_df = calculate_bollinger_bands(full_df)
    ichimoku_cloud_df = calculate_ichimoku_cloud(full_df)
    adx_df = calculate_adx_signal(full_df)
    minus_di_df = calculate_minus_di_signal(full_df)

    # Join all the datasets
    enriched_df = (
        full_df[jcols + ["close"]]
        .merge(rsi_df, how="left", on=jcols)
        .merge(macd_df, how="left", on=jcols)
        .merge(sma_df, how="left", on=jcols)
        .merge(candlestick_df, how="left", on=jcols)
        .merge(trendlines_df, how="left", on=jcols)
        .merge(volume_trend_df, how="left", on=jcols)
        .merge(
            price_levels_df.drop(columns=["close", "low", "high", "volume"]),
            how="left",
            on=jcols,
        )
        .merge(support_resistance_df, how="left", on=jcols)
        .merge(fibonacci_df, how="left", on=jcols)
        .merge(bollinger_bands_df, how="left", on=jcols)
        .merge(ichimoku_cloud_df, how="left", on=jcols)
        .merge(adx_df, how="left", on=jcols)
        .merge(minus_di_df, how="left", on=jcols)
        .drop_duplicates(["symbol", "date"])
    )

    ## add historical measurements
    enriched_df_w_history = add_history_features(enriched_df)

    enriched_df_w_history.to_parquet(fn, index=False)


def enrich_watchlist(ds: dt.datetime):
    spark = SparkSession.builder.appName("Enrichment").getOrCreate()

    watch_list = utils.get_watchlist(extend=True)
    symbols = spark.sparkContext.parallelize(watch_list)

    # Enrich each symbol in parallel
    symbols.foreach(lambda symbol: enrich(symbol, ds))

    # Stop the Spark session
    spark.stop()
