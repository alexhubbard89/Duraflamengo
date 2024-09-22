import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import datetime as dt
import fmp.settings as fmp_s
import derived_metrics.settings as der_s


def calculate_price_levels(df, eps=1.2, min_samples=3):
    """
    Calculate price levels using enhanced peak and trough analysis, volume weighting, and DBSCAN.

    Args:
        df: DataFrame with 'date', 'close', 'low', 'high', and 'volume'.
        eps: Epsilon value for DBSCAN. Default is 1.2.
        min_samples: Minimum samples for DBSCAN. Default is 3.

    Returns:
        DataFrame with price levels and cluster labels.
    """
    # Subset data to the last 4 years
    max_date = df["date"].max()
    min_date = max_date - dt.timedelta(days=365 * 4)
    df = df[df["date"] >= min_date].copy()

    # Calculate percentage change for close, high, and low
    for col in ["close", "high", "low"]:
        df[f"{col}_roc"] = df[col].pct_change()

    # Define peaks and troughs using high and low ROC
    df["is_peak"] = np.where(
        (df["close_roc"].shift(1) > 0) & (df["close_roc"] < 0), 1, 0
    )
    df["is_trough"] = np.where(
        (df["close_roc"].shift(1) < 0) & (df["close_roc"] > 0), 1, 0
    )

    # Filter for peaks and troughs
    peaks = df[df["is_peak"] == 1]
    troughs = df[df["is_trough"] == 1]

    # Combine peaks and troughs
    turning_points = pd.concat([peaks, troughs]).sort_index()

    # Add time weighting (more recent data is more significant)
    turning_points["time_weight"] = (turning_points["date"] - min_date).dt.days

    # Prepare data for clustering, including volume weighting
    X = turning_points[["close", "high", "low", "volume", "time_weight"]].values

    # DBSCAN clustering
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    clusters = dbscan.fit_predict(X)

    # Adjust clustering parameters if needed
    num_levels = len(np.unique(clusters))
    i = 0
    while num_levels <= 4 and i <= 25:
        eps *= 0.9
        min_samples += 1
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        clusters = dbscan.fit_predict(X)
        num_levels = len(np.unique(clusters))
        i += 1

    # Add cluster labels to turning points
    turning_points["price_level_cluster"] = clusters

    df_full = df.merge(
        turning_points[["date", "price_level_cluster"]], how="left", on="date"
    )

    return df_full


def analyze_price_levels(ticker="AAL", tolerance=0.10):
    """
    Analyze closing prices to find if they're near the .00, .25, .50, .75 levels within a specified tolerance.
    Additionally, calculates the wick length as a percentage of the day's total price movement.

    Args:
        ticker: The stock ticker symbol as a string.
        tolerance: The tolerance within which to consider a price close to the quarter levels.

    Returns:
        A DataFrame with dates, closing prices, the nearest quarter level, wick length in percent,
        and the direction of the wick.
    """
    historical_price_fn = f"{fmp_s.historical_ticker_price_full}/{ticker}.parquet"
    df = pd.read_parquet(historical_price_fn)[["date", "open", "close", "high", "low"]]
    max_date = df["date"].max()
    min_date = max_date - dt.timedelta(days=365 * 4)
    df = df[df["date"] >= min_date].copy()

    analysis_results = []

    def nearest_quarter(value):
        """Find the nearest quarter point."""
        return round(value * 4) / 4

    def is_near_quarter(close, tolerance):
        """Check if close is within tolerance of a quarter."""
        quarter = nearest_quarter(close)
        return abs(close - quarter) <= tolerance, quarter

    def calculate_wick_percentage(open_price, close_price, high, low):
        """Calculate the wick length as a percentage of the day's total price movement."""
        if close_price > open_price:
            wick_size = high - close_price
            total_range = high - low
        else:
            wick_size = close_price - low
            total_range = high - low
        wick_percentage = (wick_size / total_range) * 100 if total_range > 0 else 0
        return wick_percentage

    for index, row in df.iterrows():
        near_quarter, quarter_point = is_near_quarter(row["close"], tolerance)

        if near_quarter:
            wick_percentage = calculate_wick_percentage(
                row["open"], row["close"], row["high"], row["low"]
            )
            wick_direction = "upper" if row["close"] > row["open"] else "lower"

            analysis_results.append(
                [
                    row["date"],
                    row["close"],
                    quarter_point,
                    wick_percentage,
                    wick_direction,
                ]
            )

    return pd.DataFrame(
        analysis_results,
        columns=[
            "date",
            "close",
            "nearest_quarter",
            "wick_percentage",
            "wick_direction",
        ],
    )


def find_local_maximums(df):
    """
    Aggregate results to find local maximums based on the count of occurrences for each nearest_quarter.
    The strength of each local maximum is indicated by its count relative to the maximum count observed.

    Args:
        df: DataFrame with at least 'nearest_quarter' and 'date' columns.

    Returns:
        A DataFrame with each nearest_quarter, its count, and the relative strength of the count.
    """
    # Group by 'nearest_quarter' and count occurrences
    aggregated_df = (
        df.groupby(["nearest_quarter"])
        .agg({"date": "count"})
        .rename(columns={"date": "count"})
        .sort_index()
    )

    # Find peaks
    aggregated_df["count_roc"] = aggregated_df["count"].pct_change()
    aggregated_df["is_peak"] = np.where(
        (aggregated_df["count_roc"].shift(1) > 0)
        & (aggregated_df["count_roc"].shift(-1) < 0)
        & (aggregated_df["count_roc"] > 0),
        1,
        0,
    )

    # Find the maximum count for normalization
    max_count = aggregated_df["count"].max()

    # Calculate the relative strength as count/max_count and percentil
    aggregated_df["relative_strength"] = aggregated_df["count"] / max_count
    aggregated_df["relative_strength_percentile"] = aggregated_df[
        "count"
    ] / np.percentile(
        aggregated_df["count"], 99
    )  # relative to percentile

    # Identify local maximums: This step is simplified as considering high counts as local maxs.
    # More sophisticated methods could be used for identifying local maxs in different distributions.
    # For now, let's assume the higher counts indicate local maximums.

    return aggregated_df.reset_index()


from sklearn.cluster import DBSCAN
import numpy as np


def cluster_price_levels(aggregated_df, eps=0.5, min_samples=2):
    """
    Apply DBSCAN clustering on the aggregated DataFrame to find significant price levels.

    Args:
        aggregated_df: DataFrame returned by the `find_local_maximums` function.
        eps: The maximum distance between two samples for them to be considered as in the same neighborhood.
        min_samples: The number of samples in a neighborhood for a point to be considered as a core point.

    Returns:
        DataFrame with the original data and cluster labels for each price level.
    """
    # Prepare the data for clustering
    # Using 'nearest_quarter' as the value to cluster on, and 'relative_strength' as weights might help in some cases
    X = (
        aggregated_df[
            ["nearest_quarter", "is_peak", "count_roc", "relative_strength_percentile"]
        ]
        .fillna(0)
        .values
    )

    # Run DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    clusters = dbscan.fit_predict(X)

    # Add cluster labels to the DataFrame
    aggregated_df["cluster"] = clusters

    # Filter out noise points if necessary
    aggregated_df = aggregated_df[aggregated_df["cluster"] != -1]

    return aggregated_df


def find_min_date(max_min_values, price_levels_df):
    return price_levels_df.loc[
        price_levels_df["nearest_quarter"].isin(max_min_values), "date"
    ].min()


def pipeline(ticker, end_date, tolerance=0.10, percentile=99):
    results_df = analyze_price_levels(ticker=ticker, tolerance=tolerance)
    results_df = results_df[results_df["date"] <= pd.to_datetime(end_date)]
    local_maximums_df = find_local_maximums(results_df)
    clustered_df = cluster_price_levels(local_maximums_df)
    clustered_df_agg = clustered_df.groupby(["cluster"])["nearest_quarter"].describe()[
        ["count", "mean", "min", "max"]
    ]
    clustered_df_agg["percentile"] = clustered_df_agg["count"] / np.percentile(
        clustered_df_agg["count"], percentile
    )  # relative to percentile
    clustered_df_agg["min_date"] = [
        find_min_date(x, results_df) for x in clustered_df_agg[["min", "max"]].values
    ]

    return clustered_df_agg
