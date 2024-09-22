import pandas as pd
import numpy as np


class FibonacciRetracementLevels:
    """
    A class to calculate and analyze Fibonacci Retracement levels based on financial market data.

    This class takes in price data for a given financial instrument and calculates the Fibonacci Retracement
    levels over a specified lookback window. It also calculates normalized distances of price points (open,
    close, high, and low) from these Fibonacci levels, taking into account the average true range (ATR) for
    normalization.

    Attributes:
        lb_window (int): The lookback window size to calculate the retracement levels.
        fib_columns (list of str): List of column names for Fibonacci levels.
        price_columns (list of str): List of column names for price data.
        data (DataFrame): The input data along with calculated Fibonacci levels and normalized distances.
    """

    def __init__(self, data, lb_window=180):
        """
        Initializes the FibonacciRetracementLevels instance.

        Parameters:
            data (DataFrame): The input data frame containing price data and symbols.
            lb_window (int, optional): The lookback window size for calculating Fibonacci levels. Defaults to 180.
        """
        self.lb_window = lb_window
        self.fib_columns = [
            "fibonacci_level_0",
            "fibonacci_level_0.236",
            "fibonacci_level_0.382",
            "fibonacci_level_0.5",
            "fibonacci_level_0.618",
            "fibonacci_level_0.786",
            "fibonacci_level_1",
        ]
        self.price_columns = ["open", "close", "high", "low", "atr_conversion_period"]
        self.data = data[["symbol", "date"] + self.price_columns].copy()
        self.calculate_retracement_levels()
        self.calculate_normalized_distances()

    def calculate_retracement_levels(self):
        """
        Calculates Fibonacci retracement levels for the specified lookback window across the dataset.

        This method iterates over the dataset to find the maximum and minimum prices within the lookback window
        and calculates the Fibonacci retracement levels based on these values. The calculated levels are then merged
        back into the main dataset.
        """
        retracement_levels = []
        for i in range(len(self.data) - self.lb_window + 1):
            window_df = self.data.iloc[i : i + self.lb_window]
            max_price = window_df["close"].max()
            min_price = window_df["close"].min()
            levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]
            retracements = [
                (max_price - min_price) * level + min_price for level in levels
            ]
            retracement_levels.append(
                {
                    "date": window_df["date"].iloc[-1],
                    **{
                        f"fibonacci_level_{level}": retracement
                        for level, retracement in zip(levels, retracements)
                    },
                }
            )

        self.data = self.data.merge(
            pd.DataFrame(retracement_levels), how="left", on="date"
        )

    def calculate_normalized_distances(self):
        """
        Calculates and adds normalized distances of price points from Fibonacci levels to the dataset.

        For each price point (open, close, high, and low) and each Fibonacci level, this method calculates the
        relative distance of the price point from the Fibonacci level, normalized by the average true range (ATR).
        The results are added as new columns to the dataset.
        """
        for fib_component in self.fib_columns:
            for price in ["open", "close", "high", "low"]:
                column_name = f"{price}_relative_to_{fib_component}"
                self.data[column_name] = (
                    self.data[price] - self.data[fib_component]
                ) / self.data["atr_conversion_period"]

        self.data = self.data.drop(columns=self.price_columns)
