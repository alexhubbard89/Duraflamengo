import pandas as pd
import numpy as np
import talib


class IchimokuCloud:
    def __init__(
        self,
        data,
        conversion_period=9,
        base_period=26,
        leading_span_b_period=52,
        lagging_span_period=26,
    ):
        """
        Initialize the Ichimoku Cloud analysis class with market data.

        Parameters:
        - data: DataFrame containing market data with columns for Ichimoku components and price metrics ('tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span', 'open', 'close', 'high', 'low').
        """
        self.conversion_period = conversion_period
        self.base_period = base_period
        self.leading_span_b_period = leading_span_b_period
        self.lagging_span_period = lagging_span_period
        self.base_columns = [
            "high",
            "low",
            "open",
            "close",
            "atr_conversion_period",
            "atr_base_period",
            "atr_leading_span_b_period",
        ]
        self.data = data[["symbol", "date"] + self.base_columns].copy()
        self.run()

    def calculate_ichimoku_cloud(self):
        """
        Calculate the components of the Ichimoku Cloud for the given dataframe.

        Args:
            self.data: The dataframe containing the price data.
            conversion_period: The period for calculating Tenkan-sen (Conversion Line). Default is 9.
            base_period: The period for calculating Kijun-sen (Base Line). Default is 26.
            leading_span_b_period: The period for calculating Senkou Span B (Leading Span B). Default is 52.
            lagging_span_period: The period for calculating Chikou Span (Lagging Span). Default is 26.

        Returns:
            DataFrame: The dataframe with added columns for Ichimoku Cloud components.
        """
        high_converted = self.data["high"].rolling(window=self.conversion_period).max()
        low_converted = self.data["low"].rolling(window=self.conversion_period).min()
        tenkan_sen = (high_converted + low_converted) / 2

        high_base = self.data["high"].rolling(window=self.base_period).max()
        low_base = self.data["low"].rolling(window=self.base_period).min()
        kijun_sen = (high_base + low_base) / 2

        senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(self.base_period)

        high_leading_span_b = (
            self.data["high"].rolling(window=self.leading_span_b_period).max()
        )
        low_leading_span_b = (
            self.data["low"].rolling(window=self.leading_span_b_period).min()
        )
        senkou_span_b = ((high_leading_span_b + low_leading_span_b) / 2).shift(
            self.base_period
        )

        chikou_span = self.data["close"].shift(self.lagging_span_period)

        self.data["tenkan_sen"] = tenkan_sen
        self.data["kijun_sen"] = kijun_sen
        self.data["senkou_span_a"] = senkou_span_a
        self.data["senkou_span_b"] = senkou_span_b
        self.data["chikou_span"] = chikou_span

    def calculate_relative_distances(self):
        """
        Calculate the relative distances of each day's open, close, high, and low prices to the Ichimoku Cloud components and normalize these distances using the respective ATR.
        """
        # Calculate relative distances for each Ichimoku component
        for component in [
            "tenkan_sen",
            "kijun_sen",
            "senkou_span_a",
            "senkou_span_b",
            "chikou_span",
        ]:
            for price in ["open", "close", "high", "low"]:
                column_name = f"{price}_relative_to_{component}"
                if component in ["tenkan_sen", "senkou_span_a", "chikou_span"]:
                    self.data[column_name] = (
                        self.data[price] - self.data[component]
                    ) / self.data["atr_conversion_period"]
                elif component == "kijun_sen":
                    self.data[column_name] = (
                        self.data[price] - self.data[component]
                    ) / self.data["atr_base_period"]
                elif component == "senkou_span_b":
                    self.data[column_name] = (
                        self.data[price] - self.data[component]
                    ) / self.data["atr_leading_span_b_period"]

        self.data["tenkan_relative_to_kijun_momentum"] = (
            self.data["tenkan_sen"] - self.data["kijun_sen"]
        ) / self.data["atr_leading_span_b_period"]
        self.data["senkou_a_relative_to_senkou_b_momentum"] = (
            self.data["senkou_span_a"] - self.data["senkou_span_b"]
        ) / self.data["atr_leading_span_b_period"]

    def run(self):
        self.calculate_ichimoku_cloud()
        self.calculate_relative_distances()
        self.data = self.data.drop(columns=self.base_columns)
