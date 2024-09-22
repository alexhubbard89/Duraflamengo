import pandas as pd
import talib


class BollingerBands:
    def __init__(self, data, lookback=5, timeperiod=20):
        """
        Initialize the Bollinger Bands analysis class with market data.

        Parameters:
        - data: DataFrame containing market data with columns for 'upper_band_bb', 'middle_band_bb', 'lower_band_bb', 'volume', 'rsi', and price metrics ('high', 'low', 'open', 'close').
        - lookback: Integer specifying the lookback period for calculating slopes and averages, default is 20 days.
        """
        self.lookback = lookback
        self.insights = data[["symbol", "date", "open", "close", "high", "low"]].copy()
        self.run(timeperiod)

    def calculate_bollinger_bands(self, timeperiod=20):
        """
        Calculate Bollinger Bands for the given dataframe.

        Args:
            self.insights: The dataframe containing the price data with 'close' column.
            timeperiod: The time period for calculating the Bollinger Bands. Default is 20.

        Returns:
            DataFrame: The dataframe with Bollinger Bands columns ['middle_band_bb', 'upper_band_bb', 'lower_band_bb'].
        """
        # Calculate the middle band (SMA)
        middle_band = talib.SMA(self.insights["close"], timeperiod=timeperiod)

        # Calculate the upper and lower bands
        upper_band, _, lower_band = talib.BBANDS(
            self.insights["close"], timeperiod=timeperiod
        )

        # Add Bollinger Bands columns to the dataframe
        self.insights["middle_band_bb"] = middle_band
        self.insights["upper_band_bb"] = upper_band
        self.insights["lower_band_bb"] = lower_band

    def calculate_candlestick_relative_position(self):
        """Calculate the relative position of the day's high, low, open, and close prices relative to the Bollinger Bands, normalized to the range [0, 1], with 0.5 representing the middle BB."""
        self.insights["range_bb"] = (
            self.insights["upper_band_bb"] - self.insights["lower_band_bb"]
        )
        self.insights["half_range_bb"] = self.insights["range_bb"] / 2

        # Calculate relative positions for high, low, open, and close relative to the BB
        self.insights["high_relative_position_bb"] = (
            self.insights["high"] - self.insights["lower_band_bb"]
        ) / self.insights["range_bb"]
        self.insights["low_relative_position_bb"] = (
            self.insights["low"] - self.insights["lower_band_bb"]
        ) / self.insights["range_bb"]
        self.insights["open_relative_position_bb"] = (
            self.insights["open"] - self.insights["lower_band_bb"]
        ) / self.insights["range_bb"]
        self.insights["close_relative_position_bb"] = (
            self.insights["close"] - self.insights["lower_band_bb"]
        ) / self.insights["range_bb"]

    def calculate_bb_slope(self):
        """Calculate the slope of the upper and lower Bollinger Bands over the specified lookback period to determine trend direction."""
        self.insights["upper_band_slope_bb"] = (
            self.insights["upper_band_bb"]
            - self.insights["upper_band_bb"].shift(self.lookback)
        ) / self.lookback
        self.insights["lower_band_slope_bb"] = (
            self.insights["lower_band_bb"]
            - self.insights["lower_band_bb"].shift(self.lookback)
        ) / self.lookback
        self.insights["slope_indicator_bb"] = (
            self.insights["upper_band_slope_bb"] + self.insights["lower_band_slope_bb"]
        ) / 2

    def normalize_half_bb_range(self):
        """Normalize the Half BB Range against its historical average to assess current market volatility relative to historical norms."""
        self.insights["half_range_bb"] = self.insights["range_bb"] / 2
        average_hbr = (
            self.insights["half_range_bb"].rolling(window=self.lookback).mean()
        )
        self.insights["normalized_half_range_bb"] = (
            self.insights["half_range_bb"] / average_hbr
        )

    def slope_with_range_metric(self):
        """
        Calculate the Composite Metric (CM) by multiplying the slope indicator by the normalized Half BB Range.

        Interpretation:
        - High Positive CM indicates a strong uptrend with increasing volatility, potentially a good time to consider buying or holding.
        - High Negative CM indicates a strong downtrend with increasing volatility, could be a signal to sell or short.
        - Low or Near-Zero CM suggests either a weak trend or low volatility, signaling a potential consolidation phase or a market lacking clear direction.
        """
        self.insights["slope_w_range_bb"] = (
            self.insights["slope_indicator_bb"]
            * self.insights["normalized_half_range_bb"]
        )

    def run(self, timeperiod):
        self.calculate_bollinger_bands(timeperiod)
        self.calculate_candlestick_relative_position()
        self.normalize_half_bb_range()
        self.calculate_bb_slope()
        self.slope_with_range_metric()
        self.insights = self.insights.drop(columns=["open", "close", "high", "low"])
