import pandas as pd
import talib


class PriceAvg:
    """
    A class to analyze price data, calculate average prices using Simple Moving Averages (SMA),
    identify key crossover points, and calculate normalized distances between price points and average prices.
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initializes the PriceAvg class with market price data.

        :param data: DataFrame containing market price data including symbol, date, open, high, low, close, and ATR values.
                     It must have columns for open, close, high, low prices, and ATR values for the calculations.
        """
        self.avg_columns = [
            "close_avg_5",
            "close_avg_10",
            "close_avg_20",
            "close_avg_50",
            "close_avg_100",
            "close_avg_200",
        ]
        self.base_columns = ["open", "close", "high", "low", "atr_conversion_period"]
        self.data = data[["symbol", "date"] + self.base_columns].copy()
        self.calculate_avg()
        self.identify_crossovers()
        self.calculate_normalized_distances()

    def calculate_sma(self, window: int):
        """
        Calculate Simple Moving Average (SMA) for the 'close' price over a specified window size.

        :param window: The number of periods over which to calculate the SMA.
        """
        column_name = f"close_avg_{window}"
        self.data[column_name] = talib.SMA(self.data["close"], window)

    def calculate_avg(self):
        """
        Calculates SMAs for various window sizes defined in avg_columns.
        """
        for window in [5, 10, 20, 50, 100, 200]:
            self.calculate_sma(window)

    def identify_crossovers(self):
        """
        Identifies various types of golden and death crossovers among the average price columns.
        Adds columns to the data to indicate the presence of these crossovers.
        """
        # Identify golden and death crosses for different average periods
        self._identify_cross("golden_cross", "close_avg_50", "close_avg_200", 5)
        self._identify_cross("death_cross", "close_avg_50", "close_avg_200", 5)
        self._identify_cross("medium_golden_cross", "close_avg_20", "close_avg_50", 2)
        self._identify_cross("medium_death_cross", "close_avg_20", "close_avg_50", 2)
        self._identify_cross(
            "short_golden_cross", "close_avg_10", "close_avg_20", 0
        )  # No rolling sum for short
        self._identify_cross(
            "short_death_cross", "close_avg_10", "close_avg_20", 0
        )  # No rolling sum for short

    def _identify_cross(self, column_name, fast_avg, slow_avg, roll_window):
        """
        Helper function to identify and flag crossovers between two moving averages.

        :param column_name: The name for the column to be added indicating the crossover.
        :param fast_avg: Column name for the faster moving average.
        :param slow_avg: Column name for the slower moving average.
        :param roll_window: The window size for rolling sum to identify recent crossovers.
        """
        golden_cross = (
            (self.data[fast_avg] > self.data[slow_avg])
            & (self.data[fast_avg].shift(1) < self.data[slow_avg].shift(1))
        ).astype(int)

        death_cross = (
            (self.data[fast_avg] < self.data[slow_avg])
            & (self.data[fast_avg].shift(1) > self.data[slow_avg].shift(1))
        ).astype(int)

        self.data[column_name] = golden_cross
        self.data[f"recent_{column_name}"] = (
            golden_cross.rolling(roll_window).sum() > 0 if roll_window else golden_cross
        )
        self.data[f"{column_name.replace('golden', 'death')}"] = death_cross
        self.data[f"recent_{column_name.replace('golden', 'death')}"] = (
            death_cross.rolling(roll_window).sum() > 0 if roll_window else death_cross
        )

    def calculate_normalized_distances(self):
        """
        Calculates the normalized distances between the open, close, high, and low prices and the various SMAs,
        normalized by the ATR (Average True Range) values.
        """
        # Calculate relative distances for each price point to each average
        for avg_component in self.avg_columns:
            for price in ["open", "close", "high", "low"]:
                column_name = f"{price}_relative_to_{avg_component}"
                self.data[column_name] = (
                    self.data[price] - self.data[avg_component]
                ) / self.data["atr_conversion_period"]

        column_name = f"close_avg_50_relative_to_close_avg_200"
        self.data[column_name] = (
            self.data["close_avg_50"] - self.data["close_avg_200"]
        ) / self.data["atr_conversion_period"]
        column_name = f"close_avg_20_relative_to_close_avg_50"
        self.data[column_name] = (
            self.data["close_avg_20"] - self.data["close_avg_50"]
        ) / self.data["atr_conversion_period"]
        column_name = f"close_avg_5_relative_to_close_avg_50"
        self.data[column_name] = (
            self.data["close_avg_5"] - self.data["close_avg_20"]
        ) / self.data["atr_conversion_period"]

        # Drop the base price columns and ATR to simplify the dataset
        self.data = self.data.drop(columns=self.base_columns)
