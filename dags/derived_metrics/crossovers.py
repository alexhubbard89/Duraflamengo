import os
import pandas as pd
import numpy as np
import datetime as dt
import derived_metrics.settings as der_s
import common.utils as utils
import tensorflow as tf
from tensorflow.keras.models import load_model

FEATURES = [
    "close",
    "rsi",
    "macd",
    "macdSignal",
    "macdHist",
    "close_avg_5",
    "close_avg_10",
    "close_avg_20",
    "close_avg_50",
    "close_avg_100",
    "close_avg_200",
    "medium_death_cross",
    "medium_death_cross",
    "recent_medium_death_cross",
    "recent_medium_death_cross",
    "short_death_cross",
    "short_death_cross",
    "CDLENGULFING",
    "CDLHAMMER",
    "CDLHARAMI",
    "CDLMORNINGSTAR",
    "trendline",
    "upper_channel",
    "lower_channel",
    "volume_ratio",
    "high_volume",
    "high_volume_signal",
    "middle_band_bb",
    "upper_band_bb",
    "lower_band_bb",
    "tenkan_sen",
    "kijun_sen",
    "senkou_span_a",
    "senkou_span_b",
    "rsi_ma",
    "macd_ma",
    "macd_signal_ma",
    "macd_hist_ma",
    "rsi_roc",
    "macd_roc",
    "macd_signal_roc",
    "macd_hist_roc",
    "rsi_lag",
    "macd_lag",
    "macd_signal_lag",
    "macd_hist_lag",
    "rsi_bin_Oversold",
    "rsi_bin_Neutral",
    "rsi_bin_Overbought",
    "macd_bin_Negative",
    "macd_bin_Positive",
    "CDLHAMMER_History",
    "CDLHAMMER_History_Count",
    "CDLHAMMER_History_Binary",
    "CDLHARAMI_History",
    "CDLHARAMI_History_Count",
    "CDLHARAMI_History_Binary",
    "CDLENGULFING_History",
    "CDLENGULFING_History_Count",
    "CDLENGULFING_History_Binary",
    "CDLMORNINGSTAR_History",
    "CDLMORNINGSTAR_History_Count",
    "CDLMORNINGSTAR_History_Binary",
]


class CrossPredictor:
    """
    CrossPredictor class for predicting golden cross and death cross crossovers.
    """

    MAX_CLASS_VALUE = 1

    def __init__(self, golden_cross_model_path, death_cross_model_path):
        """
        Initialize the CrossPredictor object.

        Parameters:
            - golden_cross_model_path (str): Path to the golden cross model.
            - death_cross_model_path (str): Path to the death cross model.
        """
        self.golden_cross_model = self.load_model(golden_cross_model_path)
        self.death_cross_model = self.load_model(death_cross_model_path)

    @staticmethod
    def load_model(model_path) -> tf.keras.Model:
        """
        Load the model from the given path.

        Parameters:
            - model_path (str): Path to the model.

        Returns:
            - tf.keras.Model: Loaded model.
        """
        custom_objects = {"custom_activation": CrossPredictor.custom_activation}
        return load_model(model_path, custom_objects=custom_objects)

    @staticmethod
    def custom_activation(x) -> tf.Tensor:
        """
        Custom activation function for the models.

        Parameters:
            - x (tf.Tensor): Input tensor.

        Returns:
            - tf.Tensor: Output tensor.
        """
        return tf.where(
            tf.math.greater(x, CrossPredictor.MAX_CLASS_VALUE),
            tf.constant(-1, dtype=tf.float32),
            tf.where(tf.math.less(x, 0), tf.constant(0, dtype=tf.float32), x),
        )

    def predict_golden_cross(self, df) -> pd.DataFrame:
        """
        Predict the golden cross crossovers and add standard score.

        Parameters:
            - df (pd.DataFrame): Input DataFrame.

        Returns:
            - pd.DataFrame: DataFrame with predictions.
        """
        X = np.asarray(df[FEATURES]).astype(np.float32)
        df["golden_cross_prob"] = self.golden_cross_model.predict(X)
        ## add standard score
        a = df["golden_cross_prob"].rolling(25)
        df["golden_cross_prob_ss"] = (df["golden_cross_prob"] - a.mean()) / a.std()
        return df[["symbol", "date", "golden_cross_prob", "golden_cross_prob_ss"]]

    def predict_death_cross(self, df) -> pd.DataFrame:
        """
        Predict the death cross crossovers and add standard score.

        Parameters:
            - df (pd.DataFrame): Input DataFrame.

        Returns:
            - pd.DataFrame: DataFrame with predictions.
        """
        X = np.asarray(df[FEATURES]).astype(np.float32)
        df["death_cross_prob"] = self.death_cross_model.predict(X)
        ## add standard score
        a = df["death_cross_prob"].rolling(25)
        df["death_cross_prob_ss"] = (df["death_cross_prob"] - a.mean()) / a.std()
        return df[["symbol", "date", "death_cross_prob", "death_cross_prob_ss"]]

    def predict_crossovers(self, symbol, ds=None):
        """
        Predict the golden cross and death cross crossovers for a given symbol and save the predictions.

        Parameters:
            - cross_predictor (CrossPredictor): CrossPredictor object.
            - symbol (str): Ticker symbol.
            - ds (str or datetime.date): Date for the predictions (optional). If not provided, today's date is used.

        Returns:
            - bool: True if predictions are made and saved successfully, False otherwise.
        """
        if not ds:
            ds = dt.date.today()
        else:
            ds = pd.to_datetime(ds).date()
        min_date = ds - dt.timedelta(365 * 3)
        ## read data
        fn = f"{der_s.enriched_technical_analysis}/{symbol}.parquet"
        if not os.path.exists(fn):
            return False
        df = pd.read_parquet(fn)
        df = df.loc[df["date"] >= min_date].dropna(subset=FEATURES)
        ## make predictions
        # Golden Cross
        gc_df = self.predict_golden_cross(df)
        gc_df.to_parquet(f"{der_s.gc_crossover}/{symbol}.parquet")
        # Death Cross
        dc_df = self.predict_death_cross(df)
        dc_df.to_parquet(f"{der_s.dc_crossover}/{symbol}.parquet")
        return True


def predict_all_crossovers(ds: dt.date):
    """
    Predict the golden cross and death cross crossovers for all symbols in the extended watchlist.

    Parameters:
        - cross_predictor (CrossPredictor): CrossPredictor object.
        - date (str or datetime.date): Date for the predictions.

    Returns:
        None
    """
    if not ds:
        ds = dt.date.today()
    else:
        ds = pd.to_datetime(ds).date()
    watch_list = utils.get_watchlist(extend=True)
    cross_predictor = CrossPredictor(der_s.gc_model_path, der_s.dc_model_path)

    for symbol in watch_list:
        cross_predictor.predict_crossovers(symbol, ds)
