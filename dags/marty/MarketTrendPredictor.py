import pandas as pd
import numpy as np
import datetime as dt
from tensorflow.keras.models import load_model
import derived_metrics.settings as der_s
import fmp.settings as fmp_s
import os
from marty.DataProcessor import DataProcessor


class MarketTrendPredictor:
    def __init__(self):
        self.model_path = (
            os.environ["DL_DIR"] + "/marty/models/MartketTrendClassificationSystemV0.h5"
        )
        self.model = load_model(self.model_path)
        self.num_days_per_sequence = 14
        self.optimal_thresholds = {0: 0.1880774, 1: 0.3375966, 2: 0.26565328}
        self.features = [
            "atr_9",
            "atr_26",
            "atr_52",
            "atr_9_rolling_avg",
            "atr_26_rolling_avg",
            "atr_52_rolling_avg",
            "atr_9_comparison",
            "atr_26_comparison",
            "atr_52_comparison",
            "golden_cross",
            "death_cross",
            "medium_golden_cross",
            "medium_death_cross",
            "short_golden_cross",
            "short_death_cross",
            "open_relative_to_close_avg_5",
            "close_relative_to_close_avg_5",
            "high_relative_to_close_avg_5",
            "low_relative_to_close_avg_5",
            "open_relative_to_close_avg_10",
            "close_relative_to_close_avg_10",
            "high_relative_to_close_avg_10",
            "low_relative_to_close_avg_10",
            "open_relative_to_close_avg_20",
            "close_relative_to_close_avg_20",
            "high_relative_to_close_avg_20",
            "low_relative_to_close_avg_20",
            "open_relative_to_close_avg_50",
            "close_relative_to_close_avg_50",
            "high_relative_to_close_avg_50",
            "low_relative_to_close_avg_50",
            "open_relative_to_close_avg_100",
            "close_relative_to_close_avg_100",
            "high_relative_to_close_avg_100",
            "low_relative_to_close_avg_100",
            "open_relative_to_close_avg_200",
            "close_relative_to_close_avg_200",
            "high_relative_to_close_avg_200",
            "low_relative_to_close_avg_200",
            "close_avg_50_relative_to_close_avg_200",
            "close_avg_20_relative_to_close_avg_50",
            "close_avg_5_relative_to_close_avg_50",
            "rsi",
            "macd",
            "macdSignal",
            "macdHist",
            "CDL2CROWS",
            "CDL3BLACKCROWS",
            "CDL3INSIDE",
            "CDL3LINESTRIKE",
            "CDL3OUTSIDE",
            "CDL3STARSINSOUTH",
            "CDL3WHITESOLDIERS",
            "CDLABANDONEDBABY",
            "CDLADVANCEBLOCK",
            "CDLBELTHOLD",
            "CDLBREAKAWAY",
            "CDLCLOSINGMARUBOZU",
            "CDLCONCEALBABYSWALL",
            "CDLCOUNTERATTACK",
            "CDLDARKCLOUDCOVER",
            "CDLDOJI",
            "CDLDOJISTAR",
            "CDLDRAGONFLYDOJI",
            "CDLENGULFING",
            "CDLEVENINGDOJISTAR",
            "CDLEVENINGSTAR",
            "CDLGAPSIDESIDEWHITE",
            "CDLGRAVESTONEDOJI",
            "CDLHAMMER",
            "CDLHANGINGMAN",
            "CDLHARAMI",
            "CDLHARAMICROSS",
            "CDLHIGHWAVE",
            "CDLHIKKAKE",
            "CDLHIKKAKEMOD",
            "CDLHOMINGPIGEON",
            "CDLIDENTICAL3CROWS",
            "CDLINNECK",
            "CDLINVERTEDHAMMER",
            "CDLKICKING",
            "CDLKICKINGBYLENGTH",
            "CDLLADDERBOTTOM",
            "CDLLONGLEGGEDDOJI",
            "CDLLONGLINE",
            "CDLMARUBOZU",
            "CDLMATCHINGLOW",
            "CDLMATHOLD",
            "CDLMORNINGDOJISTAR",
            "CDLMORNINGSTAR",
            "CDLONNECK",
            "CDLPIERCING",
            "CDLRICKSHAWMAN",
            "CDLRISEFALL3METHODS",
            "CDLSEPARATINGLINES",
            "CDLSHOOTINGSTAR",
            "CDLSHORTLINE",
            "CDLSPINNINGTOP",
            "CDLSTALLEDPATTERN",
            "CDLSTICKSANDWICH",
            "CDLTAKURI",
            "CDLTASUKIGAP",
            "CDLTHRUSTING",
            "CDLTRISTAR",
            "CDLUNIQUE3RIVER",
            "CDLUPSIDEGAP2CROWS",
            "CDLXSIDEGAP3METHODS",
            "volume_ratio",
            "high_volume_signal",
            "adx_signal",
            "adx_diff",
            "adx_slope_3",
            "adx_slope_5",
            "adx_slope_10",
            "minus_di_signal",
            "minus_di_diff",
            "minus_di_slope_3",
            "minus_di_slope_5",
            "minus_di_slope_10",
            "open_relative_to_fibonacci_level_0",
            "close_relative_to_fibonacci_level_0",
            "high_relative_to_fibonacci_level_0",
            "low_relative_to_fibonacci_level_0",
            "open_relative_to_fibonacci_level_0.236",
            "close_relative_to_fibonacci_level_0.236",
            "high_relative_to_fibonacci_level_0.236",
            "low_relative_to_fibonacci_level_0.236",
            "open_relative_to_fibonacci_level_0.382",
            "close_relative_to_fibonacci_level_0.382",
            "high_relative_to_fibonacci_level_0.382",
            "low_relative_to_fibonacci_level_0.382",
            "open_relative_to_fibonacci_level_0.5",
            "close_relative_to_fibonacci_level_0.5",
            "high_relative_to_fibonacci_level_0.5",
            "low_relative_to_fibonacci_level_0.5",
            "open_relative_to_fibonacci_level_0.618",
            "close_relative_to_fibonacci_level_0.618",
            "high_relative_to_fibonacci_level_0.618",
            "low_relative_to_fibonacci_level_0.618",
            "open_relative_to_fibonacci_level_0.786",
            "close_relative_to_fibonacci_level_0.786",
            "high_relative_to_fibonacci_level_0.786",
            "low_relative_to_fibonacci_level_0.786",
            "open_relative_to_fibonacci_level_1",
            "close_relative_to_fibonacci_level_1",
            "high_relative_to_fibonacci_level_1",
            "low_relative_to_fibonacci_level_1",
            "half_range_bb",
            "high_relative_position_bb",
            "low_relative_position_bb",
            "open_relative_position_bb",
            "close_relative_position_bb",
            "normalized_half_range_bb",
            "upper_band_slope_bb",
            "lower_band_slope_bb",
            "slope_indicator_bb",
            "slope_w_range_bb",
            "open_relative_to_tenkan_sen",
            "close_relative_to_tenkan_sen",
            "high_relative_to_tenkan_sen",
            "low_relative_to_tenkan_sen",
            "open_relative_to_kijun_sen",
            "close_relative_to_kijun_sen",
            "high_relative_to_kijun_sen",
            "low_relative_to_kijun_sen",
            "open_relative_to_senkou_span_a",
            "close_relative_to_senkou_span_a",
            "high_relative_to_senkou_span_a",
            "low_relative_to_senkou_span_a",
            "open_relative_to_senkou_span_b",
            "close_relative_to_senkou_span_b",
            "high_relative_to_senkou_span_b",
            "low_relative_to_senkou_span_b",
            "open_relative_to_chikou_span",
            "close_relative_to_chikou_span",
            "high_relative_to_chikou_span",
            "low_relative_to_chikou_span",
            "tenkan_relative_to_kijun_momentum",
            "senkou_a_relative_to_senkou_b_momentum",
            "rsi_bin_Oversold",
            "rsi_bin_Neutral",
            "rsi_bin_Overbought",
            "macd_bin_Negative",
            "macd_bin_Positive",
            "earnings",
            "days_next_earnings",
        ]

    def preprocess_data(self, data):
        # We'll generate overlapping sequences for each day in the dataset,
        # starting from the 15th day (to ensure each sequence has 14 days).
        sequences = [
            data.iloc[start:end][self.features].to_numpy()
            for start in range(len(data) - self.num_days_per_sequence + 1)
            for end in (start + self.num_days_per_sequence,)
        ]
        data_reshaped = np.array(sequences)
        return data_reshaped

    def postprocess_predictions(self, predictions):
        """
        Post-process predictions to apply the optimal thresholds and convert to class labels.
        """
        # Apply the optimal thresholds to the predictions
        predicted_classes = np.array(
            [
                (
                    np.argmax(row)
                    if max(row) >= self.optimal_thresholds[np.argmax(row)]
                    else 1
                )  # defaulting to class 1 (neutral)
                for row in predictions
            ]
        )
        return predicted_classes

    #     def postprocess_predictions(self, predictions):
    #         # TODO: Add your postprocessing code here
    #         return predictions.argmax(axis=-1)

    def load_data(self, symbol, ds=None, lb_years=5):
        if ds is None:
            ds = dt.date.today()
        ds = pd.to_datetime(ds).date()
        lb = 365 * lb_years

        data_processor = DataProcessor(symbol, end_date=ds, lookback=lb)
        data_processor.load_data()
        data_processor.data["earnings"] = (
            data_processor.data["earnings"].fillna(0).astype(float)
        )
        return data_processor.data

    def predict(self, symbol, ds=None, lb_years=5):
        # Load data
        data = self.load_data(symbol, ds, lb_years)

        # Preprocess the data to get overlapping 14-day sequences
        X_test = self.preprocess_data(data)

        # Make predictions
        predictions = self.model.predict(X_test)

        # Create a DataFrame with predictions and their probabilities
        predictions_df = pd.DataFrame(
            predictions,
            columns=["bull_probability", "neutral_probability", "bear_probability"],
        )

        # Post-process predictions to interpret them
        predictions_df["prediction"] = self.postprocess_predictions(predictions)

        # Align predictions with the corresponding dates in the dataset
        predictions_df.index = data.index[self.num_days_per_sequence - 1 :]

        # Join the predictions DataFrame with the original data DataFrame
        # This will align the prediction for each day with the corresponding date in the original data
        result_df = data.join(predictions_df, how="left")

        return result_df
