import datetime as dt
import os
import talib

## paths
DL_DIR = os.environ["DL_DIR"]
## price
price_dir = DL_DIR + "/fmp/historical-daily-price-full"
stock_avg_buffer = DL_DIR + "/buffer/der-m-avg-price"
avg_price = DL_DIR + "/derived-measurements/avg-price-vol"
# ratios
ratios = DL_DIR + "/derived-measurements/ratios"
industry_ratios = DL_DIR + "/derived-measurements/industry-ratios"
sector_ratios = DL_DIR + "/derived-measurements/sector-ratios"
industry_rating = DL_DIR + "/derived-measurements/industry-rating"
sector_rating = DL_DIR + "/derived-measurements/sector-rating"
sector_ratio_scores = DL_DIR + "/derived-measurements/sector-ratio-scores"
sector_ratio_scores_ticker = DL_DIR + "/derived-measurements/sector-ratio-scores-ticker"
## other
asset_metrics = DL_DIR + "/derived-measurements/asset-metrics"
## technicals
sr_levels = DL_DIR + "/derived-measurements/support-resistance/data"
sr_graphs = DL_DIR + "/derived-measurements/support-resistance/graph"
## swing trades
option_swings_simulations = DL_DIR + "/derived-measurements/option-swings/simulations/"
option_swings = DL_DIR + "/derived-measurements/option-swings/discovery"
option_swings_ml = DL_DIR + "/derived-measurements/option-swings/random-forest-output"
## seasonality map
mbg_seasonality = DL_DIR + "/derived-measurements/modern-blk-girl/seasonality.parquet"
## Ticker inputs for ML
ml_ticker_signals = DL_DIR + "/derived-measurements/ml-signals/ticker-signals"
price_consolidation_sparse = (
    f"{DL_DIR}/derived-measurements/price-consolidation/sparse-matrix"
)
price_consolidation_heatmap = (
    f"{DL_DIR}/derived-measurements/price-consolidation/heatmap"
)
candlestick_graph_prep = f"{DL_DIR}/derived-measurements/candlestick-graph-prep"
enriched_technical_analysis = (
    f"{DL_DIR}/derived-measurements/enriched-technical-analysis"
)
fibonacci_retracement = f"{DL_DIR}/derived-measurements/fibonacci-retracement"
gc_crossover = f"{DL_DIR}/derived-measurements/crossovers/golden-cross"
dc_crossover = f"{DL_DIR}/derived-measurements/crossovers/death-cross"
bull_20d_classifier = f"{DL_DIR}/derived-measurements/bull-20d-classifier"
bear_20d_classifier = f"{DL_DIR}/derived-measurements/bear-20d-classifier"
bull_call_spread_option_analysis = (
    f"{DL_DIR}/derived-measurements/bull-call-spread-option-analysis"
)

## Model paths
MODEL_DIR = f"{DL_DIR.replace('data', 'code')}/models"
gc_model_path = f"{MODEL_DIR}/golden_cross_predictor_model.h5"
dc_model_path = f"{MODEL_DIR}/death_cross_predictor_model.h5"
bull_20d_model_path = f"{MODEL_DIR}/bull_flag_20d_classifier.h5"
bear_20d_model_path = f"{MODEL_DIR}/bear_flag_20d_classifier.h5"

## Data types
ratio_types = {
    "symbol": str,
    "date": dt.date,
    "adjClose": float,
    "netIncome": float,
    "revenue": float,
    "weightedAverageShsOut": float,
    "commonStock": float,
    "totalAssets": float,
    "totalLiabilities": float,
    "dividendsPaid": float,
    "eps": float,
    "pe": float,
    "ps": float,
    "pb": float,
}

industry_types = {
    "industry": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

sector_types = {
    "sector": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

industry_rating_types = {
    "industry": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

sector_rating_types = {
    "sector": str,
    "date": dt.date,
    "count": float,
    "mean": float,
    "std": float,
    "min": float,
    "25%": float,
    "50%": float,
    "75%": float,
    "max": float,
}

asset_metrics_types = {
    "close": float,
    "date": dt.date,
    "open": float,
    "symbol": str,
    "volume": float,
    "close_slope": float,
    "close_avg": float,
    "volume_avg": float,
    "description": str,
    "discount": str,
    "long_term": str,
    "mid_term": str,
    "resistance": float,
    "short_term": str,
    "stop_loss": float,
    "support": float,
    "target_price": float,
    "avg_rating": float,
    "avg_pt": float,
    "n_rating": float,
    "max_date": dt.date,
    "srv_compare": float,
    "slrv_compare": float,
    "growth_rate": float,
}

support_resistance_types = {
    "symbol": str,
    "date": dt.date,
    "open": float,
    "close": float,
    "low": float,
    "high": float,
    "volume": float,
    "support": float,
    "date_int": float,
    "resistance": float,
    "close_avg_5": float,
    "close_avg_13": float,
    "close_avg_50": float,
    "close_avg_200": float,
    "avg_volume": float,
    "range": float,
    "avg_range": float,
}

sector_ratio_types = {
    "date": dt.date,
    "symbol": str,
    "sector": str,
    "eps": float,
    "pe": float,
    "ps": float,
    "pb": float,
    "eps_score": float,
    "pe_score": float,
    "ps_score": float,
    "pb_score": float,
}

## maps
rating_map = {
    "S+": 30,
    "S": 28,
    "S-": 26,
    "A+": 24,
    "A": 22,
    "A-": 20,
    "B+": 18,
    "B": 16,
    "B-": 14,
    "C+": 12,
    "C": 10,
    "C-": 8,
    "D+": 6,
    "D": 4,
    "D-": 2,
}

# ## Candlestick pattern stuff
# ticker_methods = {
#     "CDL2CROWS": talib.CDL2CROWS,
#     "CDL3BLACKCROWS": talib.CDL3BLACKCROWS,
#     "CDL3INSIDE": talib.CDL3INSIDE,
#     "CDL3LINESTRIKE": talib.CDL3LINESTRIKE,
#     "CDL3OUTSIDE": talib.CDL3OUTSIDE,
#     "CDL3STARSINSOUTH": talib.CDL3STARSINSOUTH,
#     "CDL3WHITESOLDIERS": talib.CDL3WHITESOLDIERS,
#     "CDLABANDONEDBABY": talib.CDLABANDONEDBABY,
#     "CDLADVANCEBLOCK": talib.CDLADVANCEBLOCK,
#     "CDLBELTHOLD": talib.CDLBELTHOLD,
#     "CDLBREAKAWAY": talib.CDLBREAKAWAY,
#     "CDLCLOSINGMARUBOZU": talib.CDLCLOSINGMARUBOZU,
#     "CDLCONCEALBABYSWALL": talib.CDLCONCEALBABYSWALL,
#     "CDLCOUNTERATTACK": talib.CDLCOUNTERATTACK,
#     "CDLDARKCLOUDCOVER": talib.CDLDARKCLOUDCOVER,
#     "CDLDOJI": talib.CDLDOJI,
#     "CDLDOJISTAR": talib.CDLDOJISTAR,
#     "CDLDRAGONFLYDOJI": talib.CDLDRAGONFLYDOJI,
#     "CDLENGULFING": talib.CDLENGULFING,
#     "CDLEVENINGDOJISTAR": talib.CDLEVENINGDOJISTAR,
#     "CDLEVENINGSTAR": talib.CDLEVENINGSTAR,
#     "CDLGAPSIDESIDEWHITE": talib.CDLGAPSIDESIDEWHITE,
#     "CDLGRAVESTONEDOJI": talib.CDLGRAVESTONEDOJI,
#     "CDLHAMMER": talib.CDLHAMMER,
#     "CDLHANGINGMAN": talib.CDLHANGINGMAN,
#     "CDLHARAMI": talib.CDLHARAMI,
#     "CDLHARAMICROSS": talib.CDLHARAMICROSS,
#     "CDLHIGHWAVE": talib.CDLHIGHWAVE,
#     "CDLHIKKAKE": talib.CDLHIKKAKE,
#     "CDLHIKKAKEMOD": talib.CDLHIKKAKEMOD,
#     "CDLHOMINGPIGEON": talib.CDLHOMINGPIGEON,
#     "CDLIDENTICAL3CROWS": talib.CDLIDENTICAL3CROWS,
#     "CDLINNECK": talib.CDLINNECK,
#     "CDLINVERTEDHAMMER": talib.CDLINVERTEDHAMMER,
#     "CDLKICKING": talib.CDLKICKING,
#     "CDLKICKINGBYLENGTH": talib.CDLKICKINGBYLENGTH,
#     "CDLLADDERBOTTOM": talib.CDLLADDERBOTTOM,
#     "CDLLONGLEGGEDDOJI": talib.CDLLONGLEGGEDDOJI,
#     "CDLLONGLINE": talib.CDLLONGLINE,
#     "CDLMARUBOZU": talib.CDLMARUBOZU,
#     "CDLMATCHINGLOW": talib.CDLMATCHINGLOW,
#     "CDLMATHOLD": talib.CDLMATHOLD,
#     "CDLMORNINGDOJISTAR": talib.CDLMORNINGDOJISTAR,
#     "CDLMORNINGSTAR": talib.CDLMORNINGSTAR,
#     "CDLONNECK": talib.CDLONNECK,
#     "CDLPIERCING": talib.CDLPIERCING,
#     "CDLRICKSHAWMAN": talib.CDLRICKSHAWMAN,
#     "CDLRISEFALL3METHODS": talib.CDLRISEFALL3METHODS,
#     "CDLSEPARATINGLINES": talib.CDLSEPARATINGLINES,
#     "CDLSHOOTINGSTAR": talib.CDLSHOOTINGSTAR,
#     "CDLSHORTLINE": talib.CDLSHORTLINE,
#     "CDLSPINNINGTOP": talib.CDLSPINNINGTOP,
#     "CDLSTALLEDPATTERN": talib.CDLSTALLEDPATTERN,
#     "CDLSTICKSANDWICH": talib.CDLSTICKSANDWICH,
#     "CDLTAKURI": talib.CDLTAKURI,
#     "CDLTASUKIGAP": talib.CDLTASUKIGAP,
#     "CDLTHRUSTING": talib.CDLTHRUSTING,
#     "CDLTRISTAR": talib.CDLTRISTAR,
#     "CDLUNIQUE3RIVER": talib.CDLUNIQUE3RIVER,
#     "CDLUPSIDEGAP2CROWS": talib.CDLUPSIDEGAP2CROWS,
#     "CDLXSIDEGAP3METHODS": talib.CDLXSIDEGAP3METHODS,
# }
# patterns_dict = {
#     "CDL3BLACKCROWS": [3, "Reversal", "Three Black Crows"],
#     "CDL3INSIDE": [3, "Reversal", "Three Inside Up/Down"],
#     "CDL3LINESTRIKE": [4, "Continuation, Reversal", "Three-Line Strike"],
#     "CDL3OUTSIDE": [3, "Reversal", "Three Outside Up/Down"],
#     "CDL3WHITESOLDIERS": [3, "Reversal", "Three Advancing White Soldiers"],
#     "CDLABANDONEDBABY": [3, "Reversal", "Abandoned Baby"],
#     "CDLDARKCLOUDCOVER": [2, "Reversal, Bearish", "Dark Cloud Cover"],
#     "CDLDOJI": [1, "Neutral, Indecision", "Doji"],
#     "CDLDRAGONFLYDOJI": [1, "Reversal, Bullish", "Dragonfly Doji"],
#     "CDLENGULFING": [2, "Reversal", "Engulfing Pattern"],
#     "CDLEVENINGDOJISTAR": [2, "Reversal, Bearish", "Evening Doji Star"],
#     "CDLEVENINGSTAR": [3, "Reversal, Bearish", "Evening Star"],
#     "CDLGRAVESTONEDOJI": [1, "Reversal, Bearish", "Gravestone Doji"],
#     "CDLHAMMER": [1, "Reversal, Bullish", "Hammer"],
#     "CDLHANGINGMAN": [1, "Reversal, Bearish (waning buying power)", "Hanging Man"],
#     "CDLHARAMI": [2, "Reversal", "Harami Pattern"],
#     "CDLINVERTEDHAMMER": [1, "Reversal, Bullish", "Inverted Hammer"],
#     "CDLMARUBOZU": [1, "Neutral", "Marubozu"],
#     "CDLMORNINGDOJISTAR": [2, "Reversal, Bullish", "Morning Doji Star"],
#     "CDLMORNINGSTAR": [3, "Reversal, Bullish", "Morning Star"],
#     "CDLPIERCING": [2, "Reversal, Bullish", "Piercing Pattern"],
#     "CDLSHOOTINGSTAR": [1, "Reversal, Bearish", "Shooting Star"],
#     "CDLSPINNINGTOP": [1, "Neutral, Indecision", "Spinning Top"],
# }
# candle_names = [x.lower() for x in list(patterns_dict.keys())]

# Candlestick pattern names
candle_names = [
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
]

bull_20d_features = [
    "rsi_oversold",
    "macd_positive",
    "volume_relative_strength",
    "moving_avg_crossover",
    "price_breakout",
    "divergence",
    "candlestick_pattern",
    "bollinger_band_flag",
    "keltner_channel_flag",
    "minus_di_diff_flag",
    "minus_di_signal_flag",
    "minus_di_slope_10_flag",
    "minus_di_slope_3_flag",
    "minus_di_slope_5_flag",
    "adx_diff_flag",
    "adx_signal_flag",
    "adx_slope_10_flag",
    "adx_slope_3_flag",
    "adx_slope_5_flag",
    "price_below_lower_bb",
    "tenkan_sen_flag",
    "kijun_sen_flag",
    "senkou_span_a_flag",
    "senkou_span_b_flag",
    "chikou_span_flag",
]

bear_20d_features = [
    "rsi_overbought",
    "macd_negative",
    "volume_relative_weakness",
    "moving_avg_crossover",
    "price_breakdown",
    "divergence",
    "candlestick_pattern",
    "bollinger_band_flag",
    "keltner_channel_flag",
    "minus_di_diff_flag",
    "minus_di_signal_flag",
    "minus_di_slope_10_flag",
    "minus_di_slope_3_flag",
    "minus_di_slope_5_flag",
    "adx_diff_flag",
    "adx_signal_flag",
    "adx_slope_10_flag",
    "adx_slope_3_flag",
    "adx_slope_5_flag",
    "price_above_upper_bb",
    "tenkan_sen_flag",
    "kijun_sen_flag",
    "senkou_span_a_flag",
    "senkou_span_b_flag",
    "chikou_span_flag",
]
