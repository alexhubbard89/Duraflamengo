from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)
from scipy.stats import randint
import requests
import pandas as pd
import numpy as np
import datetime as dt
import pickle
import derived_metrics.settings as der_s
import common.settings as common_s


def add_bear_put_spread_flags(enriched_data):
    flag_cols = [
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

    # Define the candlestick patterns you want to filter
    candlestick_patterns = [
        "CDLDOJI",
        "CDLHAMMER",
        "CDLENGULFING",
        "CDLSHOOTINGSTAR",
        "CDLHARAMI",
        "CDLMORNINGSTAR",
        "CDLEVENINGSTAR",
    ]

    rsi_threshold = 70  # RSI threshold for overbought condition
    macd_histogram_threshold = 0  # MACD histogram threshold for negative histogram

    # Define the additional filters
    volume_filter = (
        enriched_data["average_volume"] < 0.5 * enriched_data["average_volume"].mean()
    )
    relative_weakness_filter = enriched_data["rsi"] > 65
    moving_average_crossover_filter = (
        enriched_data["close_avg_20"] < enriched_data["close_avg_50"]
    )
    price_breakdown_filter = (
        enriched_data["close"] < enriched_data["support"].rolling(window=5).max()
    )
    divergence_filter = (
        enriched_data["rsi"] > enriched_data["rsi"].rolling(window=25).mean()
    )

    # Create flags for each condition
    enriched_data["rsi_overbought"] = enriched_data["rsi"] > rsi_threshold
    enriched_data["macd_negative"] = (
        enriched_data["macdHist"] < macd_histogram_threshold
    )
    enriched_data["volume_relative_weakness"] = volume_filter & relative_weakness_filter
    enriched_data["moving_avg_crossover"] = moving_average_crossover_filter
    enriched_data["price_breakdown"] = price_breakdown_filter
    enriched_data["divergence"] = divergence_filter
    enriched_data["candlestick_pattern"] = (
        enriched_data[candlestick_patterns].sum(axis=1) > 0
    )
    enriched_data["bollinger_band_flag"] = (
        enriched_data["close"] < enriched_data["upper_band_bb"]
    ) & (enriched_data["close"] > enriched_data["middle_band_bb"])
    enriched_data["keltner_channel_flag"] = (
        enriched_data["close"] > enriched_data["upper_channel"]
    ) & (enriched_data["close"] < enriched_data["upper_band_bb"])
    enriched_data["minus_di_diff_flag"] = enriched_data["minus_di_diff"] > 0.0
    enriched_data["minus_di_signal_flag"] = enriched_data["minus_di_signal"] > 0.0
    enriched_data["minus_di_slope_10_flag"] = enriched_data["minus_di_slope_10"] > 0.0
    enriched_data["minus_di_slope_3_flag"] = enriched_data["minus_di_slope_3"] > 0.0
    enriched_data["minus_di_slope_5_flag"] = enriched_data["minus_di_slope_5"] > 0.0
    enriched_data["adx_diff_flag"] = enriched_data["adx_diff"] > 0.0
    enriched_data["adx_signal_flag"] = enriched_data["adx_signal"] > 0.0
    enriched_data["adx_slope_10_flag"] = enriched_data["adx_slope_10"] > 0.0
    enriched_data["adx_slope_3_flag"] = enriched_data["adx_slope_3"] > 0.0
    enriched_data["adx_slope_5_flag"] = enriched_data["adx_slope_5"] > 0.0
    enriched_data["price_above_upper_bb"] = (
        enriched_data["close"].rolling(window=5).max() > enriched_data["upper_band_bb"]
    )

    # Ichimoku Cloud signals
    enriched_data["tenkan_sen_flag"] = (
        enriched_data["close"] < enriched_data["tenkan_sen"]
    )
    enriched_data["kijun_sen_flag"] = (
        enriched_data["close"] < enriched_data["kijun_sen"]
    )
    enriched_data["senkou_span_a_flag"] = (
        enriched_data["close"] < enriched_data["senkou_span_a"]
    )
    enriched_data["senkou_span_b_flag"] = (
        enriched_data["close"] < enriched_data["senkou_span_b"]
    )
    enriched_data["chikou_span_flag"] = (
        enriched_data["close"] < enriched_data["chikou_span"]
    )

    # Calculate the sum of all flag columns
    enriched_data["flag_sum"] = enriched_data[flag_cols].sum(axis=1)
    enriched_data["flag_percent"] = enriched_data["flag_sum"] / len(flag_cols)

    # Subset the DataFrame to include the flag columns and the sum column
    return enriched_data[
        ["symbol", "date", "close"] + flag_cols + ["flag_sum", "flag_percent"]
    ]


def create_bearish_flag(close: pd.Series, window=20, threshold=0.05):
    """
    Create the class variable 'bearish_flag' based on the percentage change in price over a rolling window.
    If the percentage change exceeds the threshold, set the flag to 1 (bearish), otherwise set it to 0 (non-bearish).

    Args:
        enriched_data (pd.DataFrame): DataFrame containing the enriched data.
        window (int): Size of the rolling window.
        threshold (float): Threshold value for determining bearishness.

    Returns:
        pd.Series: Series with the 'bearish_flag' column added.

    """
    shift_df = pd.DataFrame(close)
    for i in range(1, window + 1):
        tmp = pd.DataFrame(close.shift(-i)).rename(columns={"close": f"shift_{i}"})
        shift_df = shift_df.join(tmp, how="outer", rsuffix=f"_{i}")
    value_matrix = shift_df.values
    compare_results = [np.divide(x[1:], x[0]) for x in value_matrix]
    threshold_compare = [np.where(x <= (1 - threshold)) for x in compare_results]
    return pd.Series(
        [x[0][0] + 1 if len(x[0]) else -1 for x in threshold_compare], index=close.index
    )


def train():
    """
    This is the code used to generate the current model.
    This is purely information for documentation purposes.
    Raw code found in: prototyping/marty/vertical-spreads/bear_call_spread_analysis.ipynb
    """
    do_not_run = True
    if do_not_run:
        return False
    # Load data
    ds = dt.date(2019, 1, 1)

    params = {
        "read_method": "parquet",
        "path": "enriched_technical_analysis",
        "params": {
            "evaluation": "gt",
            "slice": str(ds),
            "column": "date",
            "date_type": True,
        },
    }
    r = requests.post(common_s.LOCAL_API + "read_data", json=params)
    all_enriched_data = pd.DataFrame(r.json()).reset_index(drop=True)
    all_enriched_data["date"] = pd.to_datetime(all_enriched_data["date"]).apply(
        lambda r: r.date()
    )

    ## feature engineering
    bearish_training_df = all_enriched_data.groupby("symbol").apply(
        add_bear_put_spread_flags
    )
    bearish_training_df

    ## add class flags
    bearish_training_df["days_to_bearish"] = bearish_training_df.groupby("symbol")[
        "close"
    ].apply(create_bearish_flag)
    bearish_training_df["bearish_flag"] = (
        bearish_training_df["days_to_bearish"].apply(lambda r: r > 0).astype(int)
    )
    # Calculate the date corresponding to the number of days until bearish
    bearish_training_df["resulting_date"] = np.where(
        bearish_training_df["days_to_bearish"] == -1,
        pd.NaT,  # Return NaT (Not a Time) for days_to_bearish == -1
        bearish_training_df["date"]
        + pd.to_timedelta(bearish_training_df["days_to_bearish"], unit="D"),
    )
    # split validation
    ds_cut = dt.date(2023, 1, 1)
    extended_training = bearish_training_df.loc[bearish_training_df["date"] < ds_cut]
    extended_validation = bearish_training_df.loc[bearish_training_df["date"] >= ds_cut]

    # Define the parameter grid for the randomized search
    param_grid = {
        "hidden_layer_sizes": [(100, 50), (50, 50), (100,)],
        "activation": ["relu", "sigmoid", "tanh"],
        "alpha": [0.0001, 0.001, 0.01],
        "learning_rate": ["constant", "adaptive", "invscaling"],
        "learning_rate_init": [0.001, 0.01, 0.1],
        "batch_size": [16, 32, 64],
        "max_iter": randint(100, 1000),
    }

    # Create the MLPClassifier model
    mlp_classifier = MLPClassifier(random_state=42)

    X, y = extended_training[der_s.bear_20d_features], extended_training["bearish_flag"]

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Perform randomized search with cross-validation
    randomized_search = RandomizedSearchCV(
        mlp_classifier, param_distributions=param_grid, n_iter=10, cv=5, random_state=42
    )
    randomized_search.fit(X_train, y_train)

    # Get the best model
    best_classifier = randomized_search.best_estimator_

    # Make predictions on the test set
    y_pred = best_classifier.predict(X_test)

    # Calculate evaluation metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    # Print evaluation metrics
    print("Evaluation Metrics:")
    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1 Score: {f1}")

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        extended_training[der_s.bear_20d_features],
        extended_training["bearish_flag"],
        test_size=0.2,
        random_state=42,
    )

    # Split the extended validation data into X_val, y_val
    X_val = extended_validation[der_s.bear_20d_features]
    y_val = extended_validation["bearish_flag"]

    # Create and train the neural network model
    # mlp_bear_classifier = MLPClassifier(hidden_layer_sizes=(100, 50), random_state=42)

    # Define the MLPClassifier model with adjustable parameters
    mlp_bear_classifier = MLPClassifier(
        hidden_layer_sizes=(100,),  # Adjust the hidden layer sizes as desired
        activation="tanh",  # Try different activation functions ('relu', 'sigmoid', 'tanh')
        alpha=0.0001,  # Adjust regularization strength (alpha) if needed
        learning_rate="constant",  # Try different learning_rate options ('constant', 'adaptive', 'invscaling')
        learning_rate_init=0.001,  # Adjust the learning rate
        batch_size=64,  # Try different batch sizes ('auto', integer value)
        max_iter=714,  # Maximum number of training iterations
        random_state=42,
    )
    mlp_bear_classifier.fit(X_train, y_train)

    # Make predictions on the extended validation and testing sets
    y_val_pred = mlp_bear_classifier.predict(X_val)
    y_val_pred_proba = mlp_bear_classifier.predict_proba(X_val)[:, 1]
    y_test_pred = mlp_bear_classifier.predict(X_test)
    y_test_pred_proba = mlp_bear_classifier.predict_proba(X_test)[:, 1]

    # Calculate evaluation metrics for extended validation
    val_accuracy = accuracy_score(y_val, y_val_pred)
    val_precision = precision_score(y_val, y_val_pred)
    val_recall = recall_score(y_val, y_val_pred)
    val_f1 = f1_score(y_val, y_val_pred)

    # Calculate the confusion matrix for extended validation
    val_cm = confusion_matrix(y_val, y_val_pred)
    # val_cm_percent = (val_cm / val_cm.sum(axis=0)[:, np.newaxis]) * 100
    val_cm_percent = [
        [
            (val_cm[0][0] / val_cm.sum(axis=0)[0]) * 100,
            (val_cm[0][1] / val_cm.sum(axis=0)[1]) * 100,
        ],
        [
            (val_cm[1][0] / val_cm.sum(axis=0)[0]) * 100,
            (val_cm[1][1] / val_cm.sum(axis=0)[1]) * 100,
        ],
    ]

    # Calculate evaluation metrics for testing set
    test_accuracy = accuracy_score(y_test, y_test_pred)
    test_precision = precision_score(y_test, y_test_pred)
    test_recall = recall_score(y_test, y_test_pred)
    test_f1 = f1_score(y_test, y_test_pred)

    # Calculate the confusion matrix for testing set
    test_cm = confusion_matrix(y_test, y_test_pred)
    # test_cm_percent = (test_cm / test_cm.sum(axis=0)[:, np.newaxis]) * 100
    test_cm_percent = [
        [
            (test_cm[0][0] / test_cm.sum(axis=0)[0]) * 100,
            (test_cm[0][1] / test_cm.sum(axis=0)[1]) * 100,
        ],
        [
            (test_cm[1][0] / test_cm.sum(axis=0)[0]) * 100,
            (test_cm[1][1] / test_cm.sum(axis=0)[1]) * 100,
        ],
    ]

    # Print evaluation metrics for extended validation
    print("Extended Validation Metrics:")
    print(f"Accuracy: {val_accuracy}")
    print(f"Precision: {val_precision}")
    print(f"Recall: {val_recall}")
    print(f"F1 Score: {val_f1}")

    # Print evaluation metrics for testing set
    print("Testing Metrics:")
    print(f"Accuracy: {test_accuracy}")
    print(f"Precision: {test_precision}")
    print(f"Recall: {test_recall}")
    print(f"F1 Score: {test_f1}")

    # Get feature importances for the neural network model
    feature_importances = np.abs(mlp_bear_classifier.coefs_[0])

    # Flatten the feature importances array
    feature_importances = np.ravel(feature_importances)

    # Create a list of (feature, importance) pairs
    feature_importance_pairs = list(zip(der_s.bear_20d_features, feature_importances))

    # Sort the feature-importance pairs by importance in descending order
    sorted_feature_importances = sorted(
        feature_importance_pairs, key=lambda x: x[1], reverse=True
    )

    # Print sorted feature importances for extended validation
    print("\nExtended Validation Feature Importances (sorted):")
    for feature, importance in sorted_feature_importances:
        print(f"{feature}: {importance}")

    # # Plot the confusion matrix for extended validation (raw values)
    # plt.figure(figsize=(8, 6))
    # sns.heatmap(val_cm, annot=True, fmt="d", cmap="Blues")
    # plt.xlabel("Predicted Label")
    # plt.ylabel("True Label")
    # plt.title("Extended Validation Confusion Matrix (Raw Values)")

    # # Plot the confusion matrix for extended validation (percentages)
    # plt.figure(figsize=(8, 6))
    # sns.heatmap(val_cm_percent, annot=True, fmt=".2f", cmap="Blues")
    # plt.xlabel("Predicted Label")
    # plt.ylabel("True Label")
    # plt.title("Extended Validation Confusion Matrix (Percentages)")

    # # Plot the confusion matrix for testing set (raw values)
    # plt.figure(figsize=(8, 6))
    # sns.heatmap(test_cm, annot=True, fmt="d", cmap="Blues")
    # plt.xlabel("Predicted Label")
    # plt.ylabel("True Label")
    # plt.title("Testing Confusion Matrix (Raw Values)")

    # # Plot the confusion matrix for testing set (percentages)
    # plt.figure(figsize=(8, 6))
    # sns.heatmap(test_cm_percent, annot=True, fmt=".2f", cmap="Blues")
    # plt.xlabel("Predicted Label")
    # plt.ylabel("True Label")
    # plt.title("Testing Confusion Matrix (Percentages)")

    # # Show the plots
    # plt.show()


def daily_predict(ds: str, cutoff: float = 0.6) -> None:
    """
    Predict all ticker symbols that have been enriched through
    wit technical analysis signals.

    Args:
        ds (str): The target date for prediction in 'YYYY-MM-DD' format.
        cutoff (float): The probability threshold for classifying as bearish.

    Returns:
        None
    """
    # Load the bear Flag 20 Days Out Classifier
    with open(der_s.bear_20d_model_path, "rb") as file:
        bear_flag_20d_classifier = pickle.load(file)

    if not ds:
        ds = dt.date.today()
    ds = pd.to_datetime(ds).date()
    ds_min = ds - dt.timedelta(540)
    ds_predict_min = ds - dt.timedelta(365)
    params = {
        "read_method": "parquet",
        "path": "enriched_technical_analysis",
        "params": {
            "evaluation": "gt_e",
            "slice": str(ds_min),
            "column": "date",
            "date_type": True,
        },
    }
    r = requests.post(common_s.LOCAL_API + "read_data", json=params)
    daily_predict_df = pd.DataFrame(r.json()).reset_index(drop=True)
    daily_predict_df["date"] = pd.to_datetime(daily_predict_df["date"]).apply(
        lambda r: r.date()
    )

    ## feature engineering
    daily_predict_df_w_features = daily_predict_df.groupby("symbol").apply(
        add_bear_put_spread_flags
    )
    ## add class flag
    daily_predict_df_w_features[
        "days_to_bearish"
    ] = daily_predict_df_w_features.groupby("symbol")["close"].apply(
        create_bearish_flag
    )
    daily_predict_df_w_features["bearish_flag"] = (
        daily_predict_df_w_features["days_to_bearish"]
        .apply(lambda r: r > 0)
        .astype(int)
    )
    # Calculate the date corresponding to the number of days until bearish
    daily_predict_df_w_features["resulting_date"] = np.where(
        daily_predict_df_w_features["days_to_bearish"] == -1,
        pd.NaT,  # Return NaT (Not a Time) for days_to_bearish == -1
        daily_predict_df_w_features["date"]
        + pd.to_timedelta(daily_predict_df_w_features["days_to_bearish"], unit="D"),
    )
    daily_predict_df_w_features = daily_predict_df_w_features.loc[
        daily_predict_df_w_features["date"] >= ds_predict_min
    ]

    ## predict
    X_daily = daily_predict_df_w_features[der_s.bear_20d_features]
    daily_predict_df_w_features[
        "bear_prob_20d"
    ] = bear_flag_20d_classifier.predict_proba(X_daily)[:, 1]
    daily_predict_df_w_features["bear_class_20d"] = np.where(
        daily_predict_df_w_features["bear_prob_20d"] >= cutoff, 1, 0
    )

    daily_predict_df_w_features.to_parquet(
        f"{der_s.bear_20d_classifier}/v0/data.parquet", index=False
    )
