import pandas as pd
import tda.settings as tda_s
import derived_metrics.settings as der_s
import common.utils as utils
import os
import datetime as dt


def is_option_in_the_money(put_call, strike_price, underlying_price):
    if put_call == "PUT":
        return strike_price > underlying_price
    elif put_call == "CALL":
        return strike_price < underlying_price
    else:
        raise ValueError("Invalid option type. Must be 'PUT' or 'CALL'.")


def filter_strategy(df, min_exp=30, max_exp=45):
    return df.loc[
        ((df["daysToExpiration"] >= min_exp) & (df["daysToExpiration"] <= max_exp))
    ]


def identify_target_strike_prices(option_chain_data, num_levels):
    sorted_data = option_chain_data.sort_values("openInterest", ascending=False)
    target_strike_prices = sorted_data["strikePrice"].head(num_levels).values
    return target_strike_prices


def generate_bull_call_legs(target_strike_prices):
    bull_call_spreads = []
    sorted_strike_prices = sorted(target_strike_prices)

    for i in range(len(sorted_strike_prices) - 1):
        for j in range(i + 1, len(sorted_strike_prices)):
            buy_strike = sorted_strike_prices[i]
            sell_strike = sorted_strike_prices[j]

            if buy_strike != sell_strike:
                spread = {"buy_strike": buy_strike, "sell_strike": sell_strike}
                bull_call_spreads.append(spread)

    return pd.DataFrame(bull_call_spreads)


def merge_option_data(spread_legs_base, option_data):
    merged_data = spread_legs_base.merge(
        option_data[
            [
                "strikePrice",
                "daysToExpiration",
                "bid",
                "delta",
                "gamma",
                "theta",
                "vega",
                "volatility",
                "itm",
            ]
        ].rename(
            columns={
                "strikePrice": "buy_strike",
                "bid": "buy_premium",
                "delta": "buy_delta",
                "gamma": "buy_gamma",
                "theta": "buy_theta",
                "vega": "buy_vega",
                "volatility": "buy_volatility",
                "itm": "buy_itm",
            }
        ),
        how="left",
        on="buy_strike",
    ).merge(
        option_data[
            [
                "strikePrice",
                "daysToExpiration",
                "ask",
                "delta",
                "gamma",
                "theta",
                "vega",
                "volatility",
                "itm",
            ]
        ].rename(
            columns={
                "strikePrice": "sell_strike",
                "ask": "sell_premium",
                "delta": "sell_delta",
                "gamma": "sell_gamma",
                "theta": "sell_theta",
                "vega": "sell_vega",
                "volatility": "sell_volatility",
                "itm": "sell_itm",
            }
        ),
        how="left",
        on=["sell_strike", "daysToExpiration"],
    )

    return merged_data


def calculate_profit_metrics(spread_legs_premiums):
    spread_legs_premiums["max_loss"] = (
        spread_legs_premiums["buy_premium"] - spread_legs_premiums["sell_premium"]
    )
    spread_legs_premiums["max_gain"] = (
        spread_legs_premiums["sell_strike"]
        - spread_legs_premiums["buy_strike"]
        - spread_legs_premiums["max_loss"]
    )
    spread_legs_premiums["breakeven"] = (
        spread_legs_premiums["buy_strike"] + spread_legs_premiums["max_loss"]
    )
    return spread_legs_premiums


def calculate_probability_of_success(data):
    data["both_options_70_delta"] = (data["buy_delta"] > 0.7) & (
        data["sell_delta"] > 0.7
    )
    data["consolidated_delta"] = (data["buy_delta"] - data["sell_delta"]) > 0.7
    return data


def add_iv_flag(data, min_iv):
    data["high_iv_flag"] = data["buy_volatility"] >= min_iv
    return data


def add_rr_ratio_flag(data, min_reward_ratio):
    data["rr_ratio_flag"] = data["max_gain"] >= min_reward_ratio * data["max_loss"]
    return data


def add_min_credit_flag(data, min_credit_ratio):
    data["min_credit"] = min_credit_ratio * (data["sell_strike"] - data["buy_strike"])
    data["min_credit_flag"] = data["sell_premium"] >= min_credit_ratio * (
        data["sell_strike"] - data["buy_strike"]
    )
    return data


def add_strike_price_flag(data, underlying_price, num_steps):
    data["strike_price_flag"] = (data["sell_strike"] > underlying_price + num_steps) & (
        data["buy_strike"] <= underlying_price
    )
    return data


def pipeline(symbol, ds):
    fn = f"{tda_s.OPTIONS_ANALYTICAL_NEW}/{ds}/{symbol}_calls.parquet"
    if not os.path.isfile(fn):
        return False  ## no options data for this symbol

    # read data and clean
    option_df = pd.read_parquet(fn)
    option_df = option_df.sort_values("collected", ascending=False).drop_duplicates(
        "description"
    )
    option_df = option_df.loc[abs(option_df["delta"]) <= 1]
    option_df["itm"] = [
        is_option_in_the_money(x[0], x[1], x[2])
        for x in option_df[["putCall", "strikePrice", "underlyingPrice"]].values
    ]

    # filter and get values
    option_df_subset = filter_strategy(option_df)
    if len(option_df_subset) == 0:
        return False  ## no data
    underlyingPrice = option_df_subset["underlyingPrice"].values[0]
    collected_at = option_df_subset["collected"].values[0]
    target_strike_prices = identify_target_strike_prices(option_df_subset, 10)
    # make base and flags
    spread_legs_base = generate_bull_call_legs(target_strike_prices)
    spread_legs_premiums = merge_option_data(spread_legs_base, option_df_subset)
    spread_legs_full = calculate_profit_metrics(spread_legs_premiums)
    spread_legs_full_success = calculate_probability_of_success(spread_legs_full)
    spread_legs_full_success_iv = add_iv_flag(spread_legs_full_success, min_iv=30)
    spread_legs_full_success_iv_ratio = add_rr_ratio_flag(
        spread_legs_full_success_iv, min_reward_ratio=1.5
    )
    spread_legs_full_success_iv_ratio_credit = add_min_credit_flag(
        spread_legs_full_success_iv_ratio, 30
    )
    flag_cols = [
        "both_options_70_delta",
        "consolidated_delta",
        "high_iv_flag",
        "rr_ratio_flag",
        "min_credit_flag",
        "strike_price_flag",
    ]
    final_df = add_strike_price_flag(
        spread_legs_full_success_iv_ratio_credit, underlyingPrice, num_steps=2
    )
    final_df["flag_sum"] = final_df[flag_cols].sum(1)
    final_df = final_df.sort_values("flag_sum", ascending=False)
    final_df["underlyingPrice"] = underlyingPrice

    # write
    collected_at = (
        str(option_df_subset["collected"].values[0]).split(".")[0].replace(":", "--")
    )
    write_fn = (
        f"{der_s.bull_call_spread_option_analysis}/{ds}/{symbol}_{collected_at}.parquet"
    )
    final_df.to_parquet(write_fn, index=False)


def watch_list_pipeline(ds: dt.date):
    """
    Complete bull call analysis for all tickers in pipeline.
    This will complete the analysis for the most recent timestamp
    for the given date.

    Inputs:
        - ds: Date of analysis
    """
    ds = pd.to_datetime(ds).date()
    ## make directory, it not exist
    path = f"{der_s.bull_call_spread_option_analysis}/{ds}"
    if not os.path.isdir(path):
        os.mkdir(path)

    watch_list = utils.get_watchlist(extend=True)
    [pipeline(symbol, ds) for symbol in watch_list]
