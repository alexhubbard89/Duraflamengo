import requests
import pandas as pd
import os
import datetime as dt
import analysis.settings as analysis_s


def load_puts_discover_data(ds: dt.date) -> pd.DataFrame:
    """
    Load PUTS data to perform credit spread analysis.

    Inputs: Date to analyze.
    Return: PUTS discovery data
    """
    ## Use Marty to request data
    params = {
        "read_method": "parquet",
        "path": "analysis_put_options",
        "subdir": f"{str(ds)}.parquet",
    }
    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    return pd.DataFrame(r.json())


def load_calls_discover_data(ds: dt.date) -> pd.DataFrame:
    """
    Load CALLS data to perform credit spread analysis.

    Inputs: Date to analyze.
    Return: CALLS discovery data
    """
    ## Use Marty to request data
    params = {
        "read_method": "parquet",
        "path": "analysis_call_options",
        "subdir": f"{str(ds)}.parquet",
    }
    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    return pd.DataFrame(r.json())


def read_option_contract(ds: dt.date, ticker: str, option_type: str) -> pd.DataFrame:
    """
    Read in option contracts for a given ticker, on a given day,
    and for a given contract type.

    Inputs:
        - Date
        - Ticker symbol
        - Option type [calls, puts]

    Return: Dataframe to describe option contracts.
    """
    params = {
        "read_method": "parquet",
        "path": "tda_options",
        "subdir": f"{str(ds)}/{ticker}_{option_type}.parquet",
    }
    r = requests.post(os.environ["LOCAL_API"] + "read_data", json=params)
    df = pd.DataFrame(r.json()).sort_values(["daysToExpiration", "strikePrice"])
    df["ask_diff"] = df.groupby(["daysToExpiration"])["ask"].diff()
    df["ask_diff_2"] = df.groupby(["daysToExpiration"])["ask"].diff(2)
    df["ask_diff_3"] = df.groupby(["daysToExpiration"])["ask"].diff(3)

    ## clean ticker column
    df["symbol"] = df["symbol"].apply(lambda r: r.split("_")[0])

    ## bing in discover data
    if option_type.lower() == "calls":
        # BEAR position - I believe the price will not go higher than the observed ceiling.
        df = df.merge(
            calls_discovery_df[["close", "support", "resistance", "symbol"]],
            how="left",
            on="symbol",
        )
        df["option_position"] = "BEAR_CALL"
    elif option_type.lower() == "puts":
        # BULL position - I believe the price will not go lower than the observed floor.
        df = df.merge(
            puts_discovery_df[["close", "support", "resistance", "symbol"]],
            how="left",
            on="symbol",
        )
        df["option_position"] = "BULL_PUT"

    ## add support resistance comparisons
    df["support_compare"] = df["strikePrice"] / df["support"]
    df["resistance_compare"] = df["strikePrice"] / df["resistance"]
    return df


def check_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Subset option dataframe by liquidity filter.

    Input: Option dataframe.
    Returns: Dataframe that fits liquidity criteria.
    """
    ## Measure liquidity
    # subset, division by 0
    df_subset = df.loc[df["askSize"] > 0].copy()
    df_subset["ask_bid_diff"] = (df_subset["ask"] / df_subset["bid"]) - 1
    df_subset["demand_v_supply"] = df_subset["bidSize"] / df_subset["askSize"]

    ## met liquidity
    df_subset = df_subset.loc[df_subset["ask_bid_diff"] < 0.10].copy()
    return df_subset


def check_expiration(
    df: pd.DataFrame, min_ex: int = 15, max_ex: int = 50
) -> pd.DataFrame:
    """
    Subset dataframe for options contracts within expiration range.

    Input: Option contract dataframe
    Return: Subset of option contracts that fit experiation range.
    """
    return df.loc[
        ((df["daysToExpiration"] >= min_ex) & (df["daysToExpiration"] <= max_ex))
    ]


def determine_spread_strike(
    df: pd.DataFrame, option_type: str, strike_threshold: float = 0.05
) -> pd.DataFrame:
    """
    Subset dataframe for options contracts to by distance from support
    or resistance, depending on the option type.

    Inputs:
        - Option contract dataframe
        - Option type [calls, puts]
        - The tolerance allowed for support/resistance comparison.
    Return: Subset-dataframe to describe option contracts.
    """
    ## bing in discover data
    if option_type.lower() == "calls":
        # BEARISH: I want this to declince and be need the ceilining.
        # betting price goes down
        # Since near resistance, I'm ok with a little over ceiling
        return df.loc[df["resistance_compare"] >= (1 - strike_threshold)]
    elif option_type.lower() == "puts":
        # BULLISH: I want this to increase and be need the floor.
        # betting price goes up
        # Since near support, I'm ok with a little under floor
        return df.loc[df["support_compare"] <= (1 + strike_threshold)]


def discovery_pipeline(ds: dt.date):
    """
    Look for options contracts that fit my criteria for
    credit spreads.
    If the analysis was aleardy performed for the day,
    append the new analysis and write all data.

    Inputs: Date to analyze.
    Returns: None but writes all data to appropriate location.
    """
    ## log time of analysos
    analysis_ts = dt.datetime.now()

    ## make directories, if they don't exists
    dir_dict = {
        "calls": f"{analysis_s.credit_spreads_calls_dir}/{ds}",
        "puts": f"{analysis_s.credit_spreads_puts_dir}/{ds}",
    }
    for option_type in dir_dict:
        if not os.path.isdir(dir_dict[option_type]):
            os.mkdir(dir_dict[option_type])
    ## load global data
    calls_discovery_df = load_calls_discover_data(ds)
    puts_discovery_df = load_puts_discover_data(ds)
    ## complete analysis for different contract types
    for option_type in ["calls", "puts"]:
        if option_type.lower() == "calls":
            ticker_list = calls_discovery_df["symbol"].tolist()
        elif option_type.lower() == "puts":
            ticker_list = puts_discovery_df["symbol"].tolist()
        ## complete analysis for all tickers
        for ticker in ticker_list:
            ## analysis here
            df = read_option_contract(ds, ticker, option_type)
            if len(df) == 0:
                continue  ## no data
            df_liquidity = check_liquidity(df)
            df_liquidity_exp = check_expiration(df_liquidity)
            df_liquidity_exp_strike = determine_spread_strike(
                df_liquidity_exp, option_type
            )
            ## log analysis date
            df_liquidity_exp_strike["analysis_ts"] = analysis_ts
            fn = f"{dir_dict[option_type]}/{ticker}.parquet"
            ## append previously collected
            if os.path.isfile(fn):
                df_liquidity_exp_strike = df_liquidity_exp_strike.append(
                    pd.read_parquet(fn), ignore_index=True
                )
            ## write data
            df_liquidity_exp_strike.to_parquet(fn, index=False)
